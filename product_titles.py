# Product_titles.py (Version 10 - Persistent "Retry, Don't Skip" Logic)

import time
import logging
import os
import gspread
import re
import traceback
import smtplib
from email.mime.text import MIMEText
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

import config

# =====================================================================================
# --- CONFIGURATION ---
# =====================================================================================

INPUT_WORKSHEET_NAME = "Header scraper"
OUTPUT_WORKSHEET_NAME = "Scraped Products"
MAX_PAGES_TO_SCRAPE = 120
RESTART_DRIVER_AFTER_N_URLS = 25 

# --- NEW: Configuration for retrying a failed URL ---
MAX_RETRIES_PER_URL = 3

WEBSITE_CONFIGS = {
    "Myntra": {
        "data_selectors": {"Html 1": "h3.product-brand", "Html2": "h4.product-product"},
        "product_container": "div.product-productMetaInfo",
        "total_pages_info": "li.pagination-paginationMeta",
        "next_page_button": "li.pagination-next",
    },
}

# =====================================================================================
# --- CORE SCRIPT (No changes in this section) ---
# =====================================================================================

log_file_path = os.path.join(config.PROJECT_ROOT, 'product_titles.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_file_path, mode='w'), logging.StreamHandler()])

def send_error_email(subject, body):
    if not config.ENABLE_EMAIL_NOTIFICATIONS: return
    recipients = config.RECIPIENT_EMAIL
    try:
        msg = MIMEText(body, 'plain')
        msg['Subject'], msg['From'], msg['To'] = subject, config.SENDER_EMAIL, ", ".join(recipients)
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.SENDER_EMAIL, config.SENDER_PASSWORD)
            server.sendmail(config.SENDER_EMAIL, recipients, msg.as_string())
            logging.info("Error email sent successfully.")
    except Exception as e:
        logging.error(f"CRITICAL: FAILED TO SEND ERROR EMAIL. Error: {e}")

def setup_driver():
    logging.info("Initializing a fresh Chrome WebDriver instance...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    driver.set_script_timeout(60)
    return driver

def connect_to_google_sheets():
    logging.info("Connecting to Google Sheets API...")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(config.GCP_CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)
    logging.info("Successfully connected to Google Sheets API.")
    return client

def get_data_from_sheet(worksheet):
    logging.info(f"Fetching URLs from worksheet: '{worksheet.title}'")
    url_list = worksheet.col_values(1)[1:]
    non_empty_urls = [url for url in url_list if url.strip()]
    logging.info(f"Successfully fetched {len(non_empty_urls)} URLs to process.")
    return non_empty_urls

def parse_total_pages(text):
    match = re.search(r'of (\d+)', text)
    if match: return int(match.group(1))
    return 1

def scrape_url(driver, url, site_config):
    all_scraped_data = {col_name: [] for col_name in site_config["data_selectors"].keys()}
    driver.get(url)
    first_product_selector = site_config["data_selectors"]["Html 1"]
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, first_product_selector)))
    logging.info("Product data has loaded successfully.")
    total_pages = 1
    try:
        pages_info_element = driver.find_element(By.CSS_SELECTOR, site_config["total_pages_info"])
        total_pages = parse_total_pages(pages_info_element.text)
        logging.info(f"Detected {total_pages} total pages.")
    except NoSuchElementException:
        logging.warning("Pagination info not found. Assuming a single page.")
    pages_to_scrape = min(total_pages, MAX_PAGES_TO_SCRAPE)
    logging.info(f"Will scrape a maximum of {pages_to_scrape} pages.")
    for page_num in range(1, pages_to_scrape + 1):
        logging.info(f"Scraping page {page_num}/{pages_to_scrape}...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        first_product_selector = site_config["data_selectors"]["Html 1"]
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, first_product_selector)))
        product_containers = driver.find_elements(By.CSS_SELECTOR, site_config["product_container"])
        for container in product_containers:
            for col_name, selector in site_config["data_selectors"].items():
                element = container.find_element(By.CSS_SELECTOR, selector)
                all_scraped_data[col_name].append(element.text.strip())
        if page_num < pages_to_scrape:
            next_button = driver.find_element(By.CSS_SELECTOR, site_config["next_page_button"])
            driver.execute_script("arguments[0].click();", next_button)
            WebDriverWait(driver, 15).until(EC.url_contains(f"p={page_num + 1}"))
    return all_scraped_data

# --- MAIN EXECUTION BLOCK with RETRY LOGIC ---
if __name__ == "__main__":
    logging.info(f"--- Starting Product Title Scraper ---")
    driver = None
    try:
        gspread_client = connect_to_google_sheets()
        sheet = gspread_client.open(config.SHEET_NAME)
        input_worksheet = sheet.worksheet(INPUT_WORKSHEET_NAME)
        output_worksheet = sheet.worksheet(OUTPUT_WORKSHEET_NAME)
        urls_to_process = get_data_from_sheet(input_worksheet)
        
        logging.info(f"Clearing the output sheet '{OUTPUT_WORKSHEET_NAME}' for a fresh start...")
        output_worksheet.clear()
        headers = ["Source URL", "Html 1", "Html2"]
        output_worksheet.append_row(headers, value_input_option='USER_ENTERED')
        logging.info("Output sheet cleared and headers restored.")

        site_key = "Myntra"
        current_config = WEBSITE_CONFIGS[site_key]
        driver = setup_driver()
        
        urls_processed_since_restart = 0

        for i, url in enumerate(urls_to_process):
            if urls_processed_since_restart >= RESTART_DRIVER_AFTER_N_URLS:
                logging.warning(f"Performing scheduled restart of the browser after {urls_processed_since_restart} URLs...")
                driver.quit()
                driver = setup_driver()
                urls_processed_since_restart = 0
            
            logging.info(f"\n--- Processing URL {i+1}/{len(urls_to_process)}: {url} ---")
            
            # --- NEW: Retry loop for each URL ---
            for attempt in range(MAX_RETRIES_PER_URL):
                try:
                    scraped_data = scrape_url(driver, url, current_config)
                    
                    if scraped_data and scraped_data.get("Html 1"):
                        rows_to_append = []
                        num_products = len(scraped_data["Html 1"])
                        for product_index in range(num_products):
                            brand = scraped_data["Html 1"][product_index]
                            product_name = scraped_data["Html2"][product_index]
                            new_row = [url, brand, product_name]
                            rows_to_append.append(new_row)
                        if rows_to_append:
                            output_worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                            logging.info(f"Successfully wrote {len(rows_to_append)} new rows to '{OUTPUT_WORKSHEET_NAME}'.")
                    else:
                        logging.error(f"No data was scraped for URL: {url}")
                    
                    # If we get here, the URL was processed successfully.
                    # We break out of the retry loop.
                    break

                except Exception as e:
                    logging.error(f"!!! Attempt {attempt + 1}/{MAX_RETRIES_PER_URL} FAILED for URL: {url}")
                    logging.error(f"Error details: {e.__class__.__name__}")
                    
                    # If this was the last attempt, log a critical failure.
                    if attempt == MAX_RETRIES_PER_URL - 1:
                        logging.critical(f"!!! URL WILL BE SKIPPED after {MAX_RETRIES_PER_URL} failed attempts.")
                        # Optionally send an email alert for a skipped URL
                        # send_error_email(f"URL Skipped: {url}", f"The scraper failed to process the URL {url} after {MAX_RETRIES_PER_URL} attempts.")
                    else:
                        logging.warning("Recovering by restarting the driver and retrying the SAME URL...")

                    try:
                        driver.quit()
                    except Exception:
                        logging.error("Failed to quit the unresponsive driver.")
                    
                    driver = setup_driver()
                    urls_processed_since_restart = 0
            
            urls_processed_since_restart += 1

    except gspread.exceptions.WorksheetNotFound:
        logging.critical(f"FATAL ERROR: The output worksheet '{OUTPUT_WORKSHEET_NAME}' was not found.")
    except Exception as e:
        error_traceback = traceback.format_exc()
        logging.critical(f"A critical, unhandled error occurred outside the main loop: {e}\n{error_traceback}")
        send_error_email("Product Scraper Alert: SCRIPT CRASHED", f"Error:\n{e}\n\nTraceback:\n{error_traceback}")
    finally:
        if driver:
            logging.info("Closing WebDriver.")
            driver.quit()
        logging.info("--- Product Title Scraper Finished ---")