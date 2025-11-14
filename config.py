# config.py
import os

# --- Project Paths ---
PROJECT_ROOT = r"C:\Users\Abhishek Yadav\Documents\Product Title Scraper"

# --- DEDICATED PROFILE SETUP (IN A SAFE LOCATION) ---
# We will ONLY use this path. Chrome will create 'Default' inside it automatically.
CHROME_PROFILE_PATH = r"C:\Users\Abhishek Yadav\Documents\Product Title Scraper\Chrome-Master-Profile" 

# --- Google Sheets Config ---
SHEET_NAME = "Product Title Scraper"
WORKSHEET_NAME = "Ranking"
GCP_CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "gcp_credentials.json")

# --- Scraping Config ---
SEARCH_URL = "https://www.google.com"
KEYWORDS_PER_BATCH = 40

# --- NEW: CAPTCHA HANDLING CONFIG ---
# The total time (in seconds) the script will wait for a CAPTCHA to be solved manually.
CAPTCHA_WAIT_TIMEOUT = 900  # 900 seconds = 15 minutes
# The interval (in seconds) at which the script checks if the CAPTCHA is gone.
CAPTCHA_CHECK_INTERVAL = 5  # 5 seconds

# List of user agents to rotate for stealth
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
]

# --- EMAIL NOTIFICATION SETTINGS ---
# Set to True to enable email notifications on error
ENABLE_EMAIL_NOTIFICATIONS = True

# Your Outlook account credentials
SENDER_EMAIL = "innovatecorelab@gmail.com"  # <-- CHANGE THIS
SENDER_PASSWORD = "znkg wtmx wpmt imxl"      # <-- CHANGE THIS

# The email address(es) that will receive the error alerts.
# This is now a LIST of strings.
RECIPIENT_EMAIL = [
    "abhishek.yadav@infidigit.com",
    # You can add more emails here, just add a comma at the end.
]

# Standard Outlook SMTP Server settings
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587