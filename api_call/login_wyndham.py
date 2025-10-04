from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json
import requests
import time
import os 
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 1. Setup Selenium
options = Options()
options.add_experimental_option("detach", True)
options.add_argument("--disable-blink-features=AutomationControlled")

driver = webdriver.Chrome(options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

# 2. Load credentials
load_dotenv()
username = os.getenv("WYNDHAM_USERNAME")
password = os.getenv("WYNDHAM_PASSWORD")

if not username or not password:
    print("❌ Please set WYNDHAM_USERNAME and WYNDHAM_PASSWORD in the .env file.")
    exit()
else:
    print("✅ Username and Password loaded successfully.")

# 3. Open login page
driver.get("https://clubwyndham.wyndhamdestinations.com/us/en/login.html")

try:
    wait = WebDriverWait(driver, 20)

    # Fill username
    username_field = wait.until(EC.presence_of_element_located((By.ID, "okta-signin-username")))
    username_field.clear()
    username_field.send_keys(username)

    # Fill password
    password_field = driver.find_element(By.ID, "okta-signin-password")
    password_field.clear()
    password_field.send_keys(password)

    # Try to close cookie consent banner if present
    try:
        consent_button = driver.find_element(By.ID, "onetrust-accept-btn-handler")
        consent_button.click()
        print("✅ Cookie consent accepted.")
        time.sleep(1)  # Give time for banner to disappear
    except Exception:
        pass  # If not present, continue

    # Click login
    login_button = driver.find_element(By.ID, "okta-signin-submit")
    login_button.click()

    print("\n⏳ Complete login + OTP in the browser, then press ENTER here to continue...")
    input()  

except Exception as e:
    print(f"Error: {e}")
    driver.quit()
    exit()

# 4. Extract cookies and local storage
cookies = driver.get_cookies()
local_storage = driver.execute_script("""
    const store = {};
    for (let i = 0; i < localStorage.length; i++) {
        let key = localStorage.key(i);
        store[key] = localStorage.getItem(key);
    }
    return store;
""")

session_storage = driver.execute_script("""
    const store = {};
    for (let i = 0; i < sessionStorage.length; i++) {
        let key = sessionStorage.key(i);
        store[key] = sessionStorage.getItem(key);
    }
    return store;
""")


save_dir = "auth_data"
os.makedirs(save_dir, exist_ok=True)

with open(os.path.join(save_dir, "cookies.json"), "w") as f:
    json.dump(cookies, f, indent=4)
with open(os.path.join(save_dir, "local_storage.json"), "w") as f:
    json.dump(local_storage, f, indent=4)
with open(os.path.join(save_dir, "session_storage.json"), "w") as f:
    json.dump(session_storage, f, indent=4)
print(f"✅ Cookies, local storage, and session storage saved to '{save_dir}' folder.")

driver.quit()