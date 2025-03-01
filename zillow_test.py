import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
]
# Add a random delay
time.sleep(random.uniform(2, 5))
# Set up headless mode and random user-agent
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Run in headless mode
chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
chrome_options.add_argument("--window-size=1920,1080")  # Set window size
chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")

# Initialize the WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# Set up Selenium WebDriver
url = 'https://www.zillow.com/homes/for_rent'
driver.get(url)

# Wait for the page to load
try:
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'list-card-info')))
except Exception as e:
    print("Error: Listings did not load. Zillow may have blocked the request.")
    driver.quit()
    exit()

# Get the page source and parse it with BeautifulSoup
soup = BeautifulSoup(driver.page_source, 'html.parser')

# Find property listings
listings = soup.find_all('div', class_='list-card-info')  # Update with the correct class name

# Extract data
data = []
for listing in listings:
    try:
        address = listing.find('address', class_='list-card-addr').text.strip()
        price = listing.find('div', class_='list-card-price').text.strip()
        link = listing.find('a', class_='list-card-link')['href']
        # Ensure the link is absolute
        if not link.startswith('http'):
            link = 'https://www.zillow.com' + link
        data.append([address, price, link])
    except AttributeError:
        continue

# Save data to a CSV file
df = pd.DataFrame(data, columns=['Address', 'Price', 'Link'])
df.to_csv('zillow_listings.csv', index=False)

# Close the WebDriver
driver.quit()