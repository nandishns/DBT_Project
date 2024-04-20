from queue import Queue
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException, TimeoutException
from selenium.webdriver.common.keys import Keys
import os, time, json, requests
from kafka import KafkaProducer
from threading import Thread
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from collections import Counter
from dotenv import load_dotenv
from selenium.webdriver.chrome.service import Service

load_dotenv()
TWITTER_USER_NAME=os.getenv("TWITTER_USER_NAME")
TWITTER_PASSWORD=os.getenv("TWITTER_PASSWORD")



def setup_webdriver():

    # ChromeDriverManager().clear_cache()
    chrome_options = Options()   
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.binary_location = "/usr/bin/google-chrome" 

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)
 

def setup_kafka_producer():
    print("Setting up Kafka producer")
    return KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def scrape_slashdot(producer):
    print("Scraping Slashdot")
    try:
        driver = setup_webdriver()
        # driver.maximize_window()
        wait = WebDriverWait(driver, 10)
        driver.get("https://slashdot.org/")
        # time.sleep(5)
        all_element = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Software")))
        all_element.click()

        while True:
            products = driver.find_elements(By.CSS_SELECTOR, "div.result-heading-texts")
            for product in products:
                product_name = product.find_element(By.CSS_SELECTOR, "h3").text
                description = product.find_element(By.CSS_SELECTOR, "div.description").text
                data = {"name": product_name, "description": description}
                # Send data to Kafka
                producer.send('Software', value=data)

            time.sleep(5)
            try:
                next_button = driver.find_element(By.LINK_TEXT, "Next")
                next_button.click()
            except:
                print("No more pages to scrape for Slashdot.")
                break
            # finally:
            #   driver.quit()
    except NoSuchElementException as e:
       
        # print(f"Element not found: {e}")
        pass
    except ElementNotInteractableException as e:
     
        # print(f"Element not interactable: {e}")
        pass
    except TimeoutException as e:
       
        # print(f"Operation timed out: {e}")
        pass
    except Exception as e:
        
        # print(f"Error in scrape_slashdot: {e}")
        pass


def scrape_producthunt(producer):
    print("Scraping Product Hunt")
    def scrape_base_page(url):
        try:
            
            response = requests.get(url)
            response.raise_for_status() 

            # Parse the HTML content of the page with BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            # print(f"Failed to retrieve data from {url}: {e}")
            return []
        collected_hrefs = []

       
        divs = soup.find_all('div', class_='styles_item__Dk_nz my-2 flex flex-1 flex-row gap-2 py-2 sm:gap-4')
        for div in divs:
            # Find all anchor tags within each div
            anchor_tags = div.find_all('a')
            for a in anchor_tags:
                href = a.get('href')  # Get the href attribute
                if href and '/posts/' in href:
                    full_url = urljoin(url, href)
                    collected_hrefs.append(full_url)

        return collected_hrefs

    def extract_additional_data(post_urls):
        base_url = 'https://www.producthunt.com'
        posts_data = []
        counter = Counter(post_urls)

    
        new_post_urls = []
        for url in post_urls:
            if counter[url] > 1:
                counter[url] -= 1
            else:
                new_post_urls.append(url)

        post_urls = new_post_urls

        for url in post_urls:
            try:
                post_response = requests.get(url)
                post_response.raise_for_status()
                
                
                post_soup = BeautifulSoup(post_response.text, 'html.parser')
                
              
                post_h1 = post_soup.find('h1').text if post_soup.find('h1') else "No H1 tag found"
                
                target_divs = post_soup.find_all('div', class_='styles_htmlText__eYPgj text-16 font-normal text-dark-grey')
                div_text = ' '.join(div.text for div in target_divs)
                
            
                data = {'url': url, 'name': post_h1, 'description': div_text}
                
                # Send the data to Kafka
                producer.send('Software', value=data)
            except requests.RequestException as e:
                # print(f"Failed to retrieve or process data from {url}: {e}")
                continue  

            posts_data.append(data)

        return posts_data

    
    base_url = 'https://www.producthunt.com/all'

   
    post_hrefs = scrape_base_page(base_url)

   
    posts_info = extract_additional_data(post_hrefs)
       


def scrape_twitter(producer):
    print("Scraping Twitter")
    """Function to log into Twitter and scrape tweets based on a hashtag."""
    driver = setup_webdriver()
    try:
        # Navigating to Twitter's login page
        driver.get("https://twitter.com/login")
        time.sleep(3)  # Wait to ensure the page is fully loaded

        # Log in process
        username_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "text")))
        username_input.send_keys(TWITTER_USER_NAME)
        # print(TWITTER_USER_NAME)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Next']"))).click()
        
        password_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.NAME, "password")))
        password_input.send_keys(TWITTER_PASSWORD)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Log in']"))).click()

        # Wait for home page to load by checking presence of Home timeline
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="primaryColumn"]')))

        # Perform search
        search_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-testid='SearchBox_Search_Input']")))
        search_input.send_keys("#b2bsoftware")
        search_input.send_keys(Keys.ENTER)
        time.sleep(5)  # Wait for search results to load
        
        # Extract tweets
        tweets = []
        last_height = driver.execute_script("return document.documentElement.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            time.sleep(3)
            new_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        print("Scraping tweets")
        tweet_elements = driver.find_elements(By.CSS_SELECTOR, '[data-testid="tweet"]')
        for tweet in tweet_elements:
            # print("Tweets",tweet.text)
            tweets.append(tweet.text)

        # Printing first 5 tweets for brevity
        for tweet in tweets[:]:
            # print(tweet)
            producer.send('x-llm', tweet)
            producer.flush()

    except Exception as e:
        # print(f"Error in scrape_twitter: {e}")
        pass
    finally:
        driver.quit()



def run_safe(func, *args):
    try:
        func(*args)
    except Exception as e:
        # print(f"Error in {func.__name__}: {e}")
        pass


def main():
    producer = setup_kafka_producer()
    # Initialize and start threads

    thread1 = Thread(target=run_safe, args=(scrape_slashdot, producer))
    thread2 = Thread(target=run_safe, args=(scrape_producthunt, producer))
    thread3 = Thread(target=run_safe, args=(scrape_twitter, producer))

    
    thread1.start()
    thread2.start()
    thread3.start()
   

    # Wait for both threads to complete
    thread1.join()
    thread2.join()
    thread3.join()


    producer.close()

if __name__ == "__main__":
    main()
