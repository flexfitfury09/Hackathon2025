# scrapers/linkedin_scraper.py
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import logging
import random

# Assuming data_parser.py is in ../utils/
from utils.data_parser import parse_relative_date, extract_skills_from_text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

MAX_JOBS_TO_SCRAPE = 20 # Number of jobs to try to scrape per run
MAX_SCROLLS = 4         # How many times to scroll the job list
CARD_CLICK_RETRIES = 2  # Retries for clicking a job card

# --- XPATH Definitions ---
# These are based on recent observations and your screenshots but are HIGHLY VOLATILE.
XPATH_JOB_LIST_CONTAINER = "//main[@id='main']//ul[contains(@class, 'scaffold-layout__list-results') or contains(@class, 'jobs-search-results__list')]"
XPATH_JOB_CARD = ".//li[contains(@class, 'occludable-update') and .//div[@data-job-id]]" # Targets <li> if it has a child div with data-job-id
XPATH_CARD_TITLE_LINK = ".//div[contains(@class, 'job-card-list__title')]/a | .//a[contains(@class,'job-card-container__link')] | .//div[contains(@class,'artdeco-entity-lockup__title')]/a[contains(@href, '/jobs/view/')]" # Prioritize specific title links
XPATH_CARD_COMPANY = ".//div[contains(@class, 'artdeco-entity-lockup__subtitle')]//span[1] | .//span[contains(@class, 'job-card-container__primary-description')]"
XPATH_CARD_LOCATION = ".//ul[contains(@class,'job-card-container__metadata-wrapper')]/li[1] | .//div[contains(@class, 'artdeco-entity-lockup__caption')]//li[1] | .//div[contains(@class, 'job-card-container__metadata-item')][1]"

XPATH_DETAILS_PANE_CONTAINER = "//div[contains(@class, 'scaffold-layout__detail') and (contains(@class, 'jobs-search__job-details') or contains(@class, 'job-view-layout'))]"
XPATH_DETAILS_TITLE = f"{XPATH_DETAILS_PANE_CONTAINER}//h1 | {XPATH_DETAILS_PANE_CONTAINER}//h2[contains(@class, 'job-title') or contains(@class,'top-card-layout__title')] | {XPATH_DETAILS_PANE_CONTAINER}//div[contains(@class, 'job-details-jobs-unified-top-card__job-title')]"
XPATH_DETAILS_COMPANY_LINK = f"{XPATH_DETAILS_PANE_CONTAINER}//div[contains(@class, 'job-details-jobs-unified-top-card__company-name')]/a | {XPATH_DETAILS_PANE_CONTAINER}//div[contains(@class, 'job-details-jobs-unified-top-card__primary-description')]//a[contains(@href, '/company/')]"
XPATH_DETAILS_LOCATION = f"{XPATH_DETAILS_PANE_CONTAINER}//span[contains(@class,'jobs-unified-top-card__bullet')]/preceding-sibling::span | {XPATH_DETAILS_PANE_CONTAINER}//span[contains(@class, 'jobs-unified-top-card__location')]"
XPATH_DETAILS_DATE_POSTED = f"{XPATH_DETAILS_PANE_CONTAINER}//span[contains(@class, 'jobs-unified-top-card__posted-date')] | {XPATH_DETAILS_PANE_CONTAINER}//span[contains(@class, 'job-details-jobs-unified-top-card__job-insight') and (contains(.,'ago') or contains(.,'Posted') or contains(.,'Today'))]"
XPATH_DETAILS_DESCRIPTION_CONTAINER = f"{XPATH_DETAILS_PANE_CONTAINER}//div[contains(@id, 'job-details')] | {XPATH_DETAILS_PANE_CONTAINER}//div[contains(@class, 'jobs-description-content__text')]"

XPATH_SEE_MORE_JOBS_BUTTON = "//button[@aria-label='See more jobs']" # Check if this button is still used
XPATH_SCROLLABLE_JOB_LIST_PANE = "//div[contains(@class, 'jobs-search-results-list__container')] | //div[contains(@class, 'scaffold-layout__list')]/div[contains(@class, 'jobs-search__results-list')]"
# --- End XPATH Definitions ---

def scroll_job_list(driver, scroll_pause_time=2.5, max_scrolls=MAX_SCROLLS):
    # ... (scroll_job_list function from previous response - ensure it's correct and robust)
    logging.info("LINKEDIN_SCRAPER: Scrolling job list...")
    try:
        scroll_pane = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, XPATH_SCROLLABLE_JOB_LIST_PANE))
        )
        logging.info("LINKEDIN_SCRAPER: Found scrollable job list pane.")
    except TimeoutException:
        logging.warning("LINKEDIN_SCRAPER: Specific scrollable job list pane not found. Will try scrolling window.")
        scroll_pane = None 

    last_height = driver.execute_script("return arguments[0] ? arguments[0].scrollHeight : document.body.scrollHeight", scroll_pane)
    scrolls_done = 0
    consecutive_no_change = 0
    
    while scrolls_done < max_scrolls:
        if scroll_pane:
            driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", scroll_pane)
        else:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        time.sleep(scroll_pause_time + random.uniform(0.5, 1.5))
        new_height = driver.execute_script("return arguments[0] ? arguments[0].scrollHeight : document.body.scrollHeight", scroll_pane)
        
        if new_height == last_height:
            consecutive_no_change +=1
            logging.debug(f"LINKEDIN_SCRAPER: Scroll height unchanged ({consecutive_no_change} times).")
            if consecutive_no_change >= 2: 
                try: # Try to click "See more jobs" if it exists
                    see_more_button = driver.find_element(By.XPATH, XPATH_SEE_MORE_JOBS_BUTTON)
                    if see_more_button.is_displayed() and see_more_button.is_enabled():
                        logging.info("LINKEDIN_SCRAPER: Attempting to click 'See more jobs' button.")
                        driver.execute_script("arguments[0].click();", see_more_button)
                        time.sleep(scroll_pause_time + 1) 
                        new_height_after_click = driver.execute_script("return arguments[0] ? arguments[0].scrollHeight : document.body.scrollHeight", scroll_pane)
                        if new_height_after_click != new_height:
                            last_height = new_height_after_click
                            consecutive_no_change = 0
                            scrolls_done +=1 
                            continue
                        else:
                            logging.info("LINKEDIN_SCRAPER: 'See more jobs' clicked, but no new content loaded.")
                            break
                    else: break 
                except NoSuchElementException:
                    logging.debug("LINKEDIN_SCRAPER: No 'See more jobs' button found.")
                    break 
                except Exception as e_btn:
                    logging.warning(f"LINKEDIN_SCRAPER: Error interacting with 'See more jobs' button: {e_btn}")
                    break
        else: 
            consecutive_no_change = 0
            last_height = new_height

        scrolls_done += 1
        logging.debug(f"LINKEDIN_SCRAPER: Scrolled {scrolls_done}/{max_scrolls} times. New height: {new_height}")
    logging.info(f"LINKEDIN_SCRAPER: Finished scrolling after {scrolls_done} attempts or no new content.")


def scrape_linkedin(keyword="software engineer", location="United States"):
    logging.info(f"LINKEDIN_SCRAPER: Starting for keyword='{keyword}', location='{location}'")
    
    chrome_options = Options()
    # Comment out headless for easier debugging of XPaths initially
    # chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized") # Helps with element visibility
    # Use a common, relatively recent user agent
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")

    driver = None # Initialize driver to None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception as e:
        logging.error(f"LINKEDIN_SCRAPER: Failed to initialize WebDriver: {e}")
        return pd.DataFrame()

    # For more results, remove f_TPR or use a wider range. f_TPR=r604800 is last 7 days.
    time_filter = "r604800" 
    search_url = f"https://www.linkedin.com/jobs/search/?keywords={keyword.replace(' ', '%20')}&location={location.replace(' ', '%20')}&f_TPR={time_filter}&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0"
    
    jobs_data = []
    processed_urls = set() # To avoid processing the same job if it appears multiple times
    
    try:
        driver.get(search_url)
        logging.info(f"LINKEDIN_SCRAPER: Opened URL: {search_url}")
        # Wait for the main job list container to be present
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, XPATH_JOB_LIST_CONTAINER))
        )
        time.sleep(random.uniform(3, 5)) # Allow dynamic content to load

        scroll_job_list(driver, max_scrolls=MAX_SCROLLS)

        # Get all job cards after scrolling
        try:
            job_list_container_element = driver.find_element(By.XPATH, XPATH_JOB_LIST_CONTAINER)
            # Find elements relative to the container
            initial_job_cards = job_list_container_element.find_elements(By.XPATH, XPATH_JOB_CARD)
            num_initial_cards = len(initial_job_cards)
        except NoSuchElementException:
            logging.error(f"LINKEDIN_SCRAPER: Could not find job list container with XPath: {XPATH_JOB_LIST_CONTAINER}")
            num_initial_cards = 0
        
        logging.info(f"LINKEDIN_SCRAPER: Found {num_initial_cards} potential job cards on the page.")
        
        if num_initial_cards == 0:
            logging.warning("LINKEDIN_SCRAPER: No job cards found. LinkedIn structure might have changed, XPaths need update, or no jobs for this query.")
            # driver.save_screenshot("linkedin_no_jobs.png") # For debugging
            return pd.DataFrame() # Return empty DataFrame

        job_count = 0
        for card_index in range(num_initial_cards):
            if job_count >= MAX_JOBS_TO_SCRAPE:
                logging.info(f"LINKEDIN_SCRAPER: Reached MAX_JOBS_TO_SCRAPE limit ({MAX_JOBS_TO_SCRAPE}).")
                break
            
            card = None # Initialize card to None
            try: # Re-fetch the specific card by index in case DOM changed
                # Re-locate parent container first for robustness against stale elements
                current_job_list_container = driver.find_element(By.XPATH, XPATH_JOB_LIST_CONTAINER)
                current_job_cards = current_job_list_container.find_elements(By.XPATH, XPATH_JOB_CARD)
                if card_index < len(current_job_cards):
                    card = current_job_cards[card_index]
                else:
                    logging.warning(f"LINKEDIN_SCRAPER: Card index {card_index} out of bounds after DOM refresh. Skipping.")
                    continue 
            except (NoSuchElementException, StaleElementReferenceException) as e_refetch:
                logging.warning(f"LINKEDIN_SCRAPER: Error re-fetching card at index {card_index}: {e_refetch}. Skipping.")
                continue
            
            if card is None: continue # Should not happen if above logic is correct, but as a safeguard

            # --- Extract from Card (Fallback data and click target) ---
            card_title, card_company, card_location, card_job_url = "N/A", "N/A", "N/A", None
            card_title_link_element = None

            try:
                # Scroll the card into view for interaction
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", card)
                time.sleep(random.uniform(0.4, 0.8)) 

                card_title_link_element = card.find_element(By.XPATH, XPATH_CARD_TITLE_LINK)
                card_title = card_title_link_element.text.strip()
                card_job_url = card_title_link_element.get_attribute('href')
                if card_job_url and 'linkedin.com/jobs/view/' in card_job_url:
                     card_job_url = card_job_url.split('?')[0].strip() # Clean URL
                else: # If specific title link fails, try a more generic link within the card
                    all_links_in_card = card.find_elements(By.TAG_NAME, 'a')
                    for link_tag in all_links_in_card:
                        href = link_tag.get_attribute('href')
                        if href and 'linkedin.com/jobs/view/' in href:
                            card_job_url = href.split('?')[0].strip()
                            if not card_title_link_element: card_title_link_element = link_tag # Use this as click target
                            if card_title == "N/A" and link_tag.text.strip(): card_title = link_tag.text.strip() # Try to get title from this link
                            break
                    if not card_job_url: card_job_url = None # Ensure it's None if no valid URL found
                            
            except NoSuchElementException:
                logging.warning(f"LINKEDIN_SCRAPER: Card title/URL link not found for card index {card_index} using XPath: {XPATH_CARD_TITLE_LINK}")
            except Exception as e_card_extract:
                logging.error(f"LINKEDIN_SCRAPER: Error extracting basic card info for index {card_index}: {e_card_extract}")

            if not card_job_url or card_job_url in processed_urls:
                if not card_job_url: logging.warning(f"LINKEDIN_SCRAPER: Skipping card index {card_index} (Title: '{card_title}') due to missing job URL.")
                else: logging.debug(f"LINKEDIN_SCRAPER: Job URL {card_job_url} already processed. Skipping card index {card_index}.")
                continue
            
            try: card_company = card.find_element(By.XPATH, XPATH_CARD_COMPANY).text.strip()
            except NoSuchElementException: logging.debug(f"LINKEDIN_SCRAPER: Card company not found for '{card_title}'")
            try: card_location = card.find_element(By.XPATH, XPATH_CARD_LOCATION).text.strip()
            except NoSuchElementException: logging.debug(f"LINKEDIN_SCRAPER: Card location not found for '{card_title}'")

            # Click the card/title to load details in the right pane
            clicked_successfully = False
            if card_title_link_element: # We need a valid element to click
                for attempt in range(CARD_CLICK_RETRIES):
                    try:
                        driver.execute_script("arguments[0].click();", card_title_link_element) # JS click
                        # Wait for details pane to be visible and contain some key element (e.g., description or title)
                        WebDriverWait(driver, 15).until(
                            EC.visibility_of_element_located((By.XPATH, XPATH_DETAILS_PANE_CONTAINER))
                        )
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, f"{XPATH_DETAILS_PANE_CONTAINER}//*[self::h1 or self::h2 or self::div[contains(@id,'job-details') or contains(@class,'description')]]"))
                        )
                        time.sleep(random.uniform(1.5, 3.0)) # Allow content to fully render
                        clicked_successfully = True
                        break 
                    except (ElementClickInterceptedException, StaleElementReferenceException) as e_click:
                        logging.warning(f"LINKEDIN_SCRAPER: Card click attempt {attempt+1} for '{card_title}' intercepted or stale: {e_click}. Retrying...")
                        time.sleep(1.5)
                        # Re-fetch card_title_link_element if stale, as the card itself might be stale too
                        try:
                            current_job_list_container_temp = driver.find_element(By.XPATH, XPATH_JOB_LIST_CONTAINER) # Re-find container
                            current_job_cards_temp = current_job_list_container_temp.find_elements(By.XPATH, XPATH_JOB_CARD) # Re-find cards
                            if card_index < len(current_job_cards_temp):
                                card_temp = current_job_cards_temp[card_index] # Get current card
                                card_title_link_element = card_temp.find_element(By.XPATH, XPATH_CARD_TITLE_LINK) # Re-get title link
                        except:
                            logging.warning("LINKEDIN_SCRAPER: Failed to re-fetch click target after StaleElement.")
                            pass # Let the loop retry the click
                    except TimeoutException:
                        logging.warning(f"LINKEDIN_SCRAPER: Details pane did not load/become visible for '{card_title}' on click attempt {attempt+1}.")
                    except Exception as e_gen_click:
                        logging.error(f"LINKEDIN_SCRAPER: General error clicking card '{card_title}' on attempt {attempt+1}: {e_gen_click}")
            
            # Initialize details with card data as fallback
            details_title, details_company, details_location, date_posted_str, job_description_text = \
                card_title, card_company, card_location, "Unknown", ""

            if clicked_successfully:
                logging.debug(f"LINKEDIN_SCRAPER: Successfully clicked card: {card_title}. Extracting details.")
                try: details_title = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, XPATH_DETAILS_TITLE))).text.strip()
                except (NoSuchElementException, TimeoutException): logging.debug(f"LINKEDIN_SCRAPER: Details title not found for '{card_job_url}', using card title: '{card_title}'.")
                
                try: 
                    company_el = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, XPATH_DETAILS_COMPANY_LINK)))
                    details_company = company_el.text.strip()
                except (NoSuchElementException, TimeoutException): logging.debug(f"LINKEDIN_SCRAPER: Details company not found for '{card_job_url}', using card company: '{card_company}'.")
                
                try: details_location = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, XPATH_DETAILS_LOCATION))).text.strip()
                except (NoSuchElementException, TimeoutException): logging.debug(f"LINKEDIN_SCRAPER: Details location not found for '{card_job_url}', using card location: '{card_location}'.")
                
                try:
                    date_elements = WebDriverWait(driver,5).until(EC.presence_of_all_elements_located((By.XPATH, XPATH_DETAILS_DATE_POSTED)))
                    if date_elements: date_posted_str = date_elements[0].text.strip().split('·')[0].strip() # Get first part if "·" exists
                except (NoSuchElementException, TimeoutException): logging.warning(f"LINKEDIN_SCRAPER: Date posted not found in details for '{details_title}'.")
                
                try:
                    desc_container = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, XPATH_DETAILS_DESCRIPTION_CONTAINER)))
                    # Try to get innerText first for better formatting, then fall back to .text
                    job_description_text = desc_container.get_attribute("innerText") 
                    if not job_description_text or len(job_description_text) < 20 : # If innerText is too short or empty
                        job_description_text = desc_container.text
                    job_description_text = job_description_text.strip() if job_description_text else ""
                except (NoSuchElementException, TimeoutException): logging.warning(f"LINKEDIN_SCRAPER: Job description not found in details for '{details_title}'.")
            else:
                logging.warning(f"LINKEDIN_SCRAPER: Failed to click/load details for '{card_title}' (URL: {card_job_url}). Using card data as primary.")
            
            # Use the best available title
            final_title = details_title if details_title != "N/A" and details_title else card_title
            final_company = details_company if details_company != "N/A" and details_company else card_company
            final_location = details_location if details_location != "N/A" and details_location else card_location

            skills = extract_skills_from_text(final_title + " " + job_description_text)

            jobs_data.append({
                'title': final_title,
                'company': final_company,
                'location': final_location,
                'skills': ", ".join(skills) if skills else "N/A", # Store as comma-separated string
                'date_posted': date_posted_str,
                'parsed_date': parse_relative_date(date_posted_str),
                'source': 'LinkedIn',
                'search_keyword': keyword,
                'url': card_job_url # This URL should be the definitive one from the card
            })
            processed_urls.add(card_job_url)
            job_count += 1
            logging.info(f"LINKEDIN_SCRAPER: Scraped ({job_count}/{MAX_JOBS_TO_SCRAPE}): {final_title} at {final_company}")
            time.sleep(random.uniform(0.6, 1.5)) # Polite delay between processing jobs

    except TimeoutException:
        logging.error("LINKEDIN_SCRAPER: Timeout waiting for critical initial elements on LinkedIn (e.g., job list container).")
        # driver.save_screenshot("linkedin_timeout_critical.png") # For debugging
    except Exception as e:
        logging.error(f"LINKEDIN_SCRAPER: An unexpected error occurred: {e}", exc_info=True) # exc_info for full traceback
        # driver.save_screenshot("linkedin_unexpected_error.png") # For debugging
    finally:
        if driver: # Check if driver was initialized
            driver.quit()
            logging.info("LINKEDIN_SCRAPER: WebDriver closed.")

    df = pd.DataFrame(jobs_data)
    # Final check for duplicates based on URL, though processed_urls should handle most
    if not df.empty and 'url' in df.columns:
        df.drop_duplicates(subset=['url'], keep='first', inplace=True)
    logging.info(f"LINKEDIN_SCRAPER: Finished. Total jobs scraped: {len(df)}")
    return df

if __name__ == '__main__':
    # --- Test the scraper ---
    # It's crucial to test non-headless first to verify XPaths!
    # To do this, comment out `chrome_options.add_argument("--headless")` inside `scrape_linkedin`
    
    test_keyword = "Python Developer" 
    test_location = "United States" 
    # test_location = "Remote"
    
    logging.info(f"--- Running LinkedIn Scraper Standalone Test for '{test_keyword}' in '{test_location}' ---")
    
    # Ensure utils can be imported if running directly from scrapers/
    import sys
    import os
    if os.path.basename(os.getcwd()) == 'scrapers': # If CWD is scrapers/
        sys.path.append(os.path.join(os.path.dirname(__file__), '..')) 
        from utils.data_parser import parse_relative_date, extract_skills_from_text # Re-import for this scope
        from utils.db_manager import store_jobs, init_db # Re-import for this scope
    else: # Assuming running from project root
        from utils.data_parser import parse_relative_date, extract_skills_from_text
        from utils.db_manager import store_jobs, init_db


    linkedin_df = scrape_linkedin(keyword=test_keyword, location=test_location)
    
    if not linkedin_df.empty:
        print(f"\n--- Successfully scraped {len(linkedin_df)} jobs from LinkedIn ---")
        print("--- Sample Data: ---")
        print(linkedin_df.head())
        
        # Optional: Store to DB during standalone test
        print("\n--- Storing to Database (Test) ---")
        init_db() 
        num_stored = store_jobs(linkedin_df)
        print(f"--- Stored {num_stored} new unique jobs in the database. ---")
    else:
        print(f"--- No jobs scraped from LinkedIn for '{test_keyword}' in '{test_location}'. Check logs and XPaths. ---")