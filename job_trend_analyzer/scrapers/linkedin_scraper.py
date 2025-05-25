# scrapers/linkedin_scraper.py
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, ElementClickInterceptedException,
    StaleElementReferenceException, WebDriverException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import logging
import random
from urllib.parse import quote_plus

# Adjust path if utils is one level up
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.data_parser import parse_relative_date, extract_skills_from_text

logger = logging.getLogger(__name__)

MAX_JOBS_TO_SCRAPE = 25 # Max jobs to try and fully process
MAX_SCROLLS = 7 # Increased max scrolls slightly
CARD_CLICK_RETRIES = 2 # Reduced retries, as too many can also cause issues
SCROLL_PAUSE_TIME = 2.0 # Base scroll pause

# --- XPATH Definitions ---
# THESE ARE HIGHLY VOLATILE AND LIKELY TO BREAK WITH LINKEDIN UI CHANGES.
# YOU WILL LIKELY NEED TO UPDATE THESE REGULARLY BY INSPECTING LINKEDIN'S HTML.
# Common strategy: Look for elements with 'data-entity-urn' or stable-looking class names.

XPATH_JOB_LIST_CONTAINER = "//ul[contains(@class, 'jobs-search__results-list') or contains(@class, 'scaffold-layout__list-container')]"
XPATH_JOB_CARD = ".//li[contains(@class, 'jobs-search-results__list-item') or contains(@class, 'occludable-update')][.//div[contains(@class, 'job-card-container')] or .//a[contains(@href, '/jobs/view/')]]"
XPATH_CARD_TITLE_LINK = ".//a[contains(@class,'job-card-list__title') and @href]"
XPATH_CARD_COMPANY = ".//a[contains(@class,'job-card-container__company-name')] | .//span[contains(@class,'job-card-container__company-name')] | .//div[contains(@class,'job-card-container__subtitle')]"
XPATH_CARD_LOCATION = ".//li[contains(@class,'job-card-container__metadata-item') and contains(@class, 'job-card-container__metadata-item--location')]//span | .//ul[contains(@class,'job-card-container__metadata-list')]//li[contains(@class,'job-card-container__metadata-item')][1]//span"
XPATH_CARD_DATE_POSTED = ".//li[contains(@class, 'job-card-container__footer-item')]//time | .//span[contains(@class,'job-card-list__posted-date')]"

XPATH_DETAILS_PANE_CONTAINER = "//div[contains(@class, 'jobs-search__job-details--container') or contains(@class,'scaffold-layout__detail') or contains(@class,'job-view-layout') or section[contains(@class, 'job-view-layout')]]"
XPATH_DETAILS_TITLE = f"{XPATH_DETAILS_PANE_CONTAINER}//h1[contains(@class,'job-details-jobs-unified-top-card__job-title') or contains(@class,'top-card-layout__title') or contains(@class,'job-title')]"
XPATH_DETAILS_COMPANY_LINK_TEXT = f"{XPATH_DETAILS_PANE_CONTAINER}//span[contains(@class,'jobs-unified-top-card__company-name')]//a[contains(@href, '/company/') or contains(@class, 'app-aware-link')] | {XPATH_DETAILS_PANE_CONTAINER}//div[contains(@class,'job-details-jobs-unified-top-card__primary-description-container')]//a[contains(@href, '/company/')]"
XPATH_DETAILS_LOCATION = f"{XPATH_DETAILS_PANE_CONTAINER}//span[contains(@class, 'jobs-unified-top-card__location') or contains(@class,'job-details-jobs-unified-top-card__bullet')]"
XPATH_DETAILS_DATE_POSTED = f"{XPATH_DETAILS_PANE_CONTAINER}//span[contains(@class, 'jobs-unified-top-card__posted-date') or contains(@class,'job-details-jobs-unified-top-card__job-insight')]"
XPATH_DETAILS_DESCRIPTION_CONTAINER = f"{XPATH_DETAILS_PANE_CONTAINER}//div[contains(@class, 'jobs-description-content__text') or contains(@class, 'jobs-description__content') or contains(@class, 'job-description__content') or @id='job-details' or contains(@class,'jobs-description')]"

XPATH_SEE_MORE_JOBS_BUTTON = "//button[@aria-label='See more jobs' or text()='See more jobs' or @name='see-more-jobs']"
XPATH_SCROLLABLE_JOB_LIST_PANE = "//div[contains(@class, 'jobs-search-results-list__container') or (contains(@class, 'scaffold-layout__list-container') and contains(@class, 'jobs-search-results-list')) or div[contains(@class, 'jobs-search-results-list')]]"
# --- End XPATH Definitions ---

def scroll_job_list(driver, scroll_pause_time=SCROLL_PAUSE_TIME, max_scrolls=MAX_SCROLLS):
    logger.info("LINKEDIN_SCRAPER: Scrolling job list to load more jobs...")
    scrollable_pane = None
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, XPATH_JOB_LIST_CONTAINER))
        )
        scrollable_pane_elements = driver.find_elements(By.XPATH, XPATH_SCROLLABLE_JOB_LIST_PANE)
        if scrollable_pane_elements:
            scrollable_pane = scrollable_pane_elements[0]
            logger.debug("LINKEDIN_SCRAPER: Found specific scrollable job list pane.")
        else:
            logger.warning("LINKEDIN_SCRAPER: Specific scrollable job list pane not found by XPATH. Will try scrolling the main window body.")
            scrollable_pane = driver.find_element(By.TAG_NAME, "body")
    except TimeoutException:
        logger.warning("LINKEDIN_SCRAPER: Job list or specific scrollable job list pane not found by XPATH. Will try scrolling the main window body.")
        scrollable_pane = driver.find_element(By.TAG_NAME, "body")
    except Exception as e:
        logger.error(f"LINKEDIN_SCRAPER: Error finding scrollable pane: {e}. Using body as fallback.")
        scrollable_pane = driver.find_element(By.TAG_NAME, "body")

    last_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_pane)
    scrolls_done = 0
    no_change_count = 0

    while scrolls_done < max_scrolls:
        driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", scrollable_pane)
        time.sleep(scroll_pause_time + random.uniform(0.3, 0.8))
        
        new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_pane)
        if new_height == last_height:
            no_change_count += 1
            logger.debug(f"LINKEDIN_SCRAPER: Scroll height unchanged ({no_change_count} consecutive time(s)).")
            if no_change_count >= 2:
                try:
                    see_more_button_elements = driver.find_elements(By.XPATH, XPATH_SEE_MORE_JOBS_BUTTON)
                    if see_more_button_elements:
                        see_more_button = see_more_button_elements[0]
                        if see_more_button.is_displayed() and see_more_button.is_enabled():
                            logger.info("LINKEDIN_SCRAPER: Clicking 'See more jobs' button.")
                            driver.execute_script("arguments[0].click();", see_more_button)
                            time.sleep(scroll_pause_time + 1.5)
                            new_height_after_click = driver.execute_script("return arguments[0].scrollHeight", scrollable_pane)
                            if new_height_after_click == last_height:
                                logger.info("LINKEDIN_SCRAPER: 'See more jobs' clicked, but no new content or button ineffective. Ending scroll.")
                                break
                            else:
                                last_height = new_height_after_click
                                no_change_count = 0
                                logger.debug(f"LINKEDIN_SCRAPER: Content loaded after 'See more jobs' click. New height: {last_height}")
                        else:
                            logger.debug("LINKEDIN_SCRAPER: 'See more jobs' button found but not interactable. Ending scroll.")
                            break
                    else:
                        logger.debug("LINKEDIN_SCRAPER: No 'See more jobs' button found. Ending scroll.")
                        break
                except Exception as e_btn:
                    logger.warning(f"LINKEDIN_SCRAPER: Error interacting with 'See more jobs' button: {e_btn}. Ending scroll.")
                    break
        else:
            no_change_count = 0
            last_height = new_height
        
        scrolls_done += 1
        logger.debug(f"LINKEDIN_SCRAPER: Scrolled {scrolls_done}/{max_scrolls}. New height: {new_height}")
    logger.info(f"LINKEDIN_SCRAPER: Finished scrolling after {scrolls_done} attempts.")


def scrape_linkedin(keyword="software engineer", location="United States"):
    logger.info(f"LINKEDIN_SCRAPER: Starting for keyword='{keyword}', location='{location}'")
    
    chrome_options = Options()
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36")
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')

    driver = None
    try:
        logger.info("LINKEDIN_SCRAPER: Setting up Chrome WebDriver...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60) 
    except WebDriverException as e:
        logger.error(f"LINKEDIN_SCRAPER: WebDriver setup failed: {e}. Check ChromeDriver and Chrome browser compatibility.", exc_info=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"LINKEDIN_SCRAPER: Failed to initialize WebDriver: {e}", exc_info=True)
        return pd.DataFrame()

    keyword_encoded = quote_plus(keyword)
    location_encoded = quote_plus(location)
    search_url = f"https://www.linkedin.com/jobs/search/?keywords={keyword_encoded}&location={location_encoded}&f_TPR=r2592000&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum=0&sortBy=DD" 
    
    jobs_data = []
    processed_urls = set() 
    
    try:
        logger.info(f"LINKEDIN_SCRAPER: Navigating to URL: {search_url}")
        driver.get(search_url)
        time.sleep(random.uniform(5, 8))

        current_url = driver.current_url
        if "authwall" in current_url or "login" in current_url or "checkpoint" in current_url:
            logger.warning(f"LINKEDIN_SCRAPER: Redirected to LinkedIn login/authwall ({current_url}). Unauthenticated scraping is likely blocked. No jobs will be scraped.")
            return pd.DataFrame() 

        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((By.XPATH, XPATH_JOB_LIST_CONTAINER))
        )
        logger.info("LINKEDIN_SCRAPER: Job list container found.")
        
        scroll_job_list(driver, max_scrolls=MAX_SCROLLS) 

        job_list_container_element = driver.find_element(By.XPATH, XPATH_JOB_LIST_CONTAINER)
        initial_job_cards = job_list_container_element.find_elements(By.XPATH, XPATH_JOB_CARD)
        num_initial_cards = len(initial_job_cards)
        logger.info(f"LINKEDIN_SCRAPER: Found {num_initial_cards} potential job cards after scrolling.")
        
        if num_initial_cards == 0:
            logger.warning("LINKEDIN_SCRAPER: No job cards found. LinkedIn structure might have changed, XPaths need update, or no jobs for this query.")
            return pd.DataFrame()

        job_count_scraped = 0
        for card_index in range(min(num_initial_cards, MAX_JOBS_TO_SCRAPE + 15)): 
            if job_count_scraped >= MAX_JOBS_TO_SCRAPE:
                logger.info(f"LINKEDIN_SCRAPER: Reached MAX_JOBS_TO_SCRAPE limit ({MAX_JOBS_TO_SCRAPE}).")
                break
            
            card = None 
            try: 
                job_list_container_element_refetch = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, XPATH_JOB_LIST_CONTAINER))
                )
                current_job_cards = job_list_container_element_refetch.find_elements(By.XPATH, XPATH_JOB_CARD)
                if card_index < len(current_job_cards):
                    card = current_job_cards[card_index]
                else:
                    logger.warning(f"LINKEDIN_SCRAPER: Card index {card_index} out of bounds after re-fetch ({len(current_job_cards)} cards). Skipping.")
                    continue
            except (NoSuchElementException, StaleElementReferenceException, TimeoutException) as e_refetch:
                logger.warning(f"LINKEDIN_SCRAPER: Error re-fetching card at index {card_index}: {e_refetch}. Skipping.")
                continue
            
            if not card: 
                logger.debug(f"LINKEDIN_SCRAPER: Card at index {card_index} is None. Skipping.")
                continue

            card_title, card_company, card_location, card_job_url = "N/A", "N/A", "N/A", None
            card_click_target_element = None

            try:
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", card)
                time.sleep(random.uniform(0.5, 1.0)) 

                card_title_links = card.find_elements(By.XPATH, XPATH_CARD_TITLE_LINK)
                if card_title_links:
                    card_click_target_element = card_title_links[0] 
                    card_title = card_click_target_element.text.strip()
                    card_job_url = card_click_target_element.get_attribute('href')
                
                if not card_job_url:
                    all_links_in_card = card.find_elements(By.XPATH, ".//a[contains(@href, '/jobs/view/')]")
                    if all_links_in_card:
                        card_job_url = all_links_in_card[0].get_attribute('href')
                        if not card_click_target_element: card_click_target_element = all_links_in_card[0]
                        if card_title == "N/A" and all_links_in_card[0].text.strip(): card_title = all_links_in_card[0].text.strip()
                
                if card_job_url: 
                    card_job_url = card_job_url.split('?')[0].strip() 
                
                if not card_click_target_element and card:
                    card_click_target_element = card
                                
            except NoSuchElementException:
                logger.warning(f"LINKEDIN_SCRAPER: Card title/URL element not found (card {card_index}).")
            except Exception as e_card_basic:
                logger.error(f"LINKEDIN_SCRAPER: Error extracting basic card info (card {card_index}): {e_card_basic}", exc_info=False)

            if not card_job_url or card_job_url in processed_urls:
                if not card_job_url: logger.debug(f"LINKEDIN_SCRAPER: Skipping card {card_index} (Title: '{card_title}') - no valid job URL.")
                else: logger.debug(f"LINKEDIN_SCRAPER: Job URL {card_job_url} already processed. Skipping card {card_index}.")
                continue
            
            try:
                card_company_elements = card.find_elements(By.XPATH, XPATH_CARD_COMPANY)
                if card_company_elements: card_company = card_company_elements[0].text.strip()
            except: pass 
            try:
                card_location_elements = card.find_elements(By.XPATH, XPATH_CARD_LOCATION)
                if card_location_elements: card_location = card_location_elements[0].text.strip()
            except: pass 

            clicked_successfully = False
            if card_click_target_element:
                try: WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, XPATH_DETAILS_PANE_CONTAINER)))
                except: logger.debug("LINKEDIN_SCRAPER: Details pane container not immediately present before click, proceeding.")

                for attempt in range(CARD_CLICK_RETRIES):
                    try:
                        card_click_target_element.click()
                        
                        WebDriverWait(driver, 12).until(
                            EC.any_of(
                                EC.visibility_of_element_located((By.XPATH, XPATH_DETAILS_TITLE)),
                                EC.presence_of_element_located((By.XPATH, XPATH_DETAILS_DESCRIPTION_CONTAINER)) 
                            )
                        )
                        time.sleep(random.uniform(1.8, 2.8)) 
                        clicked_successfully = True
                        break 
                    except (ElementClickInterceptedException, StaleElementReferenceException) as e_click:
                        logger.warning(f"LINKEDIN_SCRAPER: Card click attempt {attempt+1} for '{card_title}' intercepted/stale: {e_click}. Retrying...")
                        time.sleep(1.2 + attempt) 
                        # --- CORRECTED BLOCK START ---
                        try: # Re-fetch logic
                            job_list_container_element_refetch = driver.find_element(By.XPATH, XPATH_JOB_LIST_CONTAINER)
                            current_job_cards_refetch = job_list_container_element_refetch.find_elements(By.XPATH, XPATH_JOB_CARD)
                            if card_index < len(current_job_cards_refetch):
                                card_refetched = current_job_cards_refetch[card_index]
                                # Re-establish click target
                                title_links_refetched = card_refetched.find_elements(By.XPATH, XPATH_CARD_TITLE_LINK)
                                if title_links_refetched:
                                    card_click_target_element = title_links_refetched[0]
                                else: # Fallback
                                    all_links_refetched = card_refetched.find_elements(By.XPATH, ".//a[contains(@href, '/jobs/view/')]")
                                    if all_links_refetched:
                                        card_click_target_element = all_links_refetched[0]
                                    else:
                                        card_click_target_element = card_refetched # Default to the card itself
                            else:
                                logger.warning(f"LINKEDIN_SCRAPER: Card index {card_index} out of bounds during re-fetch attempt for '{card_title}'. Stopping click retries.")
                                break # Cant refetch this specific card, break from CARD_CLICK_RETRIES loop
                        except Exception as e_re_re: 
                            logger.debug(f"LINKEDIN_SCRAPER: Failed to re-fetch click target for '{card_title}' during retry: {e_re_re}")
                            break # Stop trying for this card, break from CARD_CLICK_RETRIES loop
                        # --- CORRECTED BLOCK END ---
                    except TimeoutException:
                        logger.warning(f"LINKEDIN_SCRAPER: Details pane did not load/update for '{card_title}' after click attempt {attempt+1}.")
                        if "linkedin.com/jobs/view/" in driver.current_url and card_job_url in driver.current_url:
                            logger.info("LINKEDIN_SCRAPER: Click might have navigated to a full job page. Attempting to parse.")
                            clicked_successfully = True 
                            break 
                    except Exception as e_gen_click:
                        logger.error(f"LINKEDIN_SCRAPER: General error clicking card '{card_title}' (attempt {attempt+1}): {e_gen_click}", exc_info=True)
            
            details_title, details_company, details_location, date_posted_str, job_description_text = \
                card_title, card_company, card_location, "Unknown", ""

            if clicked_successfully:
                logger.debug(f"LINKEDIN_SCRAPER: Successfully clicked/navigated for '{card_title}'. Extracting details.")
                try:
                    details_title_el = WebDriverWait(driver, 4).until(EC.visibility_of_element_located((By.XPATH, XPATH_DETAILS_TITLE)))
                    details_title = details_title_el.text.strip()
                except: logger.debug(f"Details title not found for {card_job_url}. Using card title: '{card_title}'")
                
                try: 
                    company_el = WebDriverWait(driver, 4).until(EC.visibility_of_element_located((By.XPATH, XPATH_DETAILS_COMPANY_LINK_TEXT)))
                    details_company = company_el.text.strip()
                except: logger.debug(f"Details company not found for {card_job_url}. Using card company: '{card_company}'")
                
                try:
                    details_location_el = WebDriverWait(driver, 4).until(EC.visibility_of_element_located((By.XPATH, XPATH_DETAILS_LOCATION)))
                    details_location = details_location_el.text.strip()
                except: logger.debug(f"Details location not found for {card_job_url}. Using card location: '{card_location}'")
                
                try:
                    date_elements = WebDriverWait(driver,4).until(EC.presence_of_all_elements_located((By.XPATH, XPATH_DETAILS_DATE_POSTED)))
                    if date_elements: date_posted_str = date_elements[0].text.strip().split('·')[0].strip() 
                except: logger.debug(f"LINKEDIN_SCRAPER: Date posted not found in details for '{details_title}'.")
                
                try:
                    desc_container = WebDriverWait(driver,4).until(EC.presence_of_element_located((By.XPATH, XPATH_DETAILS_DESCRIPTION_CONTAINER)))
                    job_description_text = desc_container.get_attribute('innerText') or desc_container.text
                    job_description_text = job_description_text.strip() if job_description_text else ""
                except: logger.warning(f"LINKEDIN_SCRAPER: Job description container not found for '{details_title}'.")

                if "linkedin.com/jobs/view/" in driver.current_url and search_url not in driver.current_url and card_job_url in driver.current_url:
                    logger.info("LINKEDIN_SCRAPER: Navigating back to search results page.")
                    driver.get(search_url)
                    time.sleep(random.uniform(3,5))
                    try:
                        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, XPATH_JOB_LIST_CONTAINER)))
                        scroll_job_list(driver, max_scrolls=1, scroll_pause_time=1.0)
                    except TimeoutException:
                        logger.warning("LINKEDIN_SCRAPER: Failed to re-establish job list after navigating back. Subsequent scrapes might fail.")
                        break 
            else:
                logger.warning(f"LINKEDIN_SCRAPER: Failed to click/load details for '{card_title}' (URL: {card_job_url}). Using card data primarily.")
            
            final_title = details_title if details_title not in ["N/A", "", None] else card_title
            final_company = details_company if details_company not in ["N/A", "", None] else card_company
            final_location = details_location if details_location not in ["N/A", "", None] else card_location

            if final_title == "N/A" or not final_title: 
                logger.warning(f"LINKEDIN_SCRAPER: Skipping job at URL {card_job_url} due to missing title after all attempts.")
                processed_urls.add(card_job_url) 
                continue

            skills = extract_skills_from_text(final_title + " " + job_description_text)

            jobs_data.append({
                'title': final_title,
                'company': final_company,
                'location': final_location,
                'skills': ", ".join(skills) if skills else "N/A",
                'date_posted': date_posted_str,
                'parsed_date': parse_relative_date(date_posted_str),
                'source': 'LinkedIn',
                'search_keyword': keyword,
                'url': card_job_url
            })
            processed_urls.add(card_job_url)
            job_count_scraped += 1
            logger.info(f"LINKEDIN_SCRAPER: Scraped ({job_count_scraped}/{MAX_JOBS_TO_SCRAPE}): {final_title} @ {final_company}")
            time.sleep(random.uniform(1.0, 2.2)) 

    except TimeoutException as te:
        logger.error(f"LINKEDIN_SCRAPER: Timeout waiting for critical initial elements (e.g., job list container). LinkedIn might be slow, page structure changed, or CAPTCHA/authwall. URL: {search_url}", exc_info=True)
    except WebDriverException as wde: 
        logger.error(f"LINKEDIN_SCRAPER: A WebDriverException occurred: {wde}", exc_info=True)
    except Exception as e:
        logger.error(f"LINKEDIN_SCRAPER: An unexpected error occurred: {e}", exc_info=True)
    finally:
        if driver:
            driver.quit()
            logger.info("LINKEDIN_SCRAPER: WebDriver closed.")

    df = pd.DataFrame(jobs_data)
    if not df.empty and 'url' in df.columns:
        df.drop_duplicates(subset=['url'], keep='first', inplace=True)
    logger.info(f"LINKEDIN_SCRAPER: Finished. Total unique jobs scraped: {len(df)} for keyword '{keyword}'.")
    return df

if __name__ == '__main__':
    test_keyword = "Data Analyst" 
    test_location = "United States" 
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s')
    logger.info(f"--- Running LinkedIn Scraper Standalone Test for '{test_keyword}' in '{test_location}' ---")
    
    linkedin_df = scrape_linkedin(keyword=test_keyword, location=test_location)
    
    if not linkedin_df.empty:
        print(f"\n--- Successfully scraped {len(linkedin_df)} jobs from LinkedIn ---")
        print("--- Sample Data (first 5): ---")
        print(linkedin_df.head().to_string())
    else:
        print(f"--- No jobs scraped from LinkedIn for '{test_keyword}' in '{test_location}'. ---")
        print("--- Possible issues: LinkedIn login wall, CAPTCHA, outdated XPaths, or IP blocking. ---")
        print("--- Check console logs for DEBUG/WARNING/ERROR messages. Try running this script without headless mode to observe browser. ---")
        print("--- You may need to update XPaths at the top of scrapers/linkedin_scraper.py by inspecting LinkedIn's HTML. ---")