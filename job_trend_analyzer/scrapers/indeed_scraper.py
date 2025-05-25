# scrapers/indeed_scraper.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import random
from urllib.parse import urljoin, quote_plus

# Adjust path if utils is one level up
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.data_parser import parse_relative_date, extract_skills_from_text

logger = logging.getLogger(__name__)

MAX_JOBS_INDEED = 30  # Max jobs to try and fetch for a given query
PAGES_TO_SCRAPE = 3 # Number of pages to iterate through (e.g., 3 pages * 10-15 jobs/page)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36', # More recent UA
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,application/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
}

def scrape_indeed_job_details(job_url):
    logger.debug(f"INDEED_DETAILS: Fetching details for URL: {job_url}")
    try:
        time.sleep(random.uniform(0.7, 1.8)) # Polite delay
        response = requests.get(job_url, headers=HEADERS, timeout=15) # Increased timeout
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        # XPath for job description: //div[@id='jobDescriptionText']
        description_div = soup.find('div', id='jobDescriptionText')
        description_text = description_div.get_text(separator=' ', strip=True) if description_div else ""
        
        if not description_text: # Fallback selector if primary one fails
            # XPath: //div[contains(@class, 'jobsearch-jobDescriptionText')]
            description_div_fallback = soup.select_one('div.jobsearch-jobDescriptionText')
            if description_div_fallback:
                description_text = description_div_fallback.get_text(separator=' ', strip=True)
        
        return description_text
    except requests.exceptions.HTTPError as e:
        logger.warning(f"INDEED_DETAILS: HTTP error {e.response.status_code} fetching job details from {job_url}: {e}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"INDEED_DETAILS: Request error fetching job details from {job_url}: {e}")
    except Exception as e:
        logger.error(f"INDEED_DETAILS: Error parsing job details from {job_url}: {e}", exc_info=True)
    return ""

def scrape_indeed(keyword="software engineer", location="United States", scrape_full_description=False):
    logger.info(f"INDEED_SCRAPER: Starting for keyword='{keyword}', location='{location}', FullDesc={scrape_full_description}")
    base_url = "https://www.indeed.com" # Consider country-specific Indeed if needed e.g. indeed.ca
    jobs_data = []
    job_count = 0
    processed_job_keys = set() # To avoid processing duplicate job keys from pagination overlaps

    for page_num in range(PAGES_TO_SCRAPE):
        start_index = page_num * 10 # Indeed uses 'start' for pagination, often 10 jobs per page
        if job_count >= MAX_JOBS_INDEED:
            logger.info(f"INDEED_SCRAPER: Reached MAX_JOBS_INDEED limit ({MAX_JOBS_INDEED}).")
            break

        keyword_encoded = quote_plus(keyword)
        location_encoded = quote_plus(location)
        # &sort=date to get most recent jobs first. &fromage=7 for last 7 days.
        search_url = f"{base_url}/jobs?q={keyword_encoded}&l={location_encoded}&sort=date&start={start_index}"
        logger.info(f"INDEED_SCRAPER: Scraping page {page_num + 1}/{PAGES_TO_SCRAPE}: {search_url}")

        try:
            response = requests.get(search_url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            # Indeed can be sensitive; a longer delay between page requests is safer.
            time.sleep(random.uniform(2.5, 5.0)) 

            soup = BeautifulSoup(response.content, 'lxml')

            # --- Conceptual addition to find Indeed logo/link ---
            # Using CSS selector that approximates the XPath //a[@aria-label='Indeed Home']
            indeed_home_link = soup.select_one('a[aria-label="Indeed Home"]')
            if indeed_home_link:
                logger.debug(f"INDEED_SCRAPER: Found Indeed Home Link: {indeed_home_link.get('href')}")
            # --- End conceptual addition ---

            # Job cards container: Look for <ul class="jobsearch-ResultsList"> or similar
            # XPath: //ul[contains(@class, 'jobsearch-ResultsList')] | //div[@id='mosaic-provider-jobcards']
            job_list_container = soup.find('ul', class_=lambda x: x and 'jobsearch-ResultsList' in x)
            if not job_list_container:
                job_list_container = soup.find('div', id='mosaic-provider-jobcards') # For mosaic view

            if not job_list_container:
                logger.warning(f"INDEED_SCRAPER: Job cards container not found on page {page_num + 1} for URL {search_url}. HTML structure might have changed or CAPTCHA triggered.")
                # Debug: Save HTML if container not found
                # with open(f"indeed_page_{page_num+1}_debug_no_container.html", "w", encoding="utf-8") as f:
                #     f.write(soup.prettify())
                continue # Try next page or break

            # Job cards: Usually <li> elements or <div> with specific class/data attributes
            # XPath for cards in list view: //div[contains(@class, 'job_seen_beacon')] or //div[contains(@class, 'cardOutline')]
            # XPath for cards in mosaic view: //a[@data-jk] (links that are job cards)
            job_cards = job_list_container.select('div.job_seen_beacon, div.cardOutline, td.resultContent > div.job_seen_beacon, a[data-jk]')
            
            logger.info(f"INDEED_SCRAPER: Found {len(job_cards)} potential job cards on page {page_num + 1}.")
            if not job_cards:
                logger.info(f"INDEED_SCRAPER: No job cards found on this page for {search_url}. End of results or issue.")
                break # No more jobs on this page

            for card in job_cards:
                if job_count >= MAX_JOBS_INDEED: break

                # Title: Look for <h2><a><span>job title</span></a></h2> or similar
                # XPath: .//h2[contains(@class,'jobTitle')]/a/span[@title] | .//h2[contains(@class,'jobTitle')]/span[@title] | .//a[contains(@class,'jcs-JobTitle')]/span
                title_tag = card.select_one('h2.jobTitle a span[title], h2.jobTitle span[title], a.jcs-JobTitle span, span[id^="jobTitle-"]')
                title = title_tag.get_text(strip=True) if title_tag else "N/A"
                if title == "N/A" and card.select_one('h2.jobTitle'): # Fallback
                    title = card.select_one('h2.jobTitle').get_text(strip=True).replace("new","").strip()


                # Company: <span class="companyName"> or <span data-testid="company-name">
                # XPath: .//span[@class='companyName'] | .//span[@data-testid='company-name']
                company_tag = card.select_one('span.companyName, [data-testid="company-name"]')
                company = company_tag.get_text(strip=True) if company_tag else "N/A"

                # Location: <div class="companyLocation"> or <div data-testid="text-location">
                # XPath: .//div[@class='companyLocation'] | .//div[@data-testid='text-location']
                location_tag = card.select_one('div.companyLocation, [data-testid="text-location"]')
                location_text = location_tag.get_text(strip=True) if location_tag else "N/A"

                # Date Posted: <span class="date"> or specific CSS classes
                # XPath: .//span[contains(@class,'date')] | .//div[contains(@class,'jobMetaDataGroup')]/div[1]/span | .//span[contains(@class,'css-')] (more fragile)
                date_posted_tag = card.select_one('span.date, div.jobMetaDataGroup > div:first-child > span, span.css-10pe3me, span.css-1flbcou') # Added another common date selector
                date_posted_str = date_posted_tag.get_text(strip=True).replace('PostedPosted', 'Posted').split('·')[0].strip() if date_posted_tag else "Unknown"
                if "hiring ongoing" in date_posted_str.lower() or not date_posted_str: date_posted_str = "Unknown"
                
                job_key_attr = card.get('data-jk') # From mosaic card or job_seen_beacon div
                if not job_key_attr: # Try to find within a child if card itself doesn't have it
                    job_key_element = card.select_one('a[data-jk]')
                    if job_key_element:
                        job_key_attr = job_key_element.get('data-jk')
                
                job_url = None
                if job_key_attr:
                    if job_key_attr in processed_job_keys:
                        logger.debug(f"INDEED_SCRAPER: Job key {job_key_attr} already processed. Skipping.")
                        continue
                    job_url = f"{base_url}/viewjob?jk={job_key_attr}"
                else: # Fallback: try to get from a link if no job key
                    link_tag = card.select_one('h2.jobTitle a[href], a.jcs-JobTitle[href]')
                    if link_tag and link_tag.get('href'):
                        relative_url = link_tag.get('href')
                        if relative_url.startswith(('/rc/clk', '/pagead/clk', '/company/', '/clk?')):
                            job_url = urljoin(base_url, relative_url)
                        elif relative_url.startswith('/viewjob?jk='):
                             job_url = urljoin(base_url, relative_url)
                
                if not job_url:
                    logger.warning(f"INDEED_SCRAPER: Could not determine job URL for card. Title: {title} @ {company}. Skipping.")
                    continue
                
                # Clean URL from tracking parameters
                job_url = job_url.split('&jt=')[0].split('?clk=')[0].split('?fccid=')[0].split('&tk=')[0]

                description_to_parse = title + " " + company + " " + location_text
                # Snippet: <div class="job-snippet"> or <ul> with list items
                # XPath: .//div[contains(@class,'job-snippet')]/ul | .//div[contains(@class,'job-snippet')]
                snippet_div = card.select_one('div.job-snippet ul, div.job-snippet, ul[style^="list-style-type:circle"]')
                if snippet_div:
                    description_to_parse += " " + snippet_div.get_text(separator=' ', strip=True)
                
                if scrape_full_description:
                    full_desc_text = scrape_indeed_job_details(job_url)
                    if full_desc_text:
                        description_to_parse += " " + full_desc_text
                    else: 
                        logger.debug(f"INDEED_SCRAPER: No full description retrieved for {job_url}, using snippet and title for skills.")
                
                skills = extract_skills_from_text(description_to_parse)

                jobs_data.append({
                    'title': title,
                    'company': company,
                    'location': location_text,
                    'skills': ", ".join(skills) if skills else "N/A",
                    'date_posted': date_posted_str,
                    'parsed_date': parse_relative_date(date_posted_str),
                    'source': 'Indeed',
                    'search_keyword': keyword,
                    'url': job_url
                })
                if job_key_attr: processed_job_keys.add(job_key_attr)
                job_count += 1
                logger.info(f"INDEED_SCRAPER: Scraped ({job_count}/{MAX_JOBS_INDEED}): {title} at {company}")
                time.sleep(random.uniform(0.3, 0.8)) # Small delay between processing cards

        except requests.exceptions.HTTPError as e:
            logger.error(f"INDEED_SCRAPER: HTTP error for {search_url}: {e}. Status code: {e.response.status_code}")
            if e.response.status_code in [403, 429, 503]: # Common blocking codes
                logger.warning("INDEED_SCRAPER: Indeed might be blocking requests (CAPTCHA, rate limit). Consider proxies, longer delays, or reducing request frequency.")
                # Debug: Save HTML if blocked
                # with open(f"indeed_page_{page_num+1}_debug_blocked.html", "w", encoding="utf-8") as f:
                #     f.write(response.text) # Use .text as .content might be gzipped
            break # Stop if blocked on a page
        except requests.exceptions.RequestException as e:
            logger.error(f"INDEED_SCRAPER: Request error for {search_url}: {e}")
            break # Stop on general request errors
        except Exception as e:
            logger.error(f"INDEED_SCRAPER: General error processing page {page_num + 1} ({search_url}): {e}", exc_info=True)
            # Potentially continue to next page if it's a parsing error for one page
            
    df = pd.DataFrame(jobs_data)
    if not df.empty and 'url' in df.columns:
        df.drop_duplicates(subset=['url'], keep='first', inplace=True) # Ensure unique jobs by URL
    logger.info(f"INDEED_SCRAPER: Finished. Total unique jobs scraped: {len(df)} for keyword '{keyword}'.")
    return df

if __name__ == '__main__':
    test_keyword = "Dotnet Developer"
    test_location = "United States"
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(message)s')
    logger.info(f"--- Running Indeed Scraper Standalone Test for '{test_keyword}' in '{test_location}' ---")
    
    indeed_df = scrape_indeed(keyword=test_keyword, location=test_location, scrape_full_description=True)

    if not indeed_df.empty:
        print(f"\n--- Successfully scraped {len(indeed_df)} jobs from Indeed ---")
        print("--- Sample Data (first 5): ---")
        print(indeed_df.head().to_string())
    else:
        print(f"--- No jobs scraped from Indeed for '{test_keyword}' in '{test_location}'. Check logs/selectors. ---")