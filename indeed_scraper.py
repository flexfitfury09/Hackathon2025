# scrapers/indeed_scraper.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import random # For randomized delays
from urllib.parse import urljoin # To construct absolute URLs

# Assuming data_parser.py is in ../utils/
from utils.data_parser import parse_relative_date, extract_skills_from_text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

MAX_JOBS_INDEED = 30 # Target number of jobs to scrape from Indeed per run
PAGES_TO_SCRAPE = 2  # Number of search result pages to scrape (Indeed typically has ~15 jobs/page)

# Common headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
}

# --- Optional: Function to scrape details from individual job pages ---
# This will significantly slow down scraping but provide richer skill data.
# def scrape_indeed_job_details(job_url):
#     """Scrapes detailed information (especially full description) from a single Indeed job page."""
#     logging.debug(f"INDEED_DETAILS: Fetching details for URL: {job_url}")
#     try:
#         response = requests.get(job_url, headers=HEADERS, timeout=10)
#         response.raise_for_status()
#         time.sleep(random.uniform(1.0, 2.0)) # Polite delay

#         soup = BeautifulSoup(response.content, 'html.parser')
        
#         description_div = soup.find('div', id='jobDescriptionText')
#         description_text = description_div.get_text(separator=' ', strip=True) if description_div else ""
        
#         # You could extract more details here if needed (e.g., job type, salary if present)
        
#         return description_text

#     except requests.exceptions.RequestException as e:
#         logging.warning(f"INDEED_DETAILS: Failed to fetch job details from {job_url}: {e}")
#     except Exception as e:
#         logging.error(f"INDEED_DETAILS: Error parsing job details from {job_url}: {e}")
#     return "" # Return empty string on failure
# --- End Optional Detail Scraping Function ---


def scrape_indeed(keyword="software engineer", location="United States"):
    logging.info(f"INDEED_SCRAPER: Starting for keyword='{keyword}', location='{location}'")
    base_url = "https://www.indeed.com" # Adjust if using a country-specific Indeed domain (e.g., indeed.co.uk)
    jobs_data = []
    job_count = 0
    processed_job_keys = set() # To avoid duplicates based on Indeed's job key (jk)

    # Indeed uses 'start' parameter for pagination, typically in steps of 10
    for page_num in range(0, PAGES_TO_SCRAPE):
        start_index = page_num * 10
        if job_count >= MAX_JOBS_INDEED:
            logging.info(f"INDEED_SCRAPER: Reached MAX_JOBS_INDEED limit ({MAX_JOBS_INDEED}).")
            break

        search_url = f"{base_url}/jobs?q={keyword.replace(' ', '+')}&l={location.replace(' ', '+')}&start={start_index}"
        logging.info(f"INDEED_SCRAPER: Scraping page {page_num + 1}: {search_url}")

        try:
            response = requests.get(search_url, headers=HEADERS, timeout=15)
            response.raise_for_status() 
            time.sleep(random.uniform(1.5, 3.0)) # Polite delay between page requests

            soup = BeautifulSoup(response.content, 'html.parser')
            
            # --- Locating Job Cards ---
            # Indeed's job card selectors can change. These are common ones.
            # Look for a list container first, then individual cards.
            # Common container: <ul class="jobsearch-ResultsList"> or <div id="mosaic-provider-jobcards">
            # Individual cards often <li> or <div> with class containing 'result', 'jobCard_mainContent', 'job_seen_beacon'
            # Or inside 'mosaic-provider-jobcards', cards might be <a> tags.
            
            job_cards_container = soup.find('ul', class_='jobsearch-ResultsList')
            if not job_cards_container: # Fallback for mosaic view
                 job_cards_container = soup.find('div', id='mosaic-provider-jobcards')

            if not job_cards_container:
                logging.warning(f"INDEED_SCRAPER: Could not find main job cards container on page: {search_url}. Structure might have changed.")
                # with open(f"indeed_page_no_container_{page_num}.html", "w", encoding="utf-8") as f:
                #    f.write(response.text) # Save page for debugging
                continue # Try next page if container not found

            # Find all potential job card elements within the container
            # This XPath-like selection tries various common patterns for cards
            job_cards = job_cards_container.select('li div.job_seen_beacon, div.job_seen_beacon, div.result, div.jobCard_mainContent, td.resultContent, a[id^="job_"]') # More comprehensive selector
            
            if not job_cards: # If the above doesn't work, try a simpler approach if mosaic view is used
                if soup.find('div', id='mosaic-provider-jobcards'):
                    job_cards = soup.select('div#mosaic-provider-jobcards a[id^="job_"]') # Links within mosaic

            logging.info(f"INDEED_SCRAPER: Found {len(job_cards)} potential job cards on page {page_num + 1}.")

            if not job_cards:
                logging.info("INDEED_SCRAPER: No more job cards found on this page or subsequent pages.")
                break # Stop if no cards are found on a page

            for card in job_cards:
                if job_count >= MAX_JOBS_INDEED:
                    break

                # --- Extracting Data from each Card ---
                title_tag = card.select_one('h2.jobTitle > a > span, span[id^="jobTitle-"], a.jcs-JobTitle > span') 
                title = title_tag.get_text(strip=True) if title_tag else "N/A"
                if title == "N/A": # Try another common title pattern
                    title_tag_alt = card.select_one('h2.jobTitle, .jobTitle > span') # Simpler selector for title text
                    title = title_tag_alt.get_text(strip=True).replace("new","").strip() if title_tag_alt else "N/A"


                company_tag = card.select_one('span.companyName, [data-testid="company-name"]')
                company = company_tag.get_text(strip=True) if company_tag else "N/A"

                location_tag = card.select_one('div.companyLocation, [data-testid="text-location"]')
                location_text = location_tag.get_text(strip=True) if location_tag else "N/A"

                date_posted_tag = card.select_one('span.date, div.jobMetaDataGroup > div > span') # Common date locations
                date_posted_str = date_posted_tag.get_text(strip=True).replace('PostedPosted', 'Posted').replace('Hiring ongoing', '') if date_posted_tag else "Unknown"
                date_posted_str = date_posted_str.split('·')[0].strip() # Take first part if multiple insights

                # Job URL and Job Key (jk)
                job_url = None
                job_key = card.get('data-jk') # data-jk is a reliable identifier for Indeed jobs
                
                # Try to get URL from title link first
                link_tag = card.select_one('h2.jobTitle > a[href], a.jcs-JobTitle[href]')
                if link_tag and link_tag.get('href'):
                    relative_url = link_tag.get('href')
                    if relative_url.startswith('/'):
                        job_url = urljoin(base_url, relative_url)
                    # If it's an ad link, it might be absolute but not the final job URL
                    # Prioritize jk-based URL if jk is available
                
                if job_key: # If we have a job key, construct the canonical URL
                    job_url = f"{base_url}/viewjob?jk={job_key}"
                    if job_key in processed_job_keys:
                        logging.debug(f"INDEED_SCRAPER: Job key {job_key} already processed. Skipping duplicate.")
                        continue # Skip if already processed based on jk
                
                if not job_url: # Fallback if no jk and no good link from title
                    logging.warning(f"INDEED_SCRAPER: Could not determine job URL for card: {title} at {company}. Skipping.")
                    continue

                job_url = job_url.split('&jt=')[0].split('?clk=')[0].split('?fccid=')[0] # Clean up common tracking params


                # --- Skill Extraction ---
                # Option 1: Basic skills from title and snippet (faster)
                description_snippet_tag = card.select_one('div.job-snippet, ul[style^="list-style-type:circle"] li')
                description_snippet = description_snippet_tag.get_text(separator=' ', strip=True) if description_snippet_tag else ""
                skills = extract_skills_from_text(title + " " + description_snippet)
                
                # Option 2: Scrape full description from job detail page (slower, more accurate skills)
                # Uncomment this block and the helper function at the top to enable it.
                # if job_url:
                #     full_description = scrape_indeed_job_details(job_url)
                #     if full_description:
                #         skills = extract_skills_from_text(title + " " + full_description)
                #     else: # Fallback if detail page scraping fails
                #         skills = extract_skills_from_text(title + " " + description_snippet)
                # else: # Fallback if no URL
                #     skills = extract_skills_from_text(title + " " + description_snippet)
                # --- End Skill Extraction Options ---


                jobs_data.append({
                    'title': title,
                    'company': company,
                    'location': location_text,
                    'skills': ", ".join(skills) if skills else "N/A", # Store as comma-separated string
                    'date_posted': date_posted_str,
                    'parsed_date': parse_relative_date(date_posted_str),
                    'source': 'Indeed',
                    'search_keyword': keyword,
                    'url': job_url
                })
                if job_key: processed_job_keys.add(job_key)
                job_count += 1
                logging.info(f"INDEED_SCRAPER: Scraped ({job_count}/{MAX_JOBS_INDEED}): {title} at {company}")
                time.sleep(random.uniform(0.1, 0.3)) # Small delay between processing cards on the same page

        except requests.exceptions.HTTPError as e:
            logging.error(f"INDEED_SCRAPER: HTTP error for {search_url}: {e}. Status code: {e.response.status_code}")
            if e.response.status_code in [403, 429, 503]: # Forbidden, Too Many Requests, Service Unavailable
                logging.warning("INDEED_SCRAPER: Indeed might be blocking requests. Try again later, use proxies, or reduce request rate.")
            break # Stop if we hit a significant HTTP error for a page
        except requests.exceptions.RequestException as e:
            logging.error(f"INDEED_SCRAPER: Request error for {search_url}: {e}")
            break # Stop on other request errors
        except Exception as e:
            logging.error(f"INDEED_SCRAPER: An general error occurred on page {page_num + 1} ({search_url}): {e}", exc_info=True)
            # Continue to next page or break depending on severity (for now, continue)
            
    df = pd.DataFrame(jobs_data)
    # Final check for duplicates based on URL, though job_key set should handle most
    if not df.empty and 'url' in df.columns:
        df.drop_duplicates(subset=['url'], keep='first', inplace=True)
    logging.info(f"INDEED_SCRAPER: Finished. Total jobs scraped: {len(df)}")
    return df


if __name__ == '__main__':
    # --- Test the Indeed scraper ---
    test_keyword = "Data Analyst"
    test_location = "Canada"
    # test_location = "London, UK"

    logging.info(f"--- Running Indeed Scraper Standalone Test for '{test_keyword}' in '{test_location}' ---")

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

    indeed_df = scrape_indeed(keyword=test_keyword, location=test_location)

    if not indeed_df.empty:
        print(f"\n--- Successfully scraped {len(indeed_df)} jobs from Indeed ---")
        print("--- Sample Data: ---")
        print(indeed_df.head())
        
        # Optional: Store to DB during standalone test
        print("\n--- Storing to Database (Test) ---")
        init_db() 
        num_stored = store_jobs(indeed_df)
        print(f"--- Stored {num_stored} new unique jobs in the database. ---")
    else:
        print(f"--- No jobs scraped from Indeed for '{test_keyword}' in '{test_location}'. Check logs and selectors. ---")