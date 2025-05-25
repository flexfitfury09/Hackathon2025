# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import logging
import random

# --- Module Imports ---
# Ensure these paths are correct based on your project structure
try:
    from scrapers.linkedin_scraper import scrape_linkedin
    from scrapers.indeed_scraper import scrape_indeed, HEADERS as indeed_headers # For a small test
    from utils.db_manager import store_jobs, fetch_jobs # init_db is called in db_manager now
    from utils.data_parser import parse_relative_date, extract_skills_from_text, analyze_skills, COMMON_SKILLS
except ImportError as e:
    st.error(f"Failed to import a module. Please check your project structure and sys.path. Error: {e}")
    logging.error(f"APP: Module import error: {e}", exc_info=True)
    st.stop()


# --- Configure Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(name)s - %(module)s - %(funcName)s - %(message)s',
                    handlers=[logging.StreamHandler()]) # Ensure logs go to console for Streamlit
logger = logging.getLogger(__name__)


# --- Database Initialization Check ---
try:
    fetch_jobs(keyword="selftest_app_startup")
    logger.info("APP: Database connection test successful during app startup.")
except Exception as e:
    logger.error(f"APP: Critical error connecting to or querying database at startup. Error: {e}", exc_info=True)
    st.error(f"Critical error: Database is not accessible. Please check logs. Error: {e}")
    st.stop()


# --- Constants for Demo Mode ---
LINKEDIN_DEMO_COUNT = 30
INDEED_DEMO_COUNT = 30

# --- Demo Data Generation Function ---
def generate_demo_data(keyword, source, count):
    logger.info(f"APP_DEMO: Generating {count} demo jobs for {source} with keyword '{keyword}'")
    demo_jobs_list = []
    base_titles = ["Software Engineer", "Data Analyst", "Product Manager", "UX Designer", "DevOps Engineer", "Project Manager", "Data Scientist", "Business Analyst", "QA Tester", "Frontend Developer", "Backend Developer", "Full Stack Developer", "Cloud Engineer", "Security Analyst", "ML Engineer"]
    common_companies = ["Innovate Corp", "Tech Solutions Inc.", "Analytics Co", "AI Driven LLC", "BigTech Co", "Startup X", "Consulting Firm Y", "Data Insights Z", "Global Corp", "Local Biz", "Future Systems", "NextGen Software", "Synergy Systems", "Digital Frontier", "Quantum Leap"]
    common_locations = ["New York, NY", "San Francisco, CA", "Chicago, IL", "Austin, TX", "Remote", "Boston, MA", "Seattle, WA", "Los Angeles, CA", "Toronto, ON", "Vancouver, BC", "London, UK", "Berlin, Germany", "Paris, FR", "Amsterdam, NL", "Dublin, IE", "Hybrid - Austin, TX"]

    for i in range(count):
        title_base = random.choice(base_titles)
        if keyword.lower() in [k.lower() for k in COMMON_SKILLS if "developer" in k or "engineer" in k]:
             title_base_relevant = [k_title for k_title in base_titles if "developer" in k_title.lower() or "engineer" in k_title.lower()]
             title_base = random.choice(title_base_relevant) if title_base_relevant else title_base
        elif "data" in keyword.lower() or "analyst" in keyword.lower() or "scientist" in keyword.lower():
             title_base_relevant = [k_title for k_title in base_titles if "data" in k_title.lower() or "analyst" in k_title.lower() or "scientist" in k_title.lower()]
             title_base = random.choice(title_base_relevant) if title_base_relevant else title_base

        if " " in keyword: title = f"{title_base} ({keyword.split(' ')[0].title()})"
        elif keyword: title = f"{title_base} ({keyword.title()})"
        else: title = f"{title_base}"
        title = f"{title} - {source} Demo #{i+1}"
        company = random.choice(common_companies)
        location = random.choice(common_locations)
        days_ago = random.randint(0, 30)
        if days_ago == 0: date_str = random.choice(["Posted today", "Just posted", f"{random.randint(1,23)} hours ago"])
        elif days_ago == 1: date_str = "1 day ago"
        else: date_str = f"{days_ago} days ago"
        parsed_dt = parse_relative_date(date_str)
        num_skills_to_pick = random.randint(3, 8)
        desc_for_skills = title + " " + keyword + " " + " ".join(random.sample(COMMON_SKILLS, k=min(num_skills_to_pick * 2, len(COMMON_SKILLS))))
        skills_list = extract_skills_from_text(desc_for_skills)[:num_skills_to_pick]
        demo_jobs_list.append({
            'title': title, 'company': company, 'location': location,
            'skills': ", ".join(skills_list) if skills_list else "N/A",
            'date_posted': date_str, 'parsed_date': parsed_dt,
            'source': source, 'search_keyword': keyword,
            'url': f'https://demo.{source.lower()}.com/jobs/view/{random.randint(100000,999999)}?keyword={keyword.replace(" ","")}'
        })
    df_demo = pd.DataFrame(demo_jobs_list)
    logger.info(f"APP_DEMO: Generated demo DataFrame with {len(df_demo)} rows for '{keyword}' from {source}.")
    return df_demo


# --- Streamlit App UI ---
st.set_page_config(layout="wide", page_title="Real-Time Job Trend Analyzer")
st.title("🚀 Real-Time Job Trend Analyzer")
st.markdown("Analyze job market trends by scraping live data or using demo data.")

# --- Session State Initialization ---
if 'last_scraped_keyword' not in st.session_state: st.session_state.last_scraped_keyword = "Python Developer"
if 'last_scraped_location' not in st.session_state: st.session_state.last_scraped_location = "United States"
if 'demo_mode_active' not in st.session_state: st.session_state.demo_mode_active = True
if 'scrape_indeed_full_desc' not in st.session_state: st.session_state.scrape_indeed_full_desc = False

# --- Sidebar Controls ---
st.sidebar.header("⚙️ Scraping Controls")
keyword_input = st.sidebar.text_input("Enter Job Keyword(s)", value=st.session_state.last_scraped_keyword)
location_input = st.sidebar.text_input("Enter Location (e.g., City, Country, Remote)", value=st.session_state.last_scraped_location)

sources_to_scrape = st.sidebar.multiselect(
    "Select Job Portals:",
    options=["LinkedIn", "Indeed"],
    default=["LinkedIn", "Indeed"]
)

st.session_state.demo_mode_active = st.sidebar.checkbox("⚡ Run in Demo Data Mode (Fast, No Live Scraping)", value=st.session_state.demo_mode_active)

if "Indeed" in sources_to_scrape and not st.session_state.demo_mode_active:
    st.session_state.scrape_indeed_full_desc = st.sidebar.checkbox("Scrape Full Indeed Descriptions (Slower, More Skills)", value=st.session_state.scrape_indeed_full_desc, help="Fetches individual job pages from Indeed. Significantly increases scraping time and risk of blocks.")

if "LinkedIn" in sources_to_scrape and not st.session_state.demo_mode_active:
    st.sidebar.warning("ℹ️ LinkedIn scraping attempts unauthenticated access for public job listings. Results can be inconsistent due to LinkedIn's security measures (login walls, CAPTCHAs). It does **not** use your browser's login credentials. Success is not guaranteed and XPaths may need frequent updates in `linkedin_scraper.py`.")


if st.sidebar.button("🔍 Scrape & Analyze Jobs", type="primary", use_container_width=True):
    if not keyword_input.strip():
        st.sidebar.error("Please enter a job keyword.")
    elif not sources_to_scrape:
        st.sidebar.error("Please select at least one job portal.")
    else:
        st.session_state.last_scraped_keyword = keyword_input.strip()
        st.session_state.last_scraped_location = location_input.strip()
        
        all_processed_jobs_dfs = []
        any_errors_during_scrape = False
        progress_bar = st.progress(0)
        status_text_area = st.empty() # For displaying messages
        
        num_sources = len(sources_to_scrape)
        
        for i, source_name in enumerate(sources_to_scrape):
            with status_text_area.container(): # Use a container to manage status messages
                st.info(f"Processing {source_name} for '{st.session_state.last_scraped_keyword}' in '{st.session_state.last_scraped_location}'...")
            jobs_df_source = pd.DataFrame()

            try:
                if source_name == "LinkedIn":
                    if st.session_state.demo_mode_active:
                        jobs_df_source = generate_demo_data(st.session_state.last_scraped_keyword, "LinkedIn", LINKEDIN_DEMO_COUNT)
                        with status_text_area.container(): st.success(f"Generated {len(jobs_df_source)} demo jobs from LinkedIn.")
                    else:
                        with status_text_area.container(): st.info("Scraping LinkedIn... This may take a few minutes. A browser window will open (then close).")
                        jobs_df_source = scrape_linkedin(keyword=st.session_state.last_scraped_keyword, location=st.session_state.last_scraped_location)
                        if not jobs_df_source.empty:
                            with status_text_area.container(): st.success(f"Scraped {len(jobs_df_source)} jobs from LinkedIn.")
                        else:
                            with status_text_area.container(): st.warning("No jobs returned from LinkedIn. This could be due to network issues, LinkedIn blocking unauthenticated requests (login wall/CAPTCHA), outdated XPaths, or no jobs matching your query. Check console logs for details.")
                
                elif source_name == "Indeed":
                    if st.session_state.demo_mode_active:
                        jobs_df_source = generate_demo_data(st.session_state.last_scraped_keyword, "Indeed", INDEED_DEMO_COUNT)
                        with status_text_area.container(): st.success(f"Generated {len(jobs_df_source)} demo jobs from Indeed.")
                    else:
                        with status_text_area.container(): st.info(f"Scraping Indeed {'with full descriptions' if st.session_state.scrape_indeed_full_desc else 'summary only'}...")
                        jobs_df_source = scrape_indeed(
                            keyword=st.session_state.last_scraped_keyword, 
                            location=st.session_state.last_scraped_location,
                            scrape_full_description=st.session_state.scrape_indeed_full_desc
                        )
                        if not jobs_df_source.empty:
                           with status_text_area.container(): st.success(f"Scraped {len(jobs_df_source)} jobs from Indeed.")
                        else:
                            with status_text_area.container(): st.warning("No jobs returned from Indeed. This could be due to network issues, Indeed blocking requests, outdated selectors, or no jobs matching your query. Check console logs.")

                if not jobs_df_source.empty:
                    all_processed_jobs_dfs.append(jobs_df_source)
                
            except Exception as e_scrape:
                logger.error(f"APP: Error during scraping/processing for {source_name}: {e_scrape}", exc_info=True)
                with status_text_area.container(): st.error(f"An error occurred while processing {source_name}: {e_scrape}")
                any_errors_during_scrape = True
            
            progress_bar.progress((i + 1) / num_sources)
            if i < num_sources -1 : # If not the last source, clear for next message
                 # Let messages accumulate or show final after loop
                 pass


        # --- After loop, process results ---
        if all_processed_jobs_dfs:
            final_df = pd.concat(all_processed_jobs_dfs, ignore_index=True)
            if not final_df.empty:
                if 'parsed_date' not in final_df.columns: final_df['parsed_date'] = pd.NaT 
                else: final_df['parsed_date'] = pd.to_datetime(final_df['parsed_date'], errors='coerce')
                if 'search_keyword' not in final_df.columns: final_df['search_keyword'] = st.session_state.last_scraped_keyword
                if 'skills' not in final_df.columns: final_df['skills'] = "N/A"

                logger.info(f"APP: Attempting to store {len(final_df)} combined jobs.")
                num_stored = store_jobs(final_df)
                with status_text_area.container(): 
                    if num_stored > 0 : st.success(f"Successfully processed sources. Stored {num_stored} new unique jobs in the database from this run.")
                    else: st.info("Successfully processed sources. No new unique jobs from this run were stored (possibly already in database or empty results from scrapers).")
            else: # final_df is empty after concat
                with status_text_area.container(): st.warning("No jobs found after combining sources from this run. Check individual scraper logs.")
        elif not any_errors_during_scrape: 
             with status_text_area.container(): st.info("No new job data was scraped or generated from any selected source in this run.")
        else: # No DFs but there were errors
             with status_text_area.container(): st.error("Scraping completed with errors. Some sources might not have provided data. Check messages above and console logs.")
        
        if all_processed_jobs_dfs and any(not df.empty for df in all_processed_jobs_dfs) and not any_errors_during_scrape and num_stored > 0:
            st.balloons()
        
        progress_bar.empty()
        # Do not clear status_text_area here so user sees final message.
        st.rerun() 

# --- Main Display Area ---
st.header("📊 Job Market Insights")
st.markdown("---")

# --- Filter Controls for Display ---
filter_options_base = ["All Stored Jobs"]
try:
    all_db_data_for_filter = fetch_jobs() 
    if not all_db_data_for_filter.empty and 'search_keyword' in all_db_data_for_filter.columns:
        unique_stored_keywords = sorted(all_db_data_for_filter['search_keyword'].dropna().astype(str).unique().tolist())
        filter_options = filter_options_base + unique_stored_keywords # Prepend "All"
    else:
        filter_options = filter_options_base
except Exception as e_fetch_keywords:
    logger.error(f"APP: Error fetching all keywords for filter dropdown: {e_fetch_keywords}")
    st.warning("Could not load all stored keywords for filtering due to an error.")
    filter_options = filter_options_base


# Ensure last scraped keyword is an option if it exists and isn't "All Stored Jobs"
current_keyword_val = st.session_state.last_scraped_keyword
if current_keyword_val and current_keyword_val not in filter_options and current_keyword_val != "All Stored Jobs":
    # Insert it after "All Stored Jobs" if not already present
    if "All Stored Jobs" in filter_options:
        all_index = filter_options.index("All Stored Jobs")
        temp_options = [opt for opt in filter_options if opt != current_keyword_val] # Remove if exists elsewhere
        temp_options.insert(all_index + 1, current_keyword_val)
        filter_options = sorted(list(set(temp_options)), key=lambda x: (x != "All Stored Jobs", x)) # Keep "All" first, then sort
    else: # Should not happen if filter_options_base is used
        filter_options.append(current_keyword_val)
        filter_options = sorted(list(set(filter_options)), key=lambda x: (x != "All Stored Jobs", x))


default_filter_index = 0
if current_keyword_val in filter_options:
    try:
        default_filter_index = filter_options.index(current_keyword_val)
    except ValueError: # If somehow current_keyword_val is not in the list after all
        default_filter_index = 0 


filter_keyword_display = st.selectbox(
    "Filter displayed trends by scraped keyword:",
    options=filter_options,
    index=default_filter_index,
    key="keyword_filter_selectbox" # Add a key for stability if needed
)
st.markdown(f"#### Displaying trends for: **{filter_keyword_display}**")

# --- Fetch Data for Display based on Filter ---
if filter_keyword_display == "All Stored Jobs":
    jobs_df_display = fetch_jobs() # Fetches all jobs
else:
    jobs_df_display = fetch_jobs(keyword=filter_keyword_display) # Fetches for specific keyword

if jobs_df_display.empty:
    if filter_keyword_display == "All Stored Jobs":
        st.info(f"No job data found in the database. Try scraping some jobs first!")
    else:
        st.info(f"No job data found in the database for the keyword '{filter_keyword_display}'. Try scraping for this keyword or select 'All Stored Jobs'.")
else:
    st.subheader(f"📋 Overview: {len(jobs_df_display)} Listings Found")

    # Columns to display in the table.
    display_cols_in_table = ['title', 'company', 'location', 'date_posted', 'source', 'skills', 'url']
    # Ensure all selected display columns actually exist in the fetched DataFrame
    cols_to_show_in_df_view = [col for col in display_cols_in_table if col in jobs_df_display.columns]

    # Make URL clickable
    def make_clickable(link):
        if pd.isna(link) or not isinstance(link, str) or not link.startswith('http'):
            return link
        # Show a shorter, more manageable part of the URL
        display_link = link.split("?")[0]
        if len(display_link) > 50: display_link = display_link[:25] + "..." + display_link[-20:]
        return f'<a href="{link}" target="_blank" rel="noopener noreferrer">{display_link}</a>'

    # Sort jobs_df_display by 'parsed_date' before creating df_view
    if 'parsed_date' in jobs_df_display.columns:
        logger.info("APP_DISPLAY: 'parsed_date' found. Converting to datetime and sorting jobs_df_display.")
        jobs_df_display['parsed_date'] = pd.to_datetime(jobs_df_display['parsed_date'], errors='coerce')
        jobs_df_display = jobs_df_display.sort_values(by='parsed_date', ascending=False, na_position='last')
    else:
        logger.warning("APP_DISPLAY: 'parsed_date' column not found in jobs_df_display. Table will not be sorted by actual date.")

    # Create df_view with selected columns from the (now sorted) jobs_df_display
    df_view = jobs_df_display[cols_to_show_in_df_view].copy()

    if 'url' in df_view.columns:
        df_view['url'] = df_view['url'].apply(make_clickable)
    
    st.markdown(df_view.head(25).to_html(escape=False, index=False), unsafe_allow_html=True)
    st.caption(f"Showing top {min(25, len(df_view))} listings (sorted by recency if date available).")


    # --- Charts ---
    st.markdown("---")
    st.subheader("📈 Visualizations")
    
    # Ensure 'parsed_date' is datetime for charts (might have been done above, but good to re-check)
    if 'parsed_date' in jobs_df_display.columns:
        jobs_df_display['parsed_date'] = pd.to_datetime(jobs_df_display['parsed_date'], errors='coerce')

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("##### 🏆 Top 10 Job Titles")
        if 'title' in jobs_df_display.columns and not jobs_df_display['title'].dropna().empty:
            top_titles = jobs_df_display['title'].str.lower().value_counts().nlargest(10)
            if not top_titles.empty:
                fig_titles = px.bar(top_titles, x=top_titles.values, y=top_titles.index, orientation='h', 
                                    labels={'y': 'Job Title', 'x': 'Listings'}, title="Most Frequent Job Titles")
                fig_titles.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
                st.plotly_chart(fig_titles, use_container_width=True)
            else: st.write("Not enough title data for chart.")
        else: st.write("Title data column missing or empty.")

        st.markdown("##### 🏢 Top 10 Hiring Companies")
        if 'company' in jobs_df_display.columns and not jobs_df_display['company'].dropna().empty:
            top_companies = jobs_df_display['company'].str.title().value_counts().nlargest(10) # .str.title() for consistent casing
            if not top_companies.empty:
                fig_companies = px.bar(top_companies, x=top_companies.index, y=top_companies.values,
                                     labels={'index': 'Company', 'y': 'Openings'}, title="Companies with Most Listings")
                fig_companies.update_layout(height=400)
                st.plotly_chart(fig_companies, use_container_width=True)
            else: st.write("Not enough company data for chart.")
        else: st.write("Company data column missing or empty.")


    with col2:
        st.markdown("##### 🛠️ Top 15 Most Frequent Skills")
        if 'skills' in jobs_df_display.columns and not jobs_df_display['skills'].dropna().empty:
            skill_counts = analyze_skills(jobs_df_display['skills']) 
            if skill_counts and sum(skill_counts.values()) > 0 : # Check if counter has items
                top_skills_df = pd.DataFrame(skill_counts.most_common(15), columns=['Skill', 'Frequency']) 
                if not top_skills_df.empty:
                    fig_skills = px.bar(top_skills_df, x='Frequency', y='Skill', orientation='h', title="Most In-Demand Skills")
                    fig_skills.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
                    st.plotly_chart(fig_skills, use_container_width=True)
                else: st.write("No skills found after analysis for chart.")
            else: st.write("Skill analysis yielded no results for chart (skills column might contain only 'N/A' or be empty).")
        else: st.write("Skills data column missing or empty.")

        st.markdown("##### 🏙️ Top 10 Hiring Locations (excluding Remote)")
        if 'location' in jobs_df_display.columns and not jobs_df_display['location'].dropna().empty:
            try:
                jobs_df_display['location_normalized'] = jobs_df_display['location'].astype(str).str.split(',').str[0].str.strip().str.title()
                non_remote_locations = jobs_df_display[~jobs_df_display['location_normalized'].str.contains("Remote", case=False, na=False)]
                top_cities = non_remote_locations['location_normalized'].value_counts().nlargest(10) 
                
                if not top_cities.empty:
                    fig_cities = px.bar(top_cities, x=top_cities.index, y=top_cities.values, 
                                        labels={'index': 'Location', 'y': 'Openings'}, title="Top Hiring Locations")
                    fig_cities.update_layout(height=400)
                    st.plotly_chart(fig_cities, use_container_width=True)
                else: st.write("Not enough non-remote location data for chart.")
            except Exception as e_loc_chart: 
                st.write(f"Error processing location chart: {e_loc_chart}")
                logger.error(f"APP: Error in location chart: {e_loc_chart}", exc_info=True)
        else: st.write("Location data column missing or empty.")

    st.markdown("---")
    st.subheader("📅 Posting Trends Over Time")
    if 'parsed_date' in jobs_df_display.columns and not jobs_df_display['parsed_date'].dropna().empty:
        temp_df_trends = jobs_df_display.dropna(subset=['parsed_date']).copy()
        if not temp_df_trends.empty:
            # Daily Trends
            posting_trends_daily = temp_df_trends.groupby(temp_df_trends['parsed_date'].dt.date)['title'].count().reset_index()
            posting_trends_daily.rename(columns={'title':'count', 'parsed_date':'date'}, inplace=True)
            posting_trends_daily['date'] = pd.to_datetime(posting_trends_daily['date'])
            posting_trends_daily.sort_values('date', inplace=True)
            
            if not posting_trends_daily.empty and len(posting_trends_daily) > 1:
                fig_trends_d = px.line(posting_trends_daily, x='date', y='count', 
                                     labels={'date': 'Date', 'count': 'Number of Postings'}, title="Job Postings Over Time (Daily)")
                st.plotly_chart(fig_trends_d, use_container_width=True)
            elif not posting_trends_daily.empty: 
                st.write(f"All {posting_trends_daily['count'].iloc[0]} jobs with valid dates were posted on {posting_trends_daily['date'].iloc[0].strftime('%Y-%m-%d')}.")
            else: st.write("Not enough distinct dates for daily posting trends chart.")

            # Weekly Trends
            temp_df_trends['week_start_date'] = temp_df_trends['parsed_date'].dt.to_period('W').apply(lambda r: r.start_time).dt.date
            posting_trends_weekly = temp_df_trends.groupby('week_start_date')['title'].count().reset_index()
            posting_trends_weekly.rename(columns={'title':'count', 'week_start_date':'date'}, inplace=True)
            posting_trends_weekly['date'] = pd.to_datetime(posting_trends_weekly['date'])
            posting_trends_weekly.sort_values('date', inplace=True)

            if not posting_trends_weekly.empty and len(posting_trends_weekly) > 1:
                fig_trends_w = px.line(posting_trends_weekly, x='date', y='count', 
                                     labels={'date': 'Week Starting', 'count': 'Number of Postings'}, title="Job Postings Over Time (Weekly)")
                st.plotly_chart(fig_trends_w, use_container_width=True)
            elif not posting_trends_weekly.empty:
                 st.write(f"All {posting_trends_weekly['count'].iloc[0]} jobs with valid dates were posted in the week starting {posting_trends_weekly['date'].iloc[0].strftime('%Y-%m-%d')}.")
            else: st.write("Not enough distinct weeks for weekly posting trends chart.")
        else: st.write("No valid 'parsed_date' data for trends chart after filtering NaT values.")
    else: st.write("'parsed_date' column missing or empty, cannot display posting trends.")


st.sidebar.markdown("---")
st.sidebar.info("Built with Python, Streamlit, Selenium, Requests, BeautifulSoup & Plotly.")
st.sidebar.markdown("⚠️ **Scraping Note:** Web scraping can be unreliable. Site structures change, and anti-bot measures are common. Results may vary.")