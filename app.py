# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import logging
import random # For demo data

# Import your modules
from scrapers.linkedin_scraper import scrape_linkedin
from scrapers.indeed_scraper import scrape_indeed
from utils.db_manager import store_jobs, fetch_jobs, init_db # init_db is called in db_manager now
from utils.data_parser import parse_relative_date, extract_skills_from_text, analyze_skills, COMMON_SKILLS

# Configure logging for the app
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# Database is initialized when db_manager is imported.
# We can add a check here to ensure it worked or show an error.
try:
    # Test fetch to see if DB is responsive, init_db() should have run on import
    fetch_jobs(keyword="selftest") 
    logging.info("APP: Database connection test successful.")
except Exception as e:
    logging.error(f"APP: Critical error connecting to or querying database. Please check logs. Error: {e}")
    st.error(f"Critical error: Database is not accessible. Please check logs. Error: {e}")
    st.stop()


# --- Constants for Demo Mode ---
LINKEDIN_DEMO_COUNT = 35 
INDEED_DEMO_COUNT = 35   

# --- Demo Data Generation Function ---
def generate_demo_data(keyword, source, count):
    # ... (generate_demo_data function from the previous app.py response - ensure it's complete and correct)
    logging.info(f"APP_DEMO: Generating {count} demo jobs for {source} with keyword '{keyword}'")
    demo_jobs_list = []
    base_titles = ["Software Engineer", "Data Analyst", "Product Manager", "UX Designer", "DevOps Engineer", "Project Manager", "Data Scientist", "Business Analyst", "QA Tester", "Frontend Developer", "Backend Developer", "Full Stack Developer", "Cloud Engineer", "Security Analyst"]
    common_companies = ["Innovate Corp", "Tech Solutions Inc.", "Analytics Co", "AI Driven LLC", "BigTech Co", "Startup X", "Consulting Firm Y", "Data Insights Z", "Global Corp", "Local Biz", "Future Systems", "NextGen Software", "Synergy Systems", "Digital Frontier"]
    common_locations = ["New York, NY", "San Francisco, CA", "Chicago, IL", "Austin, TX", "Remote", "Boston, MA", "Seattle, WA", "Los Angeles, CA", "Toronto, ON", "Vancouver, BC", "London, UK", "Berlin, Germany", "Paris, FR", "Amsterdam, NL", "Dublin, IE"]
    
    for i in range(count):
        # Make titles more diverse and relevant to the keyword
        title_base = random.choice(base_titles)
        if keyword.lower() in ["python developer", "software engineer", "backend developer", "frontend developer", "full stack developer", "java developer", "c# developer"]:
            title_base_relevant = [k for k in base_titles if "developer" in k.lower() or "engineer" in k.lower()]
            title_base = random.choice(title_base_relevant) if title_base_relevant else title_base
            title = f"{title_base} ({keyword.split(' ')[0].title()})" if " " in keyword else f"{title_base} ({keyword.title()})"
        elif "data analyst" in keyword.lower() or "data scientist" in keyword.lower() or "business analyst" in keyword.lower():
            title_base_relevant = [k for k in base_titles if "data" in k.lower() or "analyst" in k.lower()]
            title_base = random.choice(title_base_relevant) if title_base_relevant else title_base
            title = f"{title_base}"
        else: # Generic keyword
            title = f"{title_base} (focus: {keyword.title()})"
        
        title = f"{title} - {source} Demo #{i+1}"

        company = common_companies[i % len(common_companies)]
        location = common_locations[i % len(common_locations)]
        days_ago = random.randint(0, 28) # 0 for "today"
        if days_ago == 0: date_str = "Posted today"
        elif days_ago == 1: date_str = "1 day ago"
        else: date_str = f"{days_ago} days ago"
        parsed_dt = parse_relative_date(date_str)
        
        num_skills_to_pick = random.randint(3, 7)
        desc_for_skills = title + " " + keyword + " " + " ".join(random.sample(COMMON_SKILLS, k=min(num_skills_to_pick, len(COMMON_SKILLS))))
        skills_list = extract_skills_from_text(desc_for_skills)
        
        demo_jobs_list.append({
            'title': title,
            'company': company,
            'location': location,
            'skills': ", ".join(skills_list) if skills_list else "N/A",
            'date_posted': date_str,
            'parsed_date': parsed_dt,
            'source': source,
            'search_keyword': keyword, 
            'url': f'https://{source.lower()}.com/jobs/view/demo{source.lower()}{i+1}{keyword.replace(" ","").replace("-","")}{random.randint(10000,99999)}' # More unique URL
        })
    
    df_demo = pd.DataFrame(demo_jobs_list)
    logging.info(f"APP_DEMO: Generated demo DataFrame with {len(df_demo)} rows for '{keyword}' from {source}.")
    if not df_demo.empty:
        logging.debug(f"APP_DEMO: Sample of generated data for {keyword} from {source}:\n{df_demo.head().to_string()}")
        logging.info(f"APP_DEMO: Search keywords in generated demo data from {source}: {df_demo['search_keyword'].unique()}")
    return df_demo


# --- Streamlit App ---
st.set_page_config(layout="wide", page_title="Real-Time Job Trend Analyzer")
st.title("🚀 Real-Time Job Trend Analyzer")

if 'last_scraped_keyword' not in st.session_state:
    st.session_state.last_scraped_keyword = "Python Developer" 
if 'demo_mode_active' not in st.session_state:
    st.session_state.demo_mode_active = True 

st.sidebar.header("Scraping Controls")
keyword_input = st.sidebar.text_input("Enter Job Keyword", value=st.session_state.last_scraped_keyword)
location_input = st.sidebar.text_input("Enter Location", "United States")

sources_to_scrape = st.sidebar.multiselect(
    "Select Job Portals to Scrape:",
    options=["LinkedIn", "Indeed"],
    default=["LinkedIn", "Indeed"]
)

st.session_state.demo_mode_active = st.sidebar.checkbox("Run in Demo Data Mode (Faster, No Live Scraping)", value=st.session_state.demo_mode_active)

if st.sidebar.button("🔍 Scrape & Analyze Jobs"):
    # ... (Button click logic from the previous app.py response - ensure it's complete and correct)
    if not keyword_input.strip():
        st.sidebar.error("Please enter a job keyword.")
    elif not sources_to_scrape:
        st.sidebar.error("Please select at least one job portal to scrape.")
    else:
        st.session_state.last_scraped_keyword = keyword_input 
        total_jobs_processed_this_run = 0
        
        with st.spinner(f"Processing jobs for '{keyword_input}' in '{location_input}'..."):
            all_processed_jobs_dfs = [] 

            if "LinkedIn" in sources_to_scrape:
                st.info(f"APP: Processing LinkedIn for '{keyword_input}'...")
                if st.session_state.demo_mode_active:
                    linkedin_jobs_df = generate_demo_data(keyword_input, "LinkedIn", LINKEDIN_DEMO_COUNT)
                    st.success(f"APP: Generated {len(linkedin_jobs_df)} demo jobs from LinkedIn.")
                else:
                    linkedin_jobs_df = scrape_linkedin(keyword=keyword_input, location=location_input)
                    st.success(f"APP: Scraped {len(linkedin_jobs_df)} jobs from LinkedIn.")
                if not linkedin_jobs_df.empty:
                    all_processed_jobs_dfs.append(linkedin_jobs_df)

            if "Indeed" in sources_to_scrape:
                st.info(f"APP: Processing Indeed for '{keyword_input}'...")
                if st.session_state.demo_mode_active:
                    indeed_jobs_df = generate_demo_data(keyword_input, "Indeed", INDEED_DEMO_COUNT)
                    st.success(f"APP: Generated {len(indeed_jobs_df)} demo jobs from Indeed.")
                else:
                    indeed_jobs_df = scrape_indeed(keyword=keyword_input, location=location_input)
                    st.success(f"APP: Scraped {len(indeed_jobs_df)} jobs from Indeed.")
                if not indeed_jobs_df.empty:
                    all_processed_jobs_dfs.append(indeed_jobs_df)

            if all_processed_jobs_dfs:
                final_df = pd.concat(all_processed_jobs_dfs, ignore_index=True)
                if not final_df.empty:
                    if 'parsed_date' in final_df.columns:
                        final_df['parsed_date'] = pd.to_datetime(final_df['parsed_date'], errors='coerce')
                    else:
                        logging.warning("APP: 'parsed_date' column missing from combined DataFrame during storage.")
                        final_df['parsed_date'] = pd.NaT 
                    
                    if 'search_keyword' not in final_df.columns:
                         logging.warning("APP: 'search_keyword' column missing in final_df. Assigning current input keyword.")
                         final_df['search_keyword'] = keyword_input 

                    logging.info(f"APP: Attempting to store {len(final_df)} combined jobs. Keywords: {final_df['search_keyword'].unique() if 'search_keyword' in final_df.columns else 'N/A'}")
                    num_stored = store_jobs(final_df) # Call to db_manager
                    st.success(f"APP: Stored {num_stored} new unique jobs in the database.")
                    total_jobs_processed_this_run = len(final_df) 
                else:
                    st.warning("APP: No jobs found after combining sources.")
            else:
                st.warning("APP: No data was scraped or generated from any selected source.")
        
        if total_jobs_processed_this_run > 0:
            st.balloons()
        st.rerun() 


st.header("📊 Job Market Insights")

# --- Filter Display Controls ---
filter_options = ["All Stored Jobs"]
try:
    all_db_data = fetch_jobs() 
    if not all_db_data.empty and 'search_keyword' in all_db_data.columns:
        unique_stored_keywords = sorted(all_db_data['search_keyword'].dropna().astype(str).unique().tolist())
        filter_options.extend(unique_stored_keywords)
except Exception as e_fetch_all_keywords:
    logging.error(f"APP: Error fetching all keywords for filter dropdown: {e_fetch_all_keywords}")

current_sidebar_keyword = keyword_input.strip()
if st.session_state.last_scraped_keyword and st.session_state.last_scraped_keyword not in filter_options:
    filter_options.append(st.session_state.last_scraped_keyword)
if current_sidebar_keyword and current_sidebar_keyword not in filter_options: 
    filter_options.append(current_sidebar_keyword)

filter_options = ["All Stored Jobs"] + sorted(list(set(opt for opt in filter_options if opt != "All Stored Jobs")))

default_filter_val = st.session_state.last_scraped_keyword
if default_filter_val not in filter_options : 
    default_filter_val = "All Stored Jobs"
try:
    default_idx = filter_options.index(default_filter_val)
except ValueError:
    default_idx = 0 

filter_keyword_display = st.selectbox(
    "Filter displayed trends by keyword:",
    options=filter_options,
    index=default_idx
)
st.markdown(f"Displaying trends for: **{filter_keyword_display}**")

# --- Fetch Data for Display ---
if filter_keyword_display == "All Stored Jobs":
    jobs_df_display = fetch_jobs()
else:
    jobs_df_display = fetch_jobs(keyword=filter_keyword_display)

if jobs_df_display.empty:
    st.warning(f"No job data found in the database for '{filter_keyword_display}'. Try scraping or check demo mode.")
else:
    # ... (Data display and charting logic from the previous app.py response - ensure it's complete and correct)
    st.subheader(f"Overview ({len(jobs_df_display)} listings found for '{filter_keyword_display}')")
    display_cols = ['title', 'company', 'location', 'date_posted', 'source', 'skills', 'url']
    cols_to_show_in_df = [col for col in display_cols if col in jobs_df_display.columns]
    st.dataframe(jobs_df_display[cols_to_show_in_df].head(25))

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏆 Top 10 Most In-Demand Job Titles")
        if 'title' in jobs_df_display.columns and not jobs_df_display['title'].dropna().empty:
            top_titles = jobs_df_display['title'].str.lower().value_counts().nlargest(10)
            if not top_titles.empty:
                fig_titles = px.bar(top_titles, x=top_titles.values, y=top_titles.index, orientation='h', labels={'y': 'Job Title', 'x': 'Listings'})
                fig_titles.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_titles, use_container_width=True)
            else: st.write("Not enough title data.")
        else: st.write("Title data column missing or empty.")

        st.subheader("🏙️ Top 10 Hiring Cities")
        if 'location' in jobs_df_display.columns and not jobs_df_display['location'].dropna().empty:
            try:
                jobs_df_display['location_normalized'] = jobs_df_display['location'].astype(str).str.split(',').str[0].str.strip().str.title()
                top_cities = jobs_df_display[jobs_df_display['location_normalized'] != 'Remote']['location_normalized'].value_counts().nlargest(10) 
                if not top_cities.empty:
                    fig_cities = px.bar(top_cities, x=top_cities.index, y=top_cities.values, labels={'index': 'City', 'y': 'Openings'})
                    st.plotly_chart(fig_cities, use_container_width=True)
                else: st.write("Not enough city data (excluding Remote).")
            except Exception as e_loc_chart: st.write(f"Error processing location chart: {e_loc_chart}")
        else: st.write("Location data column missing or empty.")

    with col2:
        st.subheader("🛠️ Top 15 Most Frequent Skills")
        if 'skills' in jobs_df_display.columns and not jobs_df_display['skills'].dropna().empty:
            skill_counts = analyze_skills(jobs_df_display['skills']) 
            if skill_counts:
                top_skills_df = pd.DataFrame(skill_counts.most_common(15), columns=['Skill', 'Frequency']) 
                if not top_skills_df.empty:
                    fig_skills = px.bar(top_skills_df, x='Frequency', y='Skill', orientation='h')
                    fig_skills.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig_skills, use_container_width=True)
                else: st.write("No skills found after analysis.")
            else: st.write("Skill analysis yielded no results.")
        else: st.write("Skills data column missing or empty.")

        st.subheader("📈 Posting Trends Over Time (Daily)")
        if 'parsed_date' in jobs_df_display.columns and not jobs_df_display['parsed_date'].dropna().empty:
            temp_df_trends = jobs_df_display.copy() # Work on a copy
            temp_df_trends['parsed_date'] = pd.to_datetime(temp_df_trends['parsed_date'], errors='coerce')
            valid_dates_df = temp_df_trends.dropna(subset=['parsed_date'])
            if not valid_dates_df.empty:
                posting_trends = valid_dates_df.groupby(valid_dates_df['parsed_date'].dt.date)['title'].count().reset_index()
                posting_trends.rename(columns={'title':'count', 'parsed_date':'date'}, inplace=True)
                posting_trends['date'] = pd.to_datetime(posting_trends['date']) # Ensure it's datetime for plotly
                posting_trends.sort_values('date', inplace=True)
                
                if not posting_trends.empty and len(posting_trends) > 1:
                    fig_trends = px.line(posting_trends, x='date', y='count', labels={'date': 'Date', 'count': 'Postings'})
                    st.plotly_chart(fig_trends, use_container_width=True)
                elif not posting_trends.empty: 
                    st.write(f"All {posting_trends['count'].iloc[0]} jobs found were posted on {posting_trends['date'].iloc[0].strftime('%Y-%m-%d')}.")
                else: st.write("Not enough distinct dates for posting trends.")
            else: st.write("No valid 'parsed_date' data for trends after conversion.")
        else: st.write("'parsed_date' column missing, empty, or all invalid.")

st.sidebar.markdown("---")
st.sidebar.markdown("Built by [Your Name/Group Here]") 
st.sidebar.markdown("Powered by Python, Selenium, Streamlit & Plotly.")