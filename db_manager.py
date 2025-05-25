# utils/db_manager.py
import sqlite3
import pandas as pd
import logging

DATABASE_NAME = 'job_listings.db' # Will be created in the root project directory
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

def init_db():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                company TEXT,
                location TEXT,
                skills TEXT,
                date_posted TEXT,       -- The original string like "2 days ago"
                parsed_date DATE,       -- The actual date object after parsing
                source TEXT,            -- e.g., "LinkedIn", "Indeed"
                search_keyword TEXT,    -- The keyword used for the search that found this job
                url TEXT UNIQUE,        -- Job URL, unique to prevent duplicates
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        logging.info("Database initialized successfully (table 'jobs' created or already exists).")
    except Exception as e:
        logging.error(f"Error initializing database table 'jobs': {e}")
    finally:
        conn.close()

def store_jobs(jobs_df):
    if not isinstance(jobs_df, pd.DataFrame) or jobs_df.empty:
        logging.info("DB_MANAGER: No new jobs to store (input DataFrame is empty or not a DataFrame).")
        return 0

    # Ensure essential columns exist, add them if not (though they should come from scrapers)
    required_cols = ['title', 'company', 'location', 'skills', 'date_posted', 'parsed_date', 'source', 'search_keyword', 'url']
    for col in required_cols:
        if col not in jobs_df.columns:
            logging.warning(f"DB_MANAGER: Column '{col}' missing in DataFrame. Adding it with None values.")
            jobs_df[col] = None # Or appropriate default like '' for text

    conn = sqlite3.connect(DATABASE_NAME)
    inserted_rows = 0
    skipped_due_to_duplicate = 0
    error_rows = 0

    logging.info(f"DB_MANAGER: Attempting to store {len(jobs_df)} jobs. Keywords in df: {jobs_df['search_keyword'].unique() if 'search_keyword' in jobs_df.columns else 'N/A'}")

    for _, row in jobs_df.iterrows():
        # Ensure URL is not None before trying to insert, as it's UNIQUE
        if row.get('url') is None or pd.isna(row.get('url')):
            logging.warning(f"DB_MANAGER: Skipping row due to missing URL. Title: {row.get('title', 'N/A')}")
            error_rows += 1
            continue
        
        # Convert NaT to None for parsed_date if it's NaT, as SQLite expects NULL for dates not properly formatted strings
        if 'parsed_date' in row and pd.isna(row['parsed_date']):
            row['parsed_date'] = None

        try:
            # Convert row to a DataFrame of 1 row to use to_sql's append logic easily
            # This ensures column order matches, etc.
            row_df = pd.DataFrame([row])
            row_df.to_sql('jobs', conn, if_exists='append', index=False)
            inserted_rows += 1
        except sqlite3.IntegrityError as e: # Handles UNIQUE constraint violation for 'url'
            if 'UNIQUE constraint failed: jobs.url' in str(e):
                logging.debug(f"DB_MANAGER: Job with URL {row.get('url')} likely already exists. Skipping.")
                skipped_due_to_duplicate += 1
            else:
                logging.error(f"DB_MANAGER: IntegrityError inserting job: {e}, URL: {row.get('url')}")
                error_rows +=1
        except Exception as e:
            logging.error(f"DB_MANAGER: General error inserting job: {e}, URL: {row.get('url')}, Row data: {row.to_dict()}")
            error_rows += 1
            
    conn.close()
    logging.info(f"DB_MANAGER: Stored {inserted_rows} new jobs. Skipped {skipped_due_to_duplicate} (duplicates). Errors: {error_rows}.")
    return inserted_rows

def fetch_jobs(keyword=None):
    conn = sqlite3.connect(DATABASE_NAME)
    query = "SELECT id, title, company, location, skills, date_posted, parsed_date, source, search_keyword, url FROM jobs" # Select specific columns
    params = []
    if keyword:
        query += " WHERE lower(search_keyword) LIKE lower(?)" 
        params.append(f'%{keyword}%')
    
    query += " ORDER BY COALESCE(parsed_date, '1970-01-01') DESC, scraped_at DESC" # Handle NULL parsed_date for sorting

    df = pd.DataFrame() # Initialize empty DataFrame
    try:
        logging.info(f"DB_MANAGER_FETCH: Executing query: \"{query}\" with params: {params}")
        df = pd.read_sql_query(query, conn, params=params)
        if not df.empty and 'parsed_date' in df.columns:
            # Convert 'parsed_date' from string (if stored as text) or from SQLite's date format to pandas datetime
            df['parsed_date'] = pd.to_datetime(df['parsed_date'], errors='coerce')
    except pd.io.sql.DatabaseError as e: # More specific error for DB issues
        logging.error(f"DB_MANAGER_FETCH: DatabaseError fetching jobs: {e}. Query: {query}, Params: {params}")
    except Exception as e:
        logging.error(f"DB_MANAGER_FETCH: General error fetching jobs: {e}. Query: {query}, Params: {params}")
    finally:
        conn.close()
    
    logging.info(f"DB_MANAGER_FETCH: Fetched {len(df)} jobs from database for effective keyword: '{keyword if keyword else 'All'}'.")
    return df

# Call init_db() when the module is loaded to ensure the table exists.
# This is generally safe. If the app is multi-processed, this might need more careful handling,
# but for Streamlit's single-process model (per user session), this is usually fine.
init_db()