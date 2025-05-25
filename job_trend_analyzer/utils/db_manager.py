# utils/db_manager.py
import sqlite3
import pandas as pd
import logging
import os

# Determine the root directory of the project
# This assumes db_manager.py is in a 'utils' subdirectory of the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_NAME = os.path.join(PROJECT_ROOT, 'job_listings.db')

logger = logging.getLogger(__name__)

def init_db():
    conn = None
    try:
        # Ensure the directory for the database exists
        db_dir = os.path.dirname(DATABASE_NAME)
        if not os.path.exists(db_dir) and db_dir: # Check if db_dir is not empty string
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"DB_MANAGER: Created directory for database: {db_dir}")

        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                company TEXT,
                location TEXT,
                skills TEXT,            -- Comma-separated string of skills
                date_posted TEXT,       -- The original string like "2 days ago"
                parsed_date DATE,       -- The actual date object after parsing
                source TEXT,            -- e.g., "LinkedIn", "Indeed"
                search_keyword TEXT,    -- The keyword used for the search that found this job
                url TEXT UNIQUE,        -- Job URL, unique to prevent duplicates
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Add index for faster lookups by URL and search_keyword
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs (url);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_search_keyword ON jobs (search_keyword);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_parsed_date ON jobs (parsed_date);")
        conn.commit()
        logger.info(f"DB_MANAGER: Database '{DATABASE_NAME}' initialized successfully.")
    except sqlite3.Error as e:
        logger.error(f"DB_MANAGER: Error initializing database table 'jobs': {e}")
    finally:
        if conn:
            conn.close()

def store_jobs(jobs_df):
    if not isinstance(jobs_df, pd.DataFrame) or jobs_df.empty:
        logger.info("DB_MANAGER: No new jobs to store (input DataFrame is empty or not a DataFrame).")
        return 0

    required_cols = ['title', 'company', 'location', 'skills', 'date_posted', 'parsed_date', 'source', 'search_keyword', 'url']
    for col in required_cols:
        if col not in jobs_df.columns:
            logger.warning(f"DB_MANAGER: Column '{col}' missing in DataFrame for storage. Adding it with default values.")
            if col == 'parsed_date':
                 jobs_df[col] = pd.NaT
            elif col == 'skills':
                 jobs_df[col] = "N/A" # Default for skills
            elif col == 'url':
                logger.error("DB_MANAGER: CRITICAL - 'url' column is missing. Cannot store jobs without URLs.")
                return 0 # Cannot proceed without URLs
            else:
                 jobs_df[col] = None # General default

    conn = None
    inserted_rows = 0
    skipped_due_to_duplicate = 0
    error_rows = 0

    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        
        logger.info(f"DB_MANAGER: Attempting to store {len(jobs_df)} jobs. Keywords in df: {jobs_df['search_keyword'].unique() if 'search_keyword' in jobs_df.columns else 'N/A'}")

        for _, row in jobs_df.iterrows():
            if pd.isna(row.get('url')) or not str(row.get('url')).strip():
                logger.warning(f"DB_MANAGER: Skipping row due to missing or invalid URL. Title: {row.get('title', 'N/A')}")
                error_rows += 1
                continue
            
            # Prepare data for insertion
            # Convert NaT to None for SQLite as it expects NULL for dates
            parsed_date_val = None if pd.isna(row.get('parsed_date')) else str(row.get('parsed_date'))
            
            # Ensure skills is a string
            skills_val = str(row.get('skills', "N/A"))

            insert_query = """
                INSERT INTO jobs (title, company, location, skills, date_posted, parsed_date, source, search_keyword, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            data_tuple = (
                str(row.get('title', 'N/A')),
                str(row.get('company', 'N/A')),
                str(row.get('location', 'N/A')),
                skills_val,
                str(row.get('date_posted', 'Unknown')),
                parsed_date_val,
                str(row.get('source', 'Unknown')),
                str(row.get('search_keyword', 'Unknown')),
                str(row.get('url')).strip()
            )
            
            try:
                cursor.execute(insert_query, data_tuple)
                inserted_rows += 1
            except sqlite3.IntegrityError as e:
                if 'UNIQUE constraint failed: jobs.url' in str(e):
                    logger.debug(f"DB_MANAGER: Job with URL {row.get('url')} already exists. Skipping.")
                    skipped_due_to_duplicate += 1
                else:
                    logger.error(f"DB_MANAGER: IntegrityError inserting job: {e}, URL: {row.get('url')}")
                    error_rows +=1
            except sqlite3.Error as e: # Catch other SQLite errors
                logger.error(f"DB_MANAGER: SQLite error inserting job: {e}, URL: {row.get('url')}, Row data: {row.to_dict()}")
                error_rows += 1
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"DB_MANAGER: General database error during store_jobs: {e}")
        if conn: conn.rollback() # Rollback on general error
    finally:
        if conn:
            conn.close()
            
    logger.info(f"DB_MANAGER: Stored {inserted_rows} new jobs. Skipped {skipped_due_to_duplicate} (duplicates). Errors: {error_rows}.")
    return inserted_rows

def fetch_jobs(keyword=None, source=None): # Added source filter
    conn = None
    df = pd.DataFrame()
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        query = "SELECT id, title, company, location, skills, date_posted, parsed_date, source, search_keyword, url, scraped_at FROM jobs"
        
        conditions = []
        params = []

        if keyword:
            conditions.append("lower(search_keyword) LIKE lower(?)")
            params.append(f'%{keyword}%')
        
        if source:
            conditions.append("lower(source) = lower(?)")
            params.append(source)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY COALESCE(parsed_date, '1970-01-01') DESC, scraped_at DESC"

        logger.info(f"DB_MANAGER_FETCH: Executing query: \"{query}\" with params: {params}")
        df = pd.read_sql_query(query, conn, params=params)
        
        if not df.empty:
            if 'parsed_date' in df.columns:
                df['parsed_date'] = pd.to_datetime(df['parsed_date'], errors='coerce')
            if 'scraped_at' in df.columns:
                df['scraped_at'] = pd.to_datetime(df['scraped_at'], errors='coerce')
    
    except sqlite3.Error as e:
        logger.error(f"DB_MANAGER_FETCH: DatabaseError fetching jobs: {e}. Query: {query}, Params: {params}")
    except Exception as e: # Catch other potential errors during DataFrame processing
        logger.error(f"DB_MANAGER_FETCH: General error fetching/processing jobs: {e}")
    finally:
        if conn:
            conn.close()
    
    logger.info(f"DB_MANAGER_FETCH: Fetched {len(df)} jobs from database for effective keyword: '{keyword if keyword else 'All'}', source: '{source if source else 'All'}'.")
    return df

# Initialize DB when module is loaded.
init_db()