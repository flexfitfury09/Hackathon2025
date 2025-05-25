# utils/data_parser.py
import dateparser
from datetime import datetime, timedelta
import re
import pandas as pd
from collections import Counter
import logging

logger = logging.getLogger(__name__)

# Common tech skills keywords (expand this list for better accuracy)
COMMON_SKILLS = sorted(list(set([
    'python', 'java', 'c++', 'c#', 'javascript', 'typescript', 'html', 'css', 'sql', 'nosql', 'mongodb', 'postgresql', 'mysql',
    'react', 'angular', 'vue', 'node.js', 'express.js', 'next.js', 'reactjs', 'vuejs', 'angularjs',
    'django', 'flask', 'spring', 'spring boot', '.net', 'ruby on rails', 'asp.net',
    'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'k8s', 'terraform', 'ansible', 'ci/cd', 'jenkins', 'gitlab ci', 'github actions',
    'machine learning', 'ml', 'deep learning', 'dl', 'data science', 'data analysis', 'artificial intelligence', 'ai', 'nlp', 'computer vision',
    'pandas', 'numpy', 'scipy', 'scikit-learn', 'sklearn', 'tensorflow', 'keras', 'pytorch', 'opencv',
    'agile', 'scrum', 'kanban', 'git', 'github', 'jira', 'rest', 'api', 'graphql', 'microservices', 'restful apis',
    'big data', 'hadoop', 'spark', 'kafka', 'data warehousing', 'etl', 'data pipelines', 'airflow',
    'cybersecurity', 'information security', 'penetration testing', 'linux', 'unix', 'bash', 'shell scripting',
    'devops', 'sre', 'ui/ux', 'figma', 'adobe xd', 'sketch', 'swift', 'kotlin', 'php', 'laravel', 'go', 'golang', 'rust', 'scala',
    'power bi', 'tableau', 'excel', 'communication', 'problem solving', 'teamwork', 'leadership', 'project management', # Added some soft skills often mentioned
    'data visualization', 'r', 'statistics', 'algorithms', 'data structures', 'oop', 'object-oriented programming',
    'cloud computing', 'serverless', 'selenium', 'beautifulsoup', 'api development' # General terms
])))


def parse_relative_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    
    current_time = datetime.now()
    date_str_lower = str(date_str).lower().strip()

    # Handle "today", "just posted", etc. directly for speed
    if any(term in date_str_lower for term in ["just posted", "posted today", "active today", "today"]):
        return current_time.date()
    
    # Handle "N days ago" / "N day ago"
    days_ago_match = re.search(r'(\d+)\s+day(s)?\s+ago', date_str_lower)
    if days_ago_match:
        days = int(days_ago_match.group(1))
        return (current_time - timedelta(days=days)).date()

    # Handle "N hours ago" / "N minutes ago"
    if "hour" in date_str_lower or "minute" in date_str_lower or "moment" in date_str_lower :
        return current_time.date() # Count as today

    # Handle "N weeks ago"
    weeks_ago_match = re.search(r'(\d+)\s+week(s)?\s+ago', date_str_lower)
    if weeks_ago_match:
        weeks = int(weeks_ago_match.group(1))
        return (current_time - timedelta(weeks=weeks)).date()

    # Handle "N months ago"
    months_ago_match = re.search(r'(\d+)\s+month(s)?\s+ago', date_str_lower)
    if months_ago_match:
        months = int(months_ago_match.group(1))
        # This is an approximation, timedelta doesn't handle months directly perfectly
        return (current_time - timedelta(days=months * 30)).date()


    try:
        # dateparser is powerful but can be slow; use it as a fallback
        # Provide reference date to help with "yesterday", "last week" etc.
        parsed = dateparser.parse(date_str, settings={'PREFER_DATES_FROM': 'past', 'RELATIVE_BASE': current_time})
        if parsed:
            return parsed.date()
    except Exception as e:
        logger.warning(f"DATA_PARSER: Could not parse date string '{date_str}' with dateparser: {e}")
    
    logger.debug(f"DATA_PARSER: Date string '{date_str}' not parsed by custom rules or dateparser. Returning None.")
    return None


def extract_skills_from_text(text_blob):
    if not text_blob or pd.isna(text_blob):
        return []
    
    text_blob_lower = str(text_blob).lower()
    found_skills = set()
    
    # Simple keyword matching, ensuring whole word matches for shorter skills
    for skill in COMMON_SKILLS:
        # For multi-word skills or skills with special characters, simple 'in' is often fine
        # Ensure skill is not empty
        if not skill:
            continue

        skill_escaped = re.escape(skill)
        try:
            # \b matches word boundaries. This is crucial for skills like 'c', 'r', 'ai', 'ml'.
            # For skills like 'c++', '#', '.net', \b might not work as expected around special chars.
            if skill in ['.net', 'c#', 'c++','node.js', 'express.js', 'react.js', 'vue.js', 'asp.net', 'ui/ux']: # Skills where \b might be tricky
                if skill in text_blob_lower: # Simpler check for these
                    found_skills.add(skill)
            elif re.search(r'\b' + skill_escaped + r'\b', text_blob_lower):
                found_skills.add(skill)
        except re.error: # Handle potential regex errors with special characters in skill names
             if skill_escaped in text_blob_lower: # Fallback for problematic skill names
                found_skills.add(skill)
    
    # Normalize (e.g., node.js -> nodejs) - you can add more normalizations
    normalized_skills = {
        skill.replace('.js', 'js') if '.js' in skill else
        skill.replace('react.js', 'reactjs') if 'react.js' == skill else # Be specific
        skill.replace('vue.js', 'vuejs') if 'vue.js' == skill else
        skill.replace('express.js', 'expressjs') if 'express.js' == skill else
        skill.replace('node.js', 'nodejs') if 'node.js' == skill else
        skill
        for skill in found_skills
    }
    
    return sorted(list(normalized_skills))

def analyze_skills(skills_series):
    """Analyzes a pandas Series of skill strings (comma-separated or list-like) and returns a Counter."""
    all_skills_flat_list = []
    if skills_series is None or skills_series.dropna().empty:
        return Counter()

    for skill_entry in skills_series.dropna():
        current_skills = []
        if isinstance(skill_entry, str):
            # Clean the string: remove brackets, then split by comma
            cleaned_entry = skill_entry.strip()
            if cleaned_entry.startswith('[') and cleaned_entry.endswith(']'):
                 cleaned_entry = cleaned_entry[1:-1] # Remove brackets

            current_skills.extend([s.strip().lower() for s in cleaned_entry.split(',') if s.strip()])

        elif isinstance(skill_entry, list): # Already a list
            current_skills.extend([str(s).strip().lower() for s in skill_entry if str(s).strip()])
        
        # Filter out empty strings that might result from splitting/stripping
        all_skills_flat_list.extend(filter(None, current_skills))
            
    return Counter(all_skills_flat_list)