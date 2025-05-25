# utils/data_parser.py
import dateparser
from datetime import datetime, timedelta
import re
import pandas as pd
from collections import Counter

# Common tech skills keywords (expand this list)
COMMON_SKILLS = [
    'python', 'java', 'c++', 'c#', 'javascript', 'typescript', 'html', 'css', 'sql', 'nosql',
    'react', 'angular', 'vue', 'node.js', 'django', 'flask', 'spring', 'ruby on rails',
    'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform', 'ansible',
    'machine learning', 'deep learning', 'data science', 'data analysis', 'artificial intelligence', 'ai',
    'pandas', 'numpy', 'scikit-learn', 'tensorflow', 'pytorch',
    'agile', 'scrum', 'git', 'jira', 'rest', 'api', 'microservices',
    'big data', 'hadoop', 'spark', 'kafka',
    'cybersecurity', 'devops', 'ui/ux', 'figma', 'swift', 'kotlin', 'php', 'go'
]

def parse_relative_date(date_str):
    if not date_str or pd.isna(date_str):
        return None
    parsed = dateparser.parse(date_str)
    if parsed:
        return parsed.date() # Return only the date part
    
    # Custom handling for "X hours/minutes ago" -> today
    if "hour" in date_str.lower() or "minute" in date_str.lower() or "moment" in date_str.lower() or "just posted" in date_str.lower():
        return datetime.now().date()
    
    return None


def extract_skills_from_text(text_blob):
    if not text_blob or pd.isna(text_blob):
        return []
    
    text_blob_lower = text_blob.lower()
    found_skills = set()
    
    # Simple keyword matching
    for skill in COMMON_SKILLS:
        # Use word boundaries to avoid matching substrings like 'java' in 'javascript' if 'javascript' isn't already a skill
        # Or match skill as a whole word, possibly with special characters around it.
        # A more robust regex might be: r'\b' + re.escape(skill) + r'\b'
        # For skills like "C++" or "Node.js", simple containment might be fine if the list is curated
        if skill in text_blob_lower:
            # Handling for "C++" vs "C"
            if skill == 'c++' and 'c++' in text_blob_lower:
                 found_skills.add('c++')
            elif skill == 'c#' and 'c#' in text_blob_lower:
                 found_skills.add('c#')
            elif skill == 'node.js' and 'node.js' in text_blob_lower:
                 found_skills.add('node.js')
            elif skill == 'ruby on rails' and 'ruby on rails' in text_blob_lower:
                 found_skills.add('ruby on rails')
            elif skill not in ['c', 'go'] or re.search(r'\b' + re.escape(skill) + r'\b', text_blob_lower): # Avoid 'c' in 'company'
                found_skills.add(skill.replace('.js', 'js')) # Normalize Node.js to nodejs for consistency
    
    return list(found_skills)

def analyze_skills(skills_series):
    all_skills = []
    for skill_list_str in skills_series.dropna():
        # Skills might be stored as stringified lists: "['python', 'java']"
        try:
            # Attempt to evaluate if it's a string representation of a list
            s_list = eval(skill_list_str) if isinstance(skill_list_str, str) and skill_list_str.startswith('[') else skill_list_str
        except: # If eval fails (e.g. simple comma separated string)
            s_list = skill_list_str
        
        if isinstance(s_list, list):
            all_skills.extend([skill.strip().lower() for skill in s_list])
        elif isinstance(s_list, str): # If it's a comma-separated string
            all_skills.extend([skill.strip().lower() for skill in s_list.split(',')])
            
    return Counter(all_skills)