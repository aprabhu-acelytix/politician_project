import os
import time
import requests
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from fuzzywuzzy import fuzz

load_dotenv()

FEC_API_KEY = os.getenv('FEC_API_KEY')
DB_URL = os.getenv('DB_URL')

# FEC endpoint for searching for candidates
FEC_SEARCH_URL = "https://api.open.fec.gov/v1/candidates/search/"

try:
    engine = create_engine(DB_URL)
    print("Database connection successful.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

# Get politicians from DB
def get_politicians_to_link():
    """
    Fetches politicians from our DB from table: politicians who are missing an FEC ID.
    """
    print("Fetching politicians missing FEC IDs from local database...")
    politicians = []
    with engine.connect() as conn:
        # Only get politicians where the ID is NULL (all of them)
        result = conn.execute(sqlalchemy.text(
            "SELECT politician_id, first_name, last_name, state, party FROM politicians WHERE fec_candidate_id IS NULL"
        ))
        for row in result:
            politicians.append({
                "db_id": row[0],
                "first": row[1],
                "last": row[2],
                "state": row[3],
                "party_short": row[4] # 'D', 'R', 'I'
            })
    print(f"Found {len(politicians)} politicians to link.")
    return politicians

# Link FEC ID
def find_and_link_fec_id(politician):
    """
    Searches the FEC API for a candidate and updates our database.
    """
    
    # Format the party name for the FEC API
    party_full = None
    if politician['party_short'] == 'Democratic':
        party_full = 'DEM'
    elif politician['party_short'] == 'Republican':
        party_full = 'REP'
    elif politician['party_short'] == 'Independent':
        party_full = 'IND'
        
    full_name = f"{politician['first']} {politician['last']}"
    
    # Extract - Search FEC API by name
    params = {
        "api_key": FEC_API_KEY,
        "q": full_name,
        "state": politician['state'],
        "party": party_full
    }
    
    response = requests.get(FEC_SEARCH_URL, params=params)
    
    if response.status_code == 429:
        print("Rate limit hit. Pausing for 1 hour...")
        time.sleep(3601) # Wait for one hour and one second
        # Retry the request
        response = requests.get(FEC_SEARCH_URL, params=params)

    if response.status_code != 200:
        print(f"  - API Error for {full_name}: {response.status_code}")
        return None

    results = response.json().get('results', [])
    
    if not results:
        print(f"  - No results for {full_name} ({party_full}, {politician['state']})")
        return None

    # Transform - Find the best match
    best_match = None
    highest_score = 0
    
    for candidate in results:
        # Use fuzzy matching to find the best name
        fec_name = candidate.get('name', '')
        score = fuzz.token_sort_ratio(full_name, fec_name)
        
        if score > highest_score and score > 80: # Set a high confidence threshold
            highest_score = score
            best_match = candidate
            
    if not best_match:
        print(f"  - No *confident* match for {full_name}. Best score was {highest_score}.")
        return None

    fec_id = best_match.get('candidate_id')
    if not fec_id:
        return None

    # Load - Update politicians table
    with engine.connect() as conn:
        with conn.begin() as transaction:
            stmt = sqlalchemy.text(
                "UPDATE politicians SET fec_candidate_id = :fec_id WHERE politician_id = :db_id"
            )
            conn.execute(stmt, {"fec_id": fec_id, "db_id": politician['db_id']})
    
    return fec_id

if __name__ == "__main__":
    politicians_list = get_politicians_to_link()
    
    if not politicians_list:
        print("All politicians already have an FEC ID. Nothing to do.")
        exit()
        
    print(f"--- Starting to link {len(politicians_list)} politicians ---")
    
    updated_count = 0
    failed_count = 0

    for i, politician in enumerate(politicians_list):
        try:
            fec_id = find_and_link_fec_id(politician)
            
            if fec_id:
                print(f"  ({i+1}/{len(politicians_list)}) SUCCESS: Linked {politician['last']} to {fec_id}")
                updated_count += 1
            else:
                print(f"  ({i+1}/{len(politicians_list)}) FAILED: Could not find match for {politician['last']}")
                failed_count += 1

            # Pause for 1 second between requests
            time.sleep(1) 
            
        except Exception as e:
            print(f"CRITICAL ERROR on {politician['last']}: {e}")
            failed_count += 1
            pass
            
    print("\n--- Linking Summary ---")
    print(f"Successfully updated: {updated_count}")
    print(f"Failed to find match: {failed_count}")