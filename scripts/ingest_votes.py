import os
import json
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
import time

# Set up environment and DB connection
load_dotenv()
DB_URL = os.getenv('DB_URL')

# Set the path to downloaded 'congress' repo data
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
CONGRESS_DATA_DIR = os.path.join(BASE_PROJECT_DIR, '..', 'congress', 'congress', 'data')

# Processing 118th and 119th Congress votes for relevant years
YEARS_TO_PROCESS = {
    '118': ['2023', '2024'],
    '119': ['2025']
}

try:
    engine = create_engine(DB_URL)
    print("Database connection successful.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

# HELPER FUNCTIONS: Load our existing data into maps

def get_politician_map():
    """
    Fetches all politicians from our DB and creates a map
    of {bioguideId (congress_id) -> politician_id} for fast lookups.
    """
    print("Fetching politician map from database...")
    politician_map = {}
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(
            "SELECT politician_id, congress_id FROM politicians WHERE congress_id IS NOT NULL"
        ))
        for row in result:
            # The JSON 'id' field is a string, so we store the key as a string
            politician_map[str(row.congress_id)] = row.politician_id
    print(f"Loaded {len(politician_map)} politicians into map.")
    return politician_map

def get_bill_map():
    """
    Fetches all bills from our DB and creates a map
    of {official_bill_number-congress -> bill_id} for fast lookups.
    """
    print("Fetching bill map from database...")
    bill_map = {}
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(
            "SELECT bill_id, official_bill_number, congress FROM bills"
        ))
        for row in result:
            # Create a key like "HR3076-117"
            composite_key = f"{row.official_bill_number.upper()}-{row.congress}"
            bill_map[composite_key] = row.bill_id
    print(f"Loaded {len(bill_map)} bills into map.")
    return bill_map

# MAIN ETL FUNCTION

def scan_and_load_votes(politician_map, bill_map):
    """
    Scans the downloaded JSON files, parses them,
    and loads them into the 'votes' table.
    This version has a more granular try/except block to handle "VP" votes.
    """
    
    votes_table = sqlalchemy.Table('votes', sqlalchemy.MetaData(), autoload_with=engine)
    total_votes_inserted = 0
    total_votes_processed = 0
    total_files_scanned = 0
    
    print(f"Scanning for vote data in: {CONGRESS_DATA_DIR}")

    with engine.connect() as conn:
        for congress, years in YEARS_TO_PROCESS.items():
            for year in years:
                year_path = os.path.join(CONGRESS_DATA_DIR, congress, 'votes', year)
                if not os.path.exists(year_path):
                    print(f"Warning: Directory not found, skipping: {year_path}")
                    continue
                
                print(f"\n--- Scanning for votes in: {year_path} ---")
                
                for root, dirs, files in os.walk(year_path):
                    for file in files:
                        if file == 'data.json':
                            total_files_scanned += 1
                            file_path = os.path.join(root, file)
                            
                            votes_to_insert_batch = [] # Holds all valid votes from this file
                            
                            try:
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    vote_data = json.load(f)
                                
                                # --- 1. VALIDATE AND GET BILL_ID ---
                                vote_category = vote_data.get('category')
                                
                                # Skip nominations or votes not tied to a bill
                                if vote_category == 'nomination' or not vote_data.get('bill'):
                                    continue
                                
                                bill_obj = vote_data.get('bill')
                                bill_type = bill_obj.get('type').upper()
                                bill_number = bill_obj.get('number')
                                bill_congress = bill_obj.get('congress')
                                
                                bill_key = f"{bill_type}{bill_number}-{bill_congress}"
                                bill_id = bill_map.get(bill_key)
                                
                                if not bill_id:
                                    continue # Skip if this vote isn't for a bill in our DB
                                
                                # --- 2. PREPARE VOTES ---
                                vote_date = vote_data.get('date')
                                
                                for vote_position, voters in vote_data.get('votes', {}).items():
                                    for voter in voters:
                                        
                                        # --- NEW GRANULAR TRY/EXCEPT ---
                                        # This try/except is for *each voter*, not the whole file.
                                        try:
                                            # Check if 'voter' is a dictionary. If it's a string (like "VP"), skip it.
                                            if not isinstance(voter, dict): 
                                                continue # Skips "VP"
                                            
                                            bioguide_id = voter.get('id') 
                                            politician_db_id = politician_map.get(bioguide_id)
                                            
                                            if politician_db_id:
                                                votes_to_insert_batch.append({
                                                    'politician_id': politician_db_id,
                                                    'bill_id': bill_id,
                                                    'date': vote_date,
                                                    'vote_position': vote_position,
                                                    'vote_category': vote_category
                                                })
                                                total_votes_processed += 1
                                        
                                        except Exception as e_inner:
                                            # This will catch the "VP" string error
                                            print(f"  SKIPPING VOTER: Error in {file_path}: {e_inner}")
                                            pass # Skip this single voter and continue
                                        # --- END NEW ---
                                
                            except Exception as e_outer:
                                # This catches file-level errors (e.g., bad JSON)
                                print(f"  SKIPPING FILE {file_path}: {e_outer}")
                                pass # Log and continue to the next file
                            
                            # --- 3. BATCH INSERT (MOVED OUTSIDE FILE-LEVEL TRY) ---
                            # We insert all the valid votes we found in this file
                            if votes_to_insert_batch:
                                try:
                                    with conn.begin() as transaction:
                                        conn.execute(votes_table.insert(), votes_to_insert_batch)
                                        total_votes_inserted += len(votes_to_insert_batch)
                                except Exception as e_db:
                                    # This would catch a DB-level error (like a data type mismatch)
                                    print(f"  ERROR batch inserting votes: {e_db}")
                                    pass # Log and continue

            print(f"  Finished processing {congress}. Total votes inserted so far: {total_votes_inserted}")

    print("\n--- Vote ETL Complete ---")
    print(f"Total files scanned: {total_files_scanned}")
    print(f"Total individual votes processed: {total_votes_processed}")
    print(f"Total new individual votes inserted: {total_votes_inserted}")

# RUN THE SCRIPT
if __name__ == "__main__":
    start_time = time.time()

    # TRUNCATE the 'votes' table to avoid duplicates
    # if we ever re-run this script.
    print("Clearing old vote data (TRUNCATE)...")
    with engine.connect() as conn:
        with conn.begin() as transaction:
            conn.execute(sqlalchemy.text("TRUNCATE TABLE votes RESTART IDENTITY"))
    print("Old vote data cleared.")

    # Step 1: Build the map of our politicians
    politician_map = get_politician_map()
    
    # Step 2: Build the map of our bills
    bill_map = get_bill_map()
    
    # Step 3: Run the main ETL
    if politician_map and bill_map:
        scan_and_load_votes(politician_map, bill_map)
    else:
        print("Error: Could not load politician or bill maps from database.")
    
    end_time = time.time()
    print(f"Total time taken: {time.time() - start_time:.2f} seconds")