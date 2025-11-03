import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from fuzzywuzzy import fuzz
import time

# Configuration and Setup

load_dotenv()
DB_URL = os.getenv('DB_URL')

# Define data files are located in the 'data' directory
DATA_DIR = '../data'
CANDIDATE_MASTER_2024 = os.path.join(DATA_DIR, '2024/cn.txt')
CANDIDATE_MASTER_2026 = os.path.join(DATA_DIR, '2026/cn.txt')

# These are the official column names from the FEC's 'cn_header.txt' file
CANDIDATE_COLUMNS = [
    'CAND_ID', 'CAND_NAME', 'CAND_PTY_AFFILIATION', 'CAND_ELECTION_YR', 
    'CAND_OFFICE_ST', 'CAND_OFFICE', 'CAND_OFFICE_DISTRICT', 'CAND_ICI', 
    'CAND_STATUS', 'CAND_PCC', 'CAND_ST1', 'CAND_ST2', 'CAND_CITY', 
    'CAND_ST', 'CAND_ZIP'
]

try:
    engine = create_engine(DB_URL)
    print("Database connection successful.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

def normalize_name(name_str):
    """
    Cleans a name string for a more accurate comparison.
    - Converts to uppercase
    - Removes periods and commas
    - Strips whitespace
    """
    if name_str is None:
        return ""
    return name_str.upper().replace('.', '').replace(',', '').strip()

# Extract - Load all data sources (with FEC name parsing)
def parse_fec_name(fec_name_str):
    """
    Parses the FEC's 'LAST, FIRST MI' format.
    """
    if ', ' in fec_name_str:
        # Split only on the first comma, just like we did before
        parts = fec_name_str.split(', ', 1) 
        return parts[0].strip(), parts[1].strip() # (last_name, first_name)
    else:
        # Failsafe for names with no comma
        return fec_name_str.strip(), "" # (last_name, empty first_name)

def load_all_data():
    """
    Loads data from our PostgreSQL DB and the FEC text files into DataFrames.
    """
    print("Loading politicians from database...")
    db_politicians_df = pd.read_sql_table('politicians', con=engine)
    print(f"  Loaded {len(db_politicians_df)} politicians from DB.")
    
    print("Loading FEC Candidate Master files (cn.txt)...")
    try:
        cn_2024_df = pd.read_csv(
            CANDIDATE_MASTER_2024, sep='|', header=None, 
            names=CANDIDATE_COLUMNS, dtype=str
        )
        cn_2026_df = pd.read_csv(
            CANDIDATE_MASTER_2026, sep='|', header=None, 
            names=CANDIDATE_COLUMNS, dtype=str
        )
        fec_candidates_df = pd.concat([cn_2024_df, cn_2026_df])
        
        fec_candidates_df = fec_candidates_df[
            fec_candidates_df['CAND_OFFICE'].isin(['H', 'S'])
        ]
        fec_candidates_df.drop_duplicates(subset=['CAND_ID'], keep='last', inplace=True)
        
        print(f"  Loaded {len(fec_candidates_df)} unique H/S candidates from FEC files.")
        
    except FileNotFoundError as e:
        print(f"ERROR: File not found. Make sure {e.filename} is in your /data folder.")
        exit()
    except Exception as e:
        print(f"Error loading FEC files: {e}")
        exit()
        
    return db_politicians_df, fec_candidates_df

# Transform - Match politicians to FEC IDs
def transform_and_link(db_df, fec_df):
    """
    Matches politicians from our DB to the FEC data by comparing
    normalized *full* name strings.
    Returns a list of dictionaries ready for database update.
    """
    print("\n--- Starting to link politicians... This may take a few minutes. ---")
    
    politicians_to_update = []
    
    for index, politician in db_df.iterrows():
        if pd.notna(politician['fec_candidate_id']):
            continue

        # --- Rebuild our DB name into the "LAST, FIRST" format ---
        db_full_name = f"{politician['last_name']}, {politician['first_name']}"
        
        # --- Normalize our DB name ---
        # "King, Angus S., Jr." -> "KING ANGUS S JR"
        db_name_norm = normalize_name(db_full_name)

        potential_matches = fec_df[fec_df['CAND_OFFICE_ST'] == politician['state']]
        
        if potential_matches.empty:
            continue

        best_match = None
        highest_score = 0
        
        for _, fec_candidate in potential_matches.iterrows():
            # --- Normalize the FEC name ---
            # "KING, ANGUS STANLEY JR" -> "KING ANGUS STANLEY JR"
            fec_name_norm = normalize_name(fec_candidate['CAND_NAME'])

            # --- Compare the two normalized strings ---
            score = fuzz.token_sort_ratio(db_name_norm, fec_name_norm)
            
            # We use a threshold of 85 for this more forgiving comparison
            if score > 85 and score > highest_score:
                highest_score = score
                best_match = fec_candidate

        if best_match is not None:
            print(f"  MATCH ({highest_score}%): {db_full_name} ==> {best_match['CAND_NAME']}")
            
            politicians_to_update.append({
                'db_id': politician['politician_id'],
                'fec_cand_id': best_match['CAND_ID'],
                'fec_comm_id': best_match['CAND_PCC'] 
            })
        else:
             print(f"  NO MATCH: {db_name_norm} (State: {politician['state']})")

    return politicians_to_update

# Load - Update database with new links
def load_links_to_db(update_list):
    """
    Updates the 'politicians' table ONE BY ONE to handle
    potential duplicate key collisions gracefully.
    """
    if not update_list:
        print("\nNo new politicians to link.")
        return

    print(f"\nFound {len(update_list)} new links. Updating database one-by-one...")
    
    updated_count = 0
    collision_count = 0
    
    # We open a single connection for all operations
    with engine.connect() as conn:
        politicians_table = sqlalchemy.Table('politicians', sqlalchemy.MetaData(), autoload_with=engine)
        
        # Loop through the list of 675 updates
        for item in update_list:
            # Each update is wrapped in its own error handler
            try:
                # We use a transaction for each update
                with conn.begin() as transaction:
                    stmt = politicians_table.update().where(
                        politicians_table.c.politician_id == item['db_id']
                    ).values(
                        fec_candidate_id=item['fec_cand_id'],
                        fec_committee_id=item['fec_comm_id']
                    )
                    conn.execute(stmt)
                    updated_count += 1
                    
            except sqlalchemy.exc.IntegrityError as e:
                # This catches the 'UniqueViolation'
                if "violates unique constraint" in str(e):
                    print(f"  COLLISION: FEC ID {item['fec_cand_id']} already linked to another record. Skipping politician {item['db_id']}.")
                    collision_count += 1
                else:
                    # Some other unexpected database error
                    print(f"  DB ERROR on {item['db_id']}: {e}")
                    collision_count += 1 # Count it as failed
            
            except Exception as e:
                print(f"  SCRIPT ERROR on {item['db_id']}: {e}")
                collision_count += 1 # Count it as failed

    print("\n--- Load Summary ---")
    print(f"Successfully linked: {updated_count}")
    print(f"Skipped (collisions): {collision_count}")
    print(f"Total processed: {updated_count + collision_count}")

if __name__ == "__main__":
    start_time = time.time()
    
    # Step 1: Extract
    db_politicians, fec_candidates = load_all_data()
    
    # Step 2: Transform
    links = transform_and_link(db_politicians, fec_candidates)
    
    # Step 3: Load
    load_links_to_db(links)
    
    end_time = time.time()
    print(f"\n--- ETL Complete ---")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")