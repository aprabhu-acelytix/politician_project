import os
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
import time

# --- 1. CONFIGURATION AND SETUP ---

load_dotenv()
DB_URL = os.getenv('DB_URL')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')

LINKAGE_FILE_2024 = os.path.join(DATA_DIR, '2024', 'ccl.txt')
LINKAGE_FILE_2026 = os.path.join(DATA_DIR, '2026', 'ccl.txt')
CONTRIB_FILE_2024 = os.path.join(DATA_DIR, '2024', 'itcont.txt')
CONTRIB_FILE_2026 = os.path.join(DATA_DIR, '2026', 'itcont.txt')

# Column headers from FEC header files
LINKAGE_COLUMNS = [
    'CAND_ID', 'CAND_ELECTION_YR', 'FEC_ELECTION_YR', 'CMTE_ID', 
    'CMTE_TP', 'CMTE_DSGN', 'LINKAGE_ID'
]
CONTRIB_COLUMNS = [
    'CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 
    'TRANSACTION_TP', 'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE', 
    'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID', 
    'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID'
]

try:
    engine = create_engine(DB_URL)
    print("Database connection successful.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

# Build politician-to-committee mapping
def get_committee_map():
    """
    Builds a "master map" of all committees that belong to our linked politicians.
    Returns two items:
    1. A Set of all relevant CMTE_IDs (for fast filtering)
    2. A mapping of {CMTE_ID -> politician_id}
    """
    print("Building politician-to-committee map...")
    
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(
            "SELECT politician_id, fec_candidate_id, fec_committee_id FROM politicians WHERE fec_candidate_id IS NOT NULL"
        ))
        linked_politicians = result.fetchall()
    
    cand_id_to_db_id = {row.fec_candidate_id: row.politician_id for row in linked_politicians}
    
    try:
        ccl_2024 = pd.read_csv(LINKAGE_FILE_2024, sep='|', header=None, names=LINKAGE_COLUMNS, dtype=str)
        ccl_2026 = pd.read_csv(LINKAGE_FILE_2026, sep='|', header=None, names=LINKAGE_COLUMNS, dtype=str)
        ccl_df = pd.concat([ccl_2024, ccl_2026]).drop_duplicates()
    except FileNotFoundError as e:
        print(f"ERROR: File not found. Make sure {e.filename} is in your data/2024 or data/2026 folder.")
        exit()

    committee_to_politician_map = {} # {CMTE_ID -> politician_id}
    all_target_committees = set()    # Set {CMTE_ID, ...}

    for cand in linked_politicians:
        if cand.fec_committee_id:
            all_target_committees.add(cand.fec_committee_id)
            committee_to_politician_map[cand.fec_committee_id] = cand.politician_id

    ccl_filtered = ccl_df[ccl_df['CAND_ID'].isin(cand_id_to_db_id.keys())]
    for _, row in ccl_filtered.iterrows():
        all_target_committees.add(row.CMTE_ID)
        committee_to_politician_map[row.CAND_ID] = cand_id_to_db_id[row.CAND_ID]

    print(f"Map built. Tracking {len(all_target_committees)} committees for {len(linked_politicians)} politicians.")
    return all_target_committees, committee_to_politician_map

# Process and load one chunk of donations
def process_donations_chunk(chunk_df, target_committees, committee_map, conn, donors_table, donations_table):
    """
    Processes one chunk (e.g., 500,000 rows) of the itcont.txt file.
    This version fixes the NaT (Not a Time) date error.
    """
    
    # Transform - Filter and prepare data
    chunk_df = chunk_df[chunk_df['CMTE_ID'].isin(target_committees)]
    if chunk_df.empty:
        return 0, 0 
    
    chunk_df = chunk_df[chunk_df['AMNDT_IND'] == 'N']
    
    chunk_df['donor_uid'] = chunk_df['NAME'].fillna('') + '|' + \
                            chunk_df['ZIP_CODE'].fillna('') + '|' + \
                            chunk_df['EMPLOYER'].fillna('')
    
    donors_df = chunk_df[['donor_uid', 'NAME', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'ENTITY_TP']].drop_duplicates(subset=['donor_uid'])
    
    donors_df = donors_df.rename(columns={
        'donor_uid': 'fec_committee_id', 
        'NAME': 'name',
        'ENTITY_TP': 'donor_type'
    })
    
    # Load - Insert new donors
    with conn.begin() as transaction:
        stmt = pg_insert(donors_table).on_conflict_do_nothing(
            index_elements=['fec_committee_id']
        )
        result = conn.execute(stmt, donors_df.to_dict('records'))
        new_donors = result.rowcount

    # Get donor IDs
    all_uids = donors_df['fec_committee_id'].tolist()
    uid_to_db_id_map = {}

    with conn.begin() as transaction:
        result = conn.execute(
            sqlalchemy.text("SELECT donor_id, fec_committee_id FROM donors WHERE fec_committee_id IN :uids"),
            {'uids': tuple(all_uids)}
        )
        for row in result:
            uid_to_db_id_map[row.fec_committee_id] = row.donor_id

    # Prepare & load donations
    chunk_df['donor_id'] = chunk_df['donor_uid'].map(uid_to_db_id_map)
    chunk_df['politician_id'] = chunk_df['CMTE_ID'].map(committee_map)
    
    donations_df = chunk_df.rename(columns={
        'TRANSACTION_DT': 'date',
        'TRANSACTION_AMT': 'amount',
        'SUB_ID': 'fec_filing_id'
    })
    
    # This line converts bad dates to 'NaT'
    donations_df['date'] = pd.to_datetime(donations_df['date'], format='%m%d%Y', errors='coerce')
    donations_df['amount'] = pd.to_numeric(donations_df['amount'], errors='coerce')
    
    donations_df = donations_df[['politician_id', 'donor_id', 'amount', 'date', 'fec_filing_id']]
    
    # We must drop any rows where 'date' became 'NaT' or other critical fields are missing
    donations_df = donations_df.dropna(subset=['politician_id', 'donor_id', 'amount', 'date'])

    with conn.begin() as transaction:
        conn.execute(donations_table.insert(), donations_df.to_dict('records'))
        new_donations = len(donations_df)

    return new_donors, new_donations

if __name__ == "__main__":
    start_time = time.time()
    
    donors_table = sqlalchemy.Table('donors', sqlalchemy.MetaData(), autoload_with=engine)
    donations_table = sqlalchemy.Table('donations', sqlalchemy.MetaData(), autoload_with=engine)
    
    print("Clearing old donation and donor data (TRUNCATE)...")
    with engine.connect() as conn:
        with conn.begin() as transaction:
            conn.execute(sqlalchemy.text("TRUNCATE TABLE donations, donors RESTART IDENTITY"))
    print("Old data cleared.")
    
    print("Verifying 'donors.fec_committee_id' column width...")
    with engine.connect() as conn:
        with conn.begin() as transaction:
            try:
                conn.execute(sqlalchemy.text("ALTER TABLE donors ALTER COLUMN fec_committee_id TYPE VARCHAR(500)"))
                print("  Column 'fec_committee_id' widened to VARCHAR(500).")
            except Exception:
                print(f"  Column is already the correct type.")
    
    target_committees, committee_map = get_committee_map()
    
    files_to_process = [CONTRIB_FILE_2024, CONTRIB_FILE_2026]
    total_new_donors = 0
    total_new_donations = 0

    with engine.connect() as conn:
        for file_path in files_to_process:
            print(f"\n--- Processing file: {file_path} ---")
            if not os.path.exists(file_path):
                print(f"Warning: File not found. Skipping.")
                continue
            
            # Added 'on_bad_lines' to handle errors ---
            chunk_num = 1
            for chunk in pd.read_csv(
                file_path, 
                sep='|', 
                header=None, 
                names=CONTRIB_COLUMNS, 
                dtype=str,
                chunksize=500000,
                on_bad_lines='warn' # This will skip bad rows instead of crashing
            ):
                chunk_start = time.time()
                print(f"  Processing chunk {chunk_num} ({(chunk_num-1)*500000} rows)...")
                
                try:
                    new_donors, new_donations = process_donations_chunk(
                        chunk, target_committees, committee_map, conn, donors_table, donations_table
                    )
                    
                    total_new_donors += new_donors
                    total_new_donations += new_donations
                    
                    print(f"    -> Added {new_donors} new donors, {new_donations} new donations.")
                    print(f"    -> Chunk processed in {time.time() - chunk_start:.2f}s")
                    chunk_num += 1
                except Exception as e:
                    print(f"CRITICAL ERROR processing chunk {chunk_num}: {e}")
                    print("Skipping this chunk and continuing...")
                    pass # Skip this entire chunk on a critical error

    end_time = time.time()
    print("\n--- BULK ETL COMPLETE ---")
    print(f"Total New Donors Inserted:     {total_new_donors}")
    print(f"Total New Donations Inserted:  {total_new_donations}")
    print(f"Total time taken: {end_time - start_time:.2f} seconds")