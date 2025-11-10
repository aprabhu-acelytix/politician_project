import os
import requests
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
import time

load_dotenv()
API_KEY = os.getenv('CONGRESS_API_KEY')
DB_URL = os.getenv('DB_URL')

BILLS_API_BASE = "https://api.congress.gov/v3/bill"

# Get all data relevant to 2024-2026 donors
# Fetch bills from the 118th (2023-24) and 119th (2025-26) Congresses
CONGRESSES_TO_FETCH = [118, 119]

try:
    engine = create_engine(DB_URL)
    print("Database connection successful.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

# FETCH AND LOAD BILLS
def parse_bill_data(bill_data):
    """
    Parses the bill object from the API response
    and prepares it for database insertion.
    """
    try:
        bill_number = bill_data.get('number')
        bill_congress = bill_data.get('congress')
        bill_type = bill_data.get('type') # <-- NEW: Get the bill type
        bill_title = bill_data.get('title')
        
        latest_action_text = None
        latest_action = bill_data.get('latestAction')
        if latest_action and isinstance(latest_action, dict):
            latest_action_text = latest_action.get('text')

        official_bill_number = f"{bill_type}{bill_number}"

        if not bill_number or not bill_type or not bill_congress:
            return None 

        return {
            "official_bill_number": official_bill_number,
            "bill_type": bill_type, # <-- NEW: Add to our dictionary
            "congress": bill_congress,
            "title": bill_title,
            "status": latest_action_text,
        }
    except Exception as e:
        print(f"  Error parsing bill data: {e}")
        return None

def fetch_and_load_bills():
    """
    Fetches all bills for the specified congresses
    and "upserts" them into the 'bills' table using the correct composite key.
    """
    
    bills_table = sqlalchemy.Table('bills', sqlalchemy.MetaData(), autoload_with=engine)
    total_bills_processed = 0

    with engine.connect() as conn:
        for congress in CONGRESSES_TO_FETCH:
            print(f"\n--- Fetching all bills for {congress}th Congress ---")
            
            list_url = f"{BILLS_API_BASE}/{congress}"
            next_url = list_url 
            
            while next_url:
                headers = {"X-API-Key": API_KEY, "Accept": "application/json"}
                
                if next_url != list_url:
                    api_call_url = next_url
                    params = None 
                else:
                    api_call_url = next_url
                    params = {'limit': 250} 
                
                time.sleep(1) 
                response = requests.get(api_call_url, headers=headers, params=params)

                if response.status_code == 429:
                    print("Rate limit hit. Pausing for 10 minutes...")
                    time.sleep(601)
                    continue 

                if response.status_code != 200:
                    print(f"Error fetching bill list: {response.status_code} {response.text}")
                    break 

                data = response.json()
                bills_list = data.get('bills', [])
                
                if not bills_list:
                    print(f"  No bills found for {congress}.")
                    break

                print(f"  Processing {len(bills_list)} bills from API...")
                
                bills_to_upsert = []
                for bill_data in bills_list:
                    parsed_bill = parse_bill_data(bill_data)
                    if parsed_bill:
                        bills_to_upsert.append(parsed_bill)
                
                if bills_to_upsert:
                    try:
                        with conn.begin() as transaction:
                            stmt = pg_insert(bills_table).values(bills_to_upsert)
                            
                            update_stmt = stmt.on_conflict_do_update(
                                # Use the new composite key
                                index_elements=['official_bill_number', 'congress'], 
                                # Update all fields, including the new bill_type
                                set_={
                                    'title': stmt.excluded.title,
                                    'status': stmt.excluded.status,
                                    'congress': stmt.excluded.congress,
                                    'bill_type': stmt.excluded.bill_type # <-- NEW
                                }
                            )
                            conn.execute(update_stmt)
                            total_bills_processed += len(bills_to_upsert)

                    except Exception as e:
                        print(f"  ERROR batch inserting bills: {e}")
                        pass 

                next_url = data.get('pagination', {}).get('next', None)
                if next_url:
                    print(f"  Fetching next page of bills...")
                else:
                    print(f"  Finished processing Congress {congress}.")

    print("\n--- Bill ETL Complete ---")
    print(f"Total bills processed/updated: {total_bills_processed}")

# Run script
if __name__ == "__main__":
    print("Starting bill ingestion...")
    fetch_and_load_bills()
    print("Bill ingestion complete.")