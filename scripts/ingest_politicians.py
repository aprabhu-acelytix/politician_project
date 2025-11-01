import os
import requests
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.engine import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Load environment variables from our .env file
load_dotenv()

# API Key for Congress.gov API
API_KEY = os.getenv('CONGRESS_API_KEY')

# This is the API endpoint to get all members.
# Congress enforces ?limit=250
MEMBER_API_URL = f"https://api.congress.gov/v3/member?limit=250"

# Database connection.
# Tells SQLAlchemy how to find database.
DB_URL = os.getenv('DB_URL')

# Create the database engine
try:
    engine = create_engine(DB_URL)
    print("Database connection successful.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

# Transform - Convert state names to standard abbreviations
STATE_TO_ABBR_MAP = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 
    'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 
    'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 
    'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI', 
    'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 
    'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 
    'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 
    'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 
    'Wisconsin': 'WI', 'Wyoming': 'WY',
    # Territories and Districts as well
    'American Samoa': 'AS', 'District of Columbia': 'DC', 'Guam': 'GU',
    'Northern Mariana Islands': 'MP', 'Puerto Rico': 'PR', 
    'Virgin Islands': 'VI'
}

# Extract - Fetch data using api 
def fetch_all_members():
    """
    - Fetches all members from the Congress.gov API.
    - Handles pagination to get all results.
    """
    print("Starting to fetch members from Congress.gov API...")
    
    all_members = []
    
    # First page URL
    next_url = MEMBER_API_URL
    
    # Keep fetching as long as the API tells us there is a "next" page
    while next_url:
        headers = {
            "X-API-Key": API_KEY,
            "Accept": "application/json"
        }
        
        response = requests.get(next_url, headers=headers)
        
        # Check for a successful response
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code} {response.text}")
            break
            
        data = response.json()
        
        # Add the members from this page to our list
        members = data.get('members', [])
        all_members.extend(members)
        print(f"Fetched {len(members)} members. Total: {len(all_members)}")

        # Check for the next page URL. If it's missing, we're done.
        next_url = data.get('pagination', {}).get('next', None)
        
    print(f"Total members fetched: {len(all_members)}")
    return all_members

# Load - Insert data into database
def load_members_to_db(members):
    """
    Loads a list of member data into the 'politicians' table.
    - Correctly parses 'name' field with suffixes (e.g., "Last, First, Jr.")
    - Cleans chamber data ("House of Representatives" -> "House")
    - Transforms full state names to 2-letter abbreviations.
    - Uses PostgreSQL-specific 'pg_insert' for 'ON CONFLICT'.
    """
    print("Starting to load members into the database...")
    
    with engine.connect() as conn:
        politicians_table = sqlalchemy.Table('politicians', sqlalchemy.MetaData(), autoload_with=engine)
        
        with conn.begin() as transaction:
            count = 0
            inserted_count = 0
            members_to_insert = []  # Batch insert

            for member in members:
                try:
                    congress_id = member.get('bioguideId')
                    full_name_str = member.get('name')

                    if not congress_id or not full_name_str:
                        print(f"SKIPPING: Member with missing bioguideId or name.")
                        continue
                    
                    # Parse name
                    first_name = None
                    last_name = None
                    
                    if ', ' in full_name_str:
                        # Split only on the first comma.
                        # "Anthony, Beryl, Jr." -> ['Anthony', 'Beryl, Jr.']
                        parts = full_name_str.split(', ', 1) 
                        last_name = parts[0].strip()
                        first_name = parts[1].strip()
                    else:
                        # Failsafe for names with no comma (e.g., "Ronnie G. Flippo")
                        last_name = full_name_str.strip()
                    
                    # Transform State name to abbreviation
                    party = member.get('partyName')
                    full_state_name = member.get('state')
                    state_abbr = STATE_TO_ABBR_MAP.get(full_state_name) 
                    
                    if not state_abbr and full_state_name:
                        print(f"Warning: No abbreviation for state: '{full_state_name}'. Skipping member {congress_id}.")
                        continue 

                    # Transform Chamber
                    chamber = None
                    raw_chamber = None
                    terms_object = member.get('terms')
                    
                    if terms_object and isinstance(terms_object, dict):
                        terms_list = terms_object.get('item')
                        if terms_list and isinstance(terms_list, list) and len(terms_list) > 0:
                            raw_chamber = terms_list[-1].get('chamber')
                    
                    if raw_chamber == "House of Representatives":
                        chamber = "House"
                    elif raw_chamber == "Senate":
                        chamber = "Senate"

                    # Add to batch
                    insert_data = {
                        "congress_id": congress_id,
                        "first_name": first_name, 
                        "last_name": last_name,
                        "party": party,
                        "state": state_abbr, 
                        "chamber": chamber
                    }
                    members_to_insert.append(insert_data)
                    count += 1
                
                except Exception as e:
                    print(f"CRITICAL ERROR processing {member.get('bioguideId')}: {e}")

            # Load - Execute Batch insert
            if members_to_insert:
                try:
                    stmt = pg_insert(politicians_table).on_conflict_do_nothing(
                        index_elements=['congress_id']
                    )
                    result = conn.execute(stmt, members_to_insert)
                    inserted_count = result.rowcount

                except Exception as e:
                    print(f"\n    DATABASE ERROR    ")
                    print(f"A critical database error occurred: {e}")
                    print("                         \n")
                    raise e 

            print(f"Processed {count} members. Successfully inserted {inserted_count} new members.")

if __name__ == "__main__":
    # Extract
    member_list = fetch_all_members()

    # Load
    if member_list:
        load_members_to_db(member_list)
    else:
        print("No members fetched, database was not updated.")