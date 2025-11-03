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

# Load - Upsert data into database
def load_members_to_db(members):
    """
    Performs an "UPSERT" on the politicians table.
    - Inserts new politicians if they don't exist.
    - Updates existing politicians with fresh data, active status, and full term history.
    - Correctly parses 'name' field with suffixes (e.g., "Last, First, Jr.")
    - Cleans chamber data ("House of Representatives" -> "House")
    - Transforms full state names to 2-letter abbreviations.
    - Uses PostgreSQL-specific 'pg_insert' for 'ON CONFLICT...DO UPDATE'.
    """
    print("Starting to load and update members in the database...") # Updated message
    
    with engine.connect() as conn:
        politicians_table = sqlalchemy.Table('politicians', sqlalchemy.MetaData(), autoload_with=engine)
        
        # Get the current year to determine 'is_active' status
        current_year = 2025 
            
        members_to_upsert = [] # Renamed to reflect UPSERT
        count = 0

        for member in members:
            try:
                congress_id = member.get('bioguideId')
                full_name_str = member.get('name')

                if not congress_id or not full_name_str:
                    print(f"SKIPPING: Member with missing bioguideId or name.")
                    continue
                
                # Parse name (Your existing logic)
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
                
                # Transform State name to abbreviation (Your existing logic)
                party = member.get('partyName')
                full_state_name = member.get('state')
                state_abbr = STATE_TO_ABBR_MAP.get(full_state_name) 
                
                if not state_abbr and full_state_name:
                    print(f"Warning: No abbreviation for state: '{full_state_name}'. Skipping member {congress_id}.")
                    continue 

                # --- NEW: Transform Chamber, Term Dates, and Active Status ---
                chamber = None
                all_start_years = []
                all_end_years = []
                is_active = False

                terms_object = member.get('terms')
                if terms_object and isinstance(terms_object, dict):
                    terms_list = terms_object.get('item')
                    if terms_list and isinstance(terms_list, list) and len(terms_list) > 0:
                        
                        # Loop through all terms (e.g., House, then Senate)
                        for term in terms_list:
                            # Handle inconsistent keys ('start' vs 'startYear')
                            start = term.get('startYear') or term.get('start')
                            if start:
                                all_start_years.append(int(start))
                            
                            # Handle inconsistent keys ('end' vs 'endYear')
                            end = term.get('endYear') or term.get('end')
                            if end:
                                all_end_years.append(int(end))
                            elif end is None: # Actively serving in this term
                                is_active = True

                        # Use the *latest* term for their current/last chamber
                        latest_term = terms_list[-1]
                        raw_chamber = latest_term.get('chamber')
                        if raw_chamber == "House of Representatives":
                            chamber = "House"
                        elif raw_chamber == "Senate":
                            chamber = "Senate"

                # Calculate final start and end years
                final_start_year = min(all_start_years) if all_start_years else None
                final_end_year = None
                
                if not is_active and all_end_years:
                    # If they are retired, their end year is the latest one
                    final_end_year = max(all_end_years)
                # If is_active, final_end_year correctly stays None (NULL)
                # --- END NEW LOGIC ---

                # Add to batch
                insert_data = {
                    "congress_id": congress_id,
                    "first_name": first_name, 
                    "last_name": last_name,
                    "party": party,
                    "state": state_abbr, 
                    "chamber": chamber,
                    "is_active": is_active,           # Your new column
                    "start_year": final_start_year,   # Your new column
                    "end_year": final_end_year        # Your new column
                }
                members_to_upsert.append(insert_data)
                count += 1
            
            except Exception as e:
                print(f"CRITICAL ERROR processing {member.get('bioguideId')}: {e}")

        # Load - Execute Batch Upsert
        if members_to_upsert:
            print(f"Updating {len(members_to_upsert)} records in the database...")
            try:
                # We must wrap this in its own transaction
                with conn.begin() as transaction:
                    stmt = pg_insert(politicians_table).values(members_to_upsert)
                    
                    # Update logic for conflicts
                    update_stmt = stmt.on_conflict_do_update(
                        index_elements=['congress_id'], # The column that causes the conflict
                        # The columns to UPDATE if a conflict occurs
                        set_={
                            'first_name': stmt.excluded.first_name,
                            'last_name': stmt.excluded.last_name,
                            'party': stmt.excluded.party,
                            'state': stmt.excluded.state,
                            'chamber': stmt.excluded.chamber,
                            'is_active': stmt.excluded.is_active,
                            'start_year': stmt.excluded.start_year,
                            'end_year': stmt.excluded.end_year
                        }
                    )
                    
                    conn.execute(update_stmt)

            except Exception as e:
                print(f"\n   DATABASE ERROR   ")
                print(f"A critical database error occurred: {e}")
                print("                    \n")
                raise e 

            print(f"Processed and updated {count} members.") # Updated message

if __name__ == "__main__":
    # Extract
    member_list = fetch_all_members()

    # Load
    if member_list:
        load_members_to_db(member_list)
    else:
        print("No members fetched, database was not updated.")