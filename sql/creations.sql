-- This script creates the tables for your politician project.
-- Use SERIAL for IDs, which makes them auto-incrementing integers.

/*-- 1. The central 'politicians' table
CREATE TABLE politicians (
    politician_id SERIAL PRIMARY KEY,
    congress_id VARCHAR(20) UNIQUE, -- ID from Congress.gov (e.g., "B001288")
    fec_candidate_id VARCHAR(20) UNIQUE, -- ID from FEC (e.g., "S6NJ00188")
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    party VARCHAR(10),
    state VARCHAR(2),
    chamber VARCHAR(10), -- 'House' or 'Senate'
    date_of_birth DATE
);*/


/*-- 2. The 'bills' table for legislation
CREATE TABLE bills (
    bill_id SERIAL PRIMARY KEY,
    official_bill_number VARCHAR(20) UNIQUE, -- (e.g., "H.R. 5110")
    congress INTEGER, -- (e.g., 117 or 118)
    title TEXT,
    summary TEXT,
    date_introduced DATE,
    status VARCHAR(50)
);*/


/*-- 3. The 'votes' JOIN table (connects politicians and bills)
CREATE TABLE votes (
    vote_id SERIAL PRIMARY KEY,
    politician_id INTEGER,
    bill_id INTEGER,
    date DATE,
    vote_position VARCHAR(20),
    
    -- This sets up the Foreign Key relationships
    FOREIGN KEY (politician_id) REFERENCES politicians(politician_id),
    FOREIGN KEY (bill_id) REFERENCES bills(bill_id)
);*/



/*
-- 4. The 'donors' table (PACs, individuals, etc.)
CREATE TABLE donors (
    donor_id SERIAL PRIMARY KEY,
    donor_source_key VARCHAR(20) UNIQUE, -- The main ID from the FEC
    name VARCHAR(255),
    donor_type VARCHAR(50), -- 'PAC', 'Individual'
    industry VARCHAR(100) -- 'Securities', 'Defense', etc.
);
*/

/*
-- 5. The 'donations' JOIN table (connects politicians and donors)
CREATE TABLE donations (
    donation_id SERIAL PRIMARY KEY,
    politician_id INTEGER,
    donor_id INTEGER,
    amount NUMERIC(12, 2), -- Good for currency
    date DATE,
    fec_filing_id VARCHAR(50),
    
    -- This sets up the Foreign Key relationships
    FOREIGN KEY (politician_id) REFERENCES politicians(politician_id),
    FOREIGN KEY (donor_id) REFERENCES donors(donor_id)
);*/

-- Forgot about Independent party :3 / Future proofing
/*ALTER TABLE politicians
ALTER COLUMN party TYPE VARCHAR(50);*/

--TRUNCATE TABLE politicians RESTART IDENTITY CASCADE;

-- Add new fec_committee-id column to 'politicians'
/*ALTER TABLE politicians
ADD COLUMN fec_committee_id VARCHAR(20) UNIQUE;*/


-- Add is_active, start_year, end_year columns to politicians table
/*
ALTER TABLE politicians
ADD COLUMN IF NOT EXISTS is_active BOOLEAN,
ADD COLUMN IF NOT EXISTS start_year INTEGER,
ADD COLUMN IF NOT EXISTS end_year INTEGER;
*/

-- Alter bills.status to TEXT for longer status descriptions
/*
ALTER TABLE bills
ALTER COLUMN status TYPE TEXT;
*/

-- Ran the commands below because the script was failing due to unique constraint on official_bill_number only
-- 1. Drop the old, incorrect unique constraint
/*
ALTER TABLE bills
DROP CONSTRAINT bills_official_bill_number_key;

-- 2. Create the new, correct composite unique constraint
ALTER TABLE bills
ADD CONSTRAINT bills_congress_number_unique UNIQUE (official_bill_number, congress);


-- 3. Truncate the tables to prepare for a clean re-ingestion
-- (CASCADE will also clear the empty 'votes' table)
TRUNCATE TABLE bills RESTART IDENTITY CASCADE;
*/

-- Ran the commands below to add bill_type column to bills table
/*
-- 1. Clear the data and all dependent votes
TRUNCATE TABLE bills RESTART IDENTITY CASCADE;

-- 2. Fix the 'status' column to allow long text
ALTER TABLE bills
ALTER COLUMN status TYPE TEXT;

-- 3. Add your new 'bill_type' column
ALTER TABLE bills
ADD COLUMN bill_type VARCHAR(10);

-- 4. Drop the old, incorrect unique constraint
ALTER TABLE bills
DROP CONSTRAINT bills_official_bill_number_key;

-- 5. Add the new, correct composite unique constraint
ALTER TABLE bills
ADD CONSTRAINT bills_congress_number_unique UNIQUE (official_bill_number, congress);
*/

--Adding vote_category column to votes table
/*
ALTER TABLE votes
ADD COLUMN vote_category VARCHAR(50);
*/