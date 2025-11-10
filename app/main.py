"""
FastAPI main application for the Politicians API.
Provides endpoints to query politicians, donations, bills, and votes.
"""
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

from .database import get_db
from .models import Politician, Donor, Donation, Bill, Vote

# Create FastAPI application
app = FastAPI(
    title="Politicians API",
    description="API for querying US politicians, campaign donations, bills, and votes",
    version="1.0.0"
)


@app.get("/")
def read_root():
    """
    Hello World endpoint to verify the API is running.
    """
    return {
        "message": "Welcome to the Politicians API!",
        "version": "1.0.0",
        "endpoints": {
            "politicians": "/politicians",
            "donors": "/donors",
            "donations": "/donations",
            "bills": "/bills",
            "votes": "/votes"
        }
    }


@app.get("/health")
def health_check():
    """
    Health check endpoint.
    """
    return {"status": "healthy", "service": "Politicians API"}


@app.get("/politicians")
def get_politicians(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    party: Optional[str] = Query(None, description="Filter by party (e.g., 'Democrat', 'Republican')"),
    state: Optional[str] = Query(None, description="Filter by state (e.g., 'CA', 'TX')"),
    chamber: Optional[str] = Query(None, description="Filter by chamber ('House' or 'Senate')"),
    is_active: Optional[bool] = Query(None, description="Filter by active status")
):
    """
    Get list of politicians with optional filtering.
    
    - **skip**: Number of records to skip (pagination)
    - **limit**: Maximum number of records to return (max 1000)
    - **party**: Filter by political party
    - **state**: Filter by state code
    - **chamber**: Filter by chamber (House or Senate)
    - **is_active**: Filter by active status
    """
    query = db.query(Politician)
    
    # Apply filters
    if party:
        query = query.filter(Politician.party == party)
    if state:
        query = query.filter(Politician.state == state)
    if chamber:
        query = query.filter(Politician.chamber == chamber)
    if is_active is not None:
        query = query.filter(Politician.is_active == is_active)
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    politicians = query.offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "count": len(politicians),
        "politicians": [
            {
                "politician_id": p.politician_id,
                "congress_id": p.congress_id,
                "fec_candidate_id": p.fec_candidate_id,
                "first_name": p.first_name,
                "last_name": p.last_name,
                "full_name": f"{p.first_name} {p.last_name}",
                "party": p.party,
                "state": p.state,
                "chamber": p.chamber,
                "is_active": p.is_active,
                "start_year": p.start_year,
                "end_year": p.end_year
            }
            for p in politicians
        ]
    }


@app.get("/politicians/{politician_id}")
def get_politician_by_id(
    politician_id: int,
    db: Session = Depends(get_db)
):
    """
    Get a specific politician by their database ID.
    You can also search by congress_id using query parameters.
    """
    politician = db.query(Politician).filter(Politician.politician_id == politician_id).first()
    
    if not politician:
        raise HTTPException(status_code=404, detail=f"Politician with ID {politician_id} not found")
    
    return {
        "politician_id": politician.politician_id,
        "congress_id": politician.congress_id,
        "fec_candidate_id": politician.fec_candidate_id,
        "first_name": politician.first_name,
        "last_name": politician.last_name,
        "full_name": f"{politician.first_name} {politician.last_name}",
        "party": politician.party,
        "state": politician.state,
        "chamber": politician.chamber,
        "is_active": politician.is_active,
        "start_year": politician.start_year,
        "end_year": politician.end_year
    }


@app.get("/stats")
def get_database_stats(db: Session = Depends(get_db)):
    """
    Get summary statistics about the database.
    """
    return {
        "politicians": {
            "total": db.query(Politician).count(),
            "active": db.query(Politician).filter(Politician.is_active == True).count(),
            "house": db.query(Politician).filter(Politician.chamber == "House").count(),
            "senate": db.query(Politician).filter(Politician.chamber == "Senate").count()
        },
        "donors": {
            "total": db.query(Donor).count()
        },
        "donations": {
            "total": db.query(Donation).count()
        },
        "bills": {
            "total": db.query(Bill).count()
        },
        "votes": {
            "total": db.query(Vote).count()
        }
    }


@app.get("/donors")
def get_donors(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    donor_type: Optional[str] = Query(None, description="Filter by donor type (e.g., 'PAC', 'Individual')"),
    industry: Optional[str] = Query(None, description="Filter by industry")
):
    """
    Get list of donors with optional filtering.
    """
    query = db.query(Donor)
    
    if donor_type:
        query = query.filter(Donor.donor_type == donor_type)
    if industry:
        query = query.filter(Donor.industry == industry)
    
    total = query.count()
    donors = query.offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "count": len(donors),
        "donors": [
            {
                "donor_id": d.donor_id,
                "donor_source_key": d.donor_source_key,
                "name": d.name,
                "donor_type": d.donor_type,
                "industry": d.industry
            }
            for d in donors
        ]
    }


@app.get("/donations")
def get_donations(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    politician_id: Optional[int] = Query(None, description="Filter by politician ID"),
    donor_id: Optional[int] = Query(None, description="Filter by donor ID"),
    min_amount: Optional[float] = Query(None, description="Minimum donation amount"),
    max_amount: Optional[float] = Query(None, description="Maximum donation amount")
):
    """
    Get list of donations with optional filtering.
    """
    query = db.query(Donation)
    
    if politician_id:
        query = query.filter(Donation.politician_id == politician_id)
    if donor_id:
        query = query.filter(Donation.donor_id == donor_id)
    if min_amount:
        query = query.filter(Donation.amount >= min_amount)
    if max_amount:
        query = query.filter(Donation.amount <= max_amount)
    
    total = query.count()
    donations = query.offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "count": len(donations),
        "donations": [
            {
                "donation_id": d.donation_id,
                "politician_id": d.politician_id,
                "donor_id": d.donor_id,
                "amount": float(d.amount) if d.amount else None,
                "date": d.date.isoformat() if d.date else None,
                "fec_filing_id": d.fec_filing_id
            }
            for d in donations
        ]
    }


@app.get("/bills")
def get_bills(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    congress: Optional[int] = Query(None, description="Filter by congress number"),
    bill_type: Optional[str] = Query(None, description="Filter by bill type (e.g., 'HR', 'S')")
):
    """
    Get list of bills with optional filtering.
    """
    query = db.query(Bill)
    
    if congress:
        query = query.filter(Bill.congress == congress)
    if bill_type:
        query = query.filter(Bill.bill_type == bill_type)
    
    total = query.count()
    bills = query.offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "count": len(bills),
        "bills": [
            {
                "bill_id": b.bill_id,
                "official_bill_number": b.official_bill_number,
                "congress": b.congress,
                "title": b.title,
                "summary": b.summary,
                "date_introduced": b.date_introduced.isoformat() if b.date_introduced else None,
                "status": b.status,
                "bill_type": b.bill_type
            }
            for b in bills
        ]
    }


@app.get("/votes")
def get_votes(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    politician_id: Optional[int] = Query(None, description="Filter by politician ID"),
    bill_id: Optional[int] = Query(None, description="Filter by bill ID"),
    vote_position: Optional[str] = Query(None, description="Filter by vote position (e.g., 'Yea', 'Nay')")
):
    """
    Get list of votes with optional filtering.
    """
    query = db.query(Vote)
    
    if politician_id:
        query = query.filter(Vote.politician_id == politician_id)
    if bill_id:
        query = query.filter(Vote.bill_id == bill_id)
    if vote_position:
        query = query.filter(Vote.vote_position == vote_position)
    
    total = query.count()
    votes = query.offset(skip).limit(limit).all()
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "count": len(votes),
        "votes": [
            {
                "vote_id": v.vote_id,
                "politician_id": v.politician_id,
                "bill_id": v.bill_id,
                "vote_position": v.vote_position,
                "vote_category": v.vote_category,
                "date": v.date.isoformat() if v.date else None
            }
            for v in votes
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
