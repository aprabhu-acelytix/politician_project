"""
SQLAlchemy ORM models for the Politicians database.
These models map to the existing database tables created by the ETL scripts.
Schema verified: 2025-11-09
"""
from sqlalchemy import Column, Integer, String, Date, Numeric, ForeignKey, Text, Boolean
from sqlalchemy.orm import relationship
from .database import Base


class Politician(Base):
    """Model for the politicians table."""
    __tablename__ = "politicians"

    politician_id = Column(Integer, primary_key=True, autoincrement=True)
    congress_id = Column(String(20), unique=True)  # Bioguide ID
    fec_candidate_id = Column(String(20), unique=True)
    fec_committee_id = Column(String(20), unique=True)
    first_name = Column(String(100))
    last_name = Column(String(100))
    party = Column(String(50))
    state = Column(String(2))
    chamber = Column(String(10))
    date_of_birth = Column(Date)
    is_active = Column(Boolean)
    start_year = Column(Integer)
    end_year = Column(Integer)
    
    # Relationships
    votes = relationship("Vote", back_populates="politician")
    donations = relationship("Donation", back_populates="politician")


class Donor(Base):
    """Model for the donors table."""
    __tablename__ = "donors"

    donor_id = Column(Integer, primary_key=True, autoincrement=True)
    donor_source_key = Column(String(500), unique=True)  # The unique donor identifier from FEC
    name = Column(String(255))
    donor_type = Column(String(50))  # 'PAC', 'Individual', etc.
    industry = Column(String(100))  # 'Securities', 'Defense', etc.
    
    # Relationships
    donations = relationship("Donation", back_populates="donor")


class Donation(Base):
    """Model for the donations table."""
    __tablename__ = "donations"

    donation_id = Column(Integer, primary_key=True, autoincrement=True)
    politician_id = Column(Integer, ForeignKey("politicians.politician_id"), nullable=False)
    donor_id = Column(Integer, ForeignKey("donors.donor_id"), nullable=False)
    amount = Column(Numeric(12, 2))
    date = Column(Date)
    fec_filing_id = Column(String(50))
    
    # Relationships
    politician = relationship("Politician", back_populates="donations")
    donor = relationship("Donor", back_populates="donations")


class Bill(Base):
    """Model for the bills table."""
    __tablename__ = "bills"

    bill_id = Column(Integer, primary_key=True, autoincrement=True)
    official_bill_number = Column(String(20))
    congress = Column(Integer)
    title = Column(Text)
    summary = Column(Text)
    date_introduced = Column(Date)
    status = Column(Text)
    bill_type = Column(String(10))
    
    # Relationships
    votes = relationship("Vote", back_populates="bill")


class Vote(Base):
    """Model for the votes table."""
    __tablename__ = "votes"

    vote_id = Column(Integer, primary_key=True, autoincrement=True)
    politician_id = Column(Integer, ForeignKey("politicians.politician_id"), nullable=False)
    bill_id = Column(Integer, ForeignKey("bills.bill_id"), nullable=False)
    date = Column(Date)
    vote_position = Column(String(20))
    vote_category = Column(String(50))
    
    # Relationships
    bill = relationship("Bill", back_populates="votes")
    politician = relationship("Politician", back_populates="votes")
