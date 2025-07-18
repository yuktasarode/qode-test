from sqlalchemy import Column, Integer, String, Float, BigInteger, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True)
    ticker = Column(String, unique=True, nullable=False)

    fundamentals = relationship("Fundamental", back_populates="company", cascade="all, delete-orphan")
    prices = relationship("Price", back_populates="company", cascade="all, delete-orphan")

class Fundamental(Base):
    __tablename__ = "fundamentals"
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year = Column(Integer, nullable=False)
    roe = Column(Float)
    roce = Column(Float)
    pat = Column(BigInteger)
    pe = Column(Float)
    market_cap = Column(BigInteger)

    company = relationship("Company", back_populates="fundamentals")
    __table_args__ = (UniqueConstraint('company_id', 'year', name='_fundamentals_uc'),)

class Price(Base):
    __tablename__ = "prices"
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    year = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)

    company = relationship("Company", back_populates="prices")
    __table_args__ = (UniqueConstraint('company_id', 'year', name='_prices_uc'),)
