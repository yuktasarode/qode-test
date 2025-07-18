import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

from models import Base, Company, Fundamental, Price

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)
session = Session()

def reset_database():
    print("🧹 Dropping and recreating tables...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("✅ Schema applied.")

def insert_companies(fund_csv, price_csv):
    fund_df = pd.read_csv(fund_csv)
    price_df = pd.read_csv(price_csv, index_col=0)
    tickers = sorted(set(fund_df["companyticker"]) | set(price_df.index))
    for ticker in tickers:
        session.merge(Company(ticker=ticker))
    session.commit()
    print(f"✅ Inserted {len(tickers)} companies.")

def insert_fundamentals(fund_csv):
    df = pd.read_csv(fund_csv)
    df.rename(columns={"companyticker": "ticker", "marketcap": "market_cap"}, inplace=True)
    ticker_to_id = {c.ticker: c.id for c in session.query(Company).all()}

    fundamentals = []
    for _, row in df.iterrows():
        company_id = ticker_to_id.get(row["ticker"])
        if company_id is None:
            continue
        fundamentals.append(Fundamental(
            company_id=company_id,
            year=int(row["year"]),
            roe=row.get("roe"),
            roce=row.get("roce"),
            pat=row.get("pat"),
            pe=row.get("pe"),
            market_cap=row.get("market_cap")
        ))
    session.bulk_save_objects(fundamentals)
    session.commit()
    print(f"✅ Inserted {len(fundamentals)} fundamentals.")

def insert_prices(price_csv):
    df = pd.read_csv(price_csv, index_col=0)
    df = df.reset_index().melt(id_vars=["index"], var_name="year", value_name="price")
    df.rename(columns={"index": "ticker"}, inplace=True)
    df["year"] = df["year"].astype(int)

    ticker_to_id = {c.ticker: c.id for c in session.query(Company).all()}
    prices = []
    for _, row in df.iterrows():
        company_id = ticker_to_id.get(row["ticker"])
        if company_id is None:
            continue
        prices.append(Price(
            company_id=company_id,
            year=int(row["year"]),
            price=float(row["price"])
        ))
    session.bulk_save_objects(prices)
    session.commit()
    print(f"✅ Inserted {len(prices)} prices.")

if __name__ == "__main__":
    reset_database()
    insert_companies("./data/New-fundamental_data.csv", "./data/prices.csv")
    insert_fundamentals("./data/New-fundamental_data.csv")
    insert_prices("./data/prices.csv")
