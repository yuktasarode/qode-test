import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

def drop_existing_tables(engine):
    with engine.connect() as conn:
        print("🧹 Dropping existing tables (if any)...")
        conn.execute(text("DROP TABLE IF EXISTS fundamentals;"))
        conn.execute(text("DROP TABLE IF EXISTS prices;"))
        conn.execute(text("DROP TABLE IF EXISTS companies;"))
        print("✅ Tables dropped.")

def apply_schema(engine, schema_file="schema.sql"):
    with engine.begin() as conn:
        with open(schema_file, "r") as file:
            conn.exec_driver_sql(file.read())
        print("✅ Schema applied.")

def insert_companies(fundamental_csv, price_csv):
    fund_df = pd.read_csv(fundamental_csv)
    price_df = pd.read_csv(price_csv, index_col=0)
    tickers = sorted(set(fund_df["companyticker"]) | set(price_df.index))
    companies_df = pd.DataFrame({"ticker": tickers})
    companies_df.to_sql("companies", con=engine, if_exists="append", index=False)
    print("✅ Companies inserted.")

def insert_fundamentals(fundamental_csv):
    print("🧾 Inserting fundamentals from CSV...")
    df = pd.read_csv(fundamental_csv)

    # Rename CSV columns to match database schema
    df.rename(columns={
        "companyticker": "ticker",
        "marketcap": "market_cap"
    }, inplace=True)

    # Get company_id mapping
    companies = pd.read_sql("SELECT id, ticker FROM companies", con=engine)
    print(len(companies))
    df = df.merge(companies, on="ticker")
    df.drop(columns=["ticker"], inplace=True)
    df.rename(columns={"id": "company_id"}, inplace=True)
    print("Number of rows:", df.shape[0])
    print(df.columns)


    # Insert into fundamentals table
    df.to_sql("fundamentals", con=engine, if_exists="append", index=False)
    print("✅ Fundamentals inserted.")


def insert_prices(csv_path):
    print("📈 Inserting price data...")
    df = pd.read_csv(csv_path, index_col=0)
    df = df.reset_index().melt(id_vars=["index"], var_name="year", value_name="price")
    df.rename(columns={"index": "ticker"}, inplace=True)
    df["year"] = df["year"].astype(int)
    companies = pd.read_sql("SELECT id, ticker FROM companies", con=engine)
    df = df.merge(companies, on="ticker")
    df.rename(columns={"id": "company_id"}, inplace=True)
    df = df[["company_id", "year", "price"]]
    df.to_sql("prices", con=engine, if_exists="append", index=False)
    print("✅ Prices inserted.")

def print_table_schema(table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """))
        rows = result.fetchall()
        print(f"\n📋 Schema for '{table_name}':")
        for col in rows:
            print(f"  - {col.column_name} ({col.data_type}) nullable: {col.is_nullable}")

if __name__ == "__main__":
    drop_existing_tables(engine)
    apply_schema(engine)
    insert_companies("./data/New-fundamental_data.csv", "./data/prices.csv")
    insert_fundamentals("./data/New-fundamental_data.csv")
    insert_prices("./data/prices.csv")
    for table in ["companies", "fundamentals", "prices"]:
        print_table_schema(table)


