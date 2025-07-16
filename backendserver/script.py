import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os


# PostgreSQL database URL (replace with real credentials)
# Load .env file
load_dotenv()

# Get the DB URL
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)


def drop_existing_tables(engine):
    with engine.connect() as conn:
        print("🧹 Dropping existing tables (if any)...")
        conn.execute(text("DROP TABLE IF EXISTS fundamentals;"))
        conn.execute(text("DROP TABLE IF EXISTS companies;"))
        conn.execute(text("DROP TABLE IF EXISTS prices;"))
        print("✅ Tables dropped.")

def apply_schema(engine, schema_file="schema.sql"):
    with engine.begin() as conn:  # ✅ THIS COMMITS AUTOMATICALLY
        with open(schema_file, "r") as file:
            sql_commands = file.read()

        print("⚙️ Executing SQL:")
        print(sql_commands)

        conn.exec_driver_sql(sql_commands)
        print("✅ Schema applied.")

def insert_fundamentals_from_csv():
    print("🧾 Inserting real fundamentals from CSV...")
    fundamentals = pd.read_csv("./data/fundamental_data.csv")

    # Insert unique tickers into companies
    tickers = fundamentals["ticker"].unique()
    companies_df = pd.DataFrame({"ticker": tickers})
    companies_df.to_sql("companies", con=engine, if_exists="append", index=False)

    # Get assigned company IDs
    companies = pd.read_sql("SELECT id, ticker FROM companies", con=engine)
    fundamentals = fundamentals.merge(companies, on="ticker")
    fundamentals = fundamentals.drop(columns="ticker")
    fundamentals.rename(columns={"id": "company_id"}, inplace=True)

    fundamentals["date"] = pd.Timestamp.today().normalize()
    fundamentals.to_sql("fundamentals", con=engine, if_exists="append", index=False)
    print("✅ Real fundamentals stored.")

def print_table_schema(table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """))
        rows = result.fetchall()

        if not rows:
            print(f"⚠️ Table '{table_name}' does not exist.")
        else:
            print(f"📋 Schema for table '{table_name}':")
            for col in rows:
                print(f"  - {col.column_name} ({col.data_type}) nullable: {col.is_nullable}")

if __name__ == "__main__":
    drop_existing_tables(engine)      # Drop everything
    apply_schema(engine)              # Apply correct schema
    print_table_schema("companies")
    print_table_schema("fundamentals")
    print_table_schema("prices")
    insert_fundamentals_from_csv()    # Insert CSV data cleanly
