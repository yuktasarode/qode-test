import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
import requests
import time
import random
import os
import json
import pandas as pd
import csv


# Load the CSV
df_prices = pd.read_csv("prices_by_ticker.csv", index_col=0)

# Convert DataFrame to nested dict: {ticker: {year: price}}
prices_by_ticker = df_prices.to_dict(orient="index")

def get_equity_and_reserves_from_soup(soup):
    equity_by_year = {}
    reserves_by_year = {}

    # 1. Find the balance sheet section
    bs_section = soup.find("section", {"id": "balance-sheet"})
    if not bs_section:
        return equity_by_year, reserves_by_year

    # 2. Find the table inside balance sheet section
    table = bs_section.find("table")
    if not table:
        return equity_by_year, reserves_by_year

    # 3. Extract year headers from <thead>
    thead = table.find("thead")
    year_headers = thead.find_all("th")[1:]  # skip first column (label)
    years = [th.get_text(strip=True).replace("Mar ", "") for th in year_headers]

    # Target only 2019–2024
    target_years = ["2019", "2020", "2021", "2022", "2023", "2024"]
    year_index_map = {year: idx for idx, year in enumerate(years) if year in target_years}

    # 4. Find rows in tbody
    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    for row in rows:
        label = row.find("td", class_="text")
        if not label:
            continue

        row_title = label.get_text(strip=True).lower()
        cells = row.find_all("td")[1:]

        if "equity capital" in row_title:
            for year, idx in year_index_map.items():
                value = cells[idx].get_text(strip=True).replace(",", "")
                equity_by_year[year] = float(value) if value else None

        elif "reserves" in row_title:
            for year, idx in year_index_map.items():
                value = cells[idx].get_text(strip=True).replace(",", "")
                reserves_by_year[year] = float(value) if value else None

    return equity_by_year, reserves_by_year


def compute_roe(pat, equity, reserves):
    roe = {}
    for year in pat:
        if year in equity and year in reserves:
            denominator = equity[year] + reserves[year]
            roe[year] = round((pat[year] / denominator) * 100, 2) if denominator else None
    return roe


def get_roce_from_soup(soup):
    roce_by_year = {}

    # 1. Find the ratios section
    ratios_section = soup.find("section", {"id": "ratios"})
    if not ratios_section:
        return roce_by_year  # return empty if not found

    # 2. Find the table inside ratios section
    table = ratios_section.find("table")
    if not table:
        return roce_by_year

    # 3. Extract year headers from <thead>
    thead = table.find("thead")
    year_headers = thead.find_all("th")[1:]  # skip the first empty header
    years = [th.get_text(strip=True).replace("Mar ", "") for th in year_headers]

    # Keep only indices for 2019 to 2024
    target_years = ["2019", "2020", "2021", "2022", "2023", "2024"]
    year_index_map = {year: idx for idx, year in enumerate(years) if year in target_years}

    # 4. Find ROCE row in <tbody>
    tbody = table.find("tbody")
    rows = tbody.find_all("tr")
    for row in rows:
        label = row.find("td", class_="text")
        if label and "ROCE %" in label.get_text():
            cells = row.find_all("td")[1:]  # skip label cell
            for year, idx in year_index_map.items():
                value = cells[idx].get_text(strip=True).replace("%", "")
                roce_by_year[year] = float(value) if value else None
            break  # we found the ROCE row, no need to continue

    return roce_by_year


def get_pat_eps_from_soup(soup):
    pat_by_year = {}
    eps_by_year = {}

    # 1. Find the profit-loss section
    pl_section = soup.find("section", {"id": "profit-loss"})
    if not pl_section:
        return pat_by_year  # return empty if not found

    # 2. Find the table inside profit-loss section
    table = pl_section.find("table")
    if not table:
        return pat_by_year, eps_by_year

    # 3. Extract year headers from <thead>
    thead = table.find("thead")
    year_headers = thead.find_all("th")[1:]  # skip the first empty header
    years = [th.get_text(strip=True).replace("Mar ", "") for th in year_headers]

    # Keep only indices for 2019 to 2024
    target_years = ["2019", "2020", "2021", "2022", "2023", "2024"]
    year_index_map = {year: idx for idx, year in enumerate(years) if year in target_years}

    # 4. Find Net Profit (PAT) row in <tbody>
    tbody = table.find("tbody")
    rows = tbody.find_all("tr")
    for row in rows:
        label = row.find("td", class_="text")
        if not label:
            continue
        label_text = label.get_text(strip=True)
        

        # Extract Net Profit
        if "Net Profit" in label_text:
            cells = row.find_all("td")[1:]
            for year, idx in year_index_map.items():
                value = cells[idx].get_text(strip=True).replace(",", "")
                pat_by_year[year] = int(value) if value.isdigit() else None

        # Extract EPS
        elif "EPS in Rs" in label_text:
            cells = row.find_all("td")[1:]
            for year, idx in year_index_map.items():
                value = cells[idx].get_text(strip=True).replace(",", "")
                try:
                    eps_by_year[year] = float(value)
                except ValueError:
                    eps_by_year[year] = None
    print(eps_by_year)
    return pat_by_year, eps_by_year


def compute_pe_ratio(price_by_year, eps_by_year):
    pe_by_year = {}
    for year in price_by_year:
        price = price_by_year.get(year)
        eps = eps_by_year.get(year)
        if price is not None and eps and eps != 0:
            pe_by_year[year] = round(price / eps, 2)
    return pe_by_year


def compute_market_cap(price_by_year, equity_by_year):
    market_cap_by_year = {}
    for year in price_by_year:
        if year in equity_by_year:
            total_shares = equity_by_year[year] * 1e7  # equity in crores
            price = price_by_year[year]
            market_cap_by_year[year] = round((total_shares * price) / 1e7, 2)  # result in crores
    return market_cap_by_year




CACHE_FILE = "screener_cache.json"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "KHTML, like Gecko Chrome/91.0.4472.124 Safari/537.36"
    )
}

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def get_metrics(ticker, soup):
    ratios = {}
    prices = prices_by_ticker[ticker]
    ratios["ROCE"]=get_roce_from_soup(soup)
    ratios["PAT"], ratios["EPS"]=get_pat_eps_from_soup(soup)
    equity, reserves = get_equity_and_reserves_from_soup(soup)
    ratios["ROE"]=compute_roe(ratios["PAT"], equity, reserves)
    ratios["Market Cap"]= compute_market_cap(prices, equity)
    ratios["PE"]=compute_pe_ratio(prices, ratios["EPS"])


    return {
        "market_cap": ratios.get("Market Cap"),  # In ₹ Cr
        "pe": ratios.get("PE"),
        "roce": ratios.get("ROCE"),
        "roe": ratios.get("ROE"),
        "pat": ratios.get("PAT"),
        "eps":ratios.get("EPS")
    }

def polite_delay(min_sec=2, max_sec=5):
    delay = random.uniform(min_sec, max_sec)
    print(f"⏳ Sleeping for {delay:.2f} seconds...")
    time.sleep(delay)

def scrape_fundamentals(ticker, use_cache=True):
    print(f"🔍 Scraping: {ticker}")
    url = f"https://www.screener.in/company/{ticker.replace('.NS', '')}/"
    cache = load_cache()

    if use_cache and ticker in cache:
        print(f"🧠 Using cached data for {ticker}")
        return cache[ticker]

    try:
        for attempt in range(3):  # retry logic
            response = requests.get(url, headers=HEADERS, timeout=10)
            print(response)
            if response.status_code == 200:
                break
            print(f"⚠️ Attempt {attempt + 1} failed with status {response.status_code}")
            time.sleep(2 ** attempt)

        else:
            raise Exception(f"Failed to fetch data after 3 attempts for {ticker}")

        soup = BeautifulSoup(response.text, "html.parser")


        metrics = get_metrics(ticker,soup)
        data = {
            "ticker": ticker,
            "roce": metrics["roce"],
            "roe": metrics["roe"],
            "pe": metrics["pe"],
            "market_cap": metrics["market_cap"],
            "pat": metrics["pat"]
        }

        # Cache the result
        cache[ticker] = data
        save_cache(cache)

        # Polite delay
        polite_delay()

        return data

    except Exception as e:
        print(f"❌ Failed to scrape {ticker}: {e}")
        return None


tickers_1 = ["TCS.NS", "INFY.NS", "RELIANCE.NS", "HDFCBANK.NS", "ICICIBANK.NS",
             "KOTAKBANK.NS", "LT.NS", "SBIN.NS", "AXISBANK.NS", "BAJFINANCE.NS"]

tickers_2 = ["HINDUNILVR.NS", "ITC.NS", "NESTLEIND.NS", "MARICO.NS", "BRITANNIA.NS",
             "COLPAL.NS", "DABUR.NS", "GODREJCP.NS", "EMAMILTD.NS", "JUBLFOOD.NS"]

tickers_3 = ["ASIANPAINT.NS", "BERGEPAINT.NS", "KANSAINER.NS", "PIDILITIND.NS", "INDIGO.NS",
             "TITAN.NS", "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "EICHERMOT.NS", "TVSMOTOR.NS"]

tickers_4 = ["M&M.NS", "TATAMOTORS.NS", "ASHOKLEY.NS", "MARUTI.NS", "ULTRACEMCO.NS",
             "AMBUJACEM.NS", "ACC.NS", "SHREECEM.NS", "JKCEMENT.NS", "RAMCOCEM.NS"]

tickers_5 = ["JSWSTEEL.NS", "TATASTEEL.NS", "SAIL.NS", "HINDALCO.NS", "VEDL.NS",
             "COALINDIA.NS", "NMDC.NS", "JINDALSTEL.NS", "NATIONALUM.NS", "MOIL.NS"]

tickers_6 = ["ADANIENT.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ADANIGREEN.NS", "ADANITRANS.NS",
             "POWERGRID.NS", "NTPC.NS", "TATAPOWER.NS", "NHPC.NS", "JSWENERGY.NS"]

tickers_7 = ["BHARTIARTL.NS", "IDEA.NS", "BSOFT.NS", "TECHM.NS", "WIPRO.NS",
             "LTTS.NS", "HCLTECH.NS", "MPHASIS.NS", "COFORGE.NS", "PERSISTENT.NS"]

tickers_8 = ["HAVELLS.NS", "VGUARD.NS", "CROMPTON.NS", "BAJAJELEC.NS", "POLYCAB.NS",
             "FINCABLES.NS", "KEI.NS", "SIEMENS.NS", "ABB.NS", "SCHNEIDER.NS"]

tickers_9 = ["DMART.NS", "UBL.NS", "PAGEIND.NS", "TRENT.NS", "RAJESHEXPO.NS",
             "BATAINDIA.NS", "RELAXO.NS", "PVRINOX.NS", "INOXWIND.NS", "INDIAMART.NS"]

tickers_10 = ["BIOCON.NS", "DIVISLAB.NS", "CIPLA.NS", "SUNPHARMA.NS", "AUROPHARMA.NS",
              "LUPIN.NS", "DRREDDY.NS", "GLENMARK.NS", "ZYDUSLIFE.NS", "ALKEM.NS"]

tickers_11 = ["IRCTC.NS", "ZOMATO.NS", "NYKAA.NS", "PAYTM.NS", "POLICYBZR.NS"]


results = []
for ticker in tickers_11:
  result = scrape_fundamentals(ticker)
  if result:
      results.append(result)

metrics = ["roce", "roe", "pat", "pe", "market_cap"]

# Flattened list to write
rows = []

for company in results:
    ticker = company["ticker"]
    # Gather all years across the metrics for this company
    all_years = set()
    for metric in metrics:
        all_years.update(company.get(metric, {}).keys())
    
    for year in sorted(all_years):
        row = {
            "companyticker": ticker,
            "year": year,
        }
        for metric in metrics:
            value = company.get(metric, {}).get(year, None)
            # normalize key name for CSV
            csv_key = "marketcap" if metric == "market_cap" else metric
            row[csv_key] = value
        rows.append(row)


# Save to CSV
output_file = "NewFundament11.csv"
with open(output_file, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["companyticker", "year", "roce", "roe", "pat", "pe", "marketcap"])
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Saved {len(rows)} rows to {output_file}")


# Load the CSV file
df = pd.read_csv("/content/New Fund - All.csv")

# Replace NaNs in specific columns with 0
columns_to_fill = ["roce", "roe", "pe", "marketcap", "pat"]
df[columns_to_fill] = df[columns_to_fill].fillna(0)

# (Optional) Save back to CSV
df.to_csv("New-fundamental_data.csv", index=False)
