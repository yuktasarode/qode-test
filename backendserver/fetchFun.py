import requests
from bs4 import BeautifulSoup
import time
import random
import os
import json

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

def get_metrics(soup):
    ratios = {}
    ratio_list = soup.select('ul#top-ratios li')

    for li in ratio_list:
        name_tag = li.select_one("span.name")
        number_tag = li.select_one("span.number")

        if name_tag and number_tag:
            key = name_tag.get_text(strip=True)
            val_str = number_tag.get_text(strip=True).replace(",", "")
            try:
                val = float(val_str)
                ratios[key] = val
            except ValueError:
                continue

    return {
        "market_cap": ratios.get("Market Cap"),
        "pe": ratios.get("Stock P/E"),
        "roce": ratios.get("ROCE"),
        "roe": ratios.get("ROE"),
        "pat": None
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


        # for attempt in range(3):
        #     response = requests.get(url, headers=HEADERS, timeout=10)
        #     if response.status_code == 200:
        #         break
        #     delay = random.randint(1, 5)
        #     print(f"⚠️ Attempt {attempt + 1} failed with status {response.status_code}. Retrying in {delay}s...")
        #     time.sleep(delay)

        else:
            raise Exception(f"Failed to fetch data after 3 attempts for {ticker}")

        soup = BeautifulSoup(response.text, "html.parser")


        metrics = get_metrics(soup)
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


df = pd.DataFrame(results)
df.to_csv("fundamentals11.csv", index=False)
print("✅ Saved to fundamentals11.csv")