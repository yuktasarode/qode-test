from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, func, Table, MetaData, select, and_, text, inspect
from pydantic import BaseModel
from dateutil.relativedelta import relativedelta
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import PlainTextResponse, StreamingResponse
import time
import random
from datetime import datetime
from dotenv import load_dotenv
import os
from io import BytesIO
import zipfile


# Create FastAPI app
app = FastAPI()

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return PlainTextResponse("Rate limit exceeded", status_code=429)



# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can replace "*" with specific domains in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------ Database Connection ------------------
# Replace with your actual credentials
# Load .env file
load_dotenv()

# Get the DB URL
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)

metadata = MetaData()
companies = Table("companies", metadata, autoload_with=engine)
fundamentals = Table("fundamentals", metadata, autoload_with=engine)
prices=Table("prices",metadata,autoload_with=engine)

inspector = inspect(engine)
tables = inspector.get_table_names()
print("Tables in database:", tables)

save_dir = "data/tmp"
os.makedirs(save_dir, exist_ok=True)


# /ping route (GET)
@app.get("/ping")
async def ping():
    return {"message": "pong"}

# Define input model for /echo POST body
class EchoData(BaseModel):
    message: str = None 

class BacktestConfig(BaseModel):
    initial_capital: float
    start_date: str
    end_date: str
    rebalance_frequency: str  # "monthly", "quarterly", "yearly"
    position_sizing: str      # "equal", "market_cap", "roce"
    portfolio_size: int
    market_cap_min: float
    market_cap_max: float
    roce: float
    pat: float
    ranking: str  # "roe:desc,pe:asc"
    compranking: str



def calculate_metrics(portfolio: pd.DataFrame):
    returns = portfolio['value'].pct_change().dropna()

    if len(returns) < 2:
        return {
            "cagr": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0
        }

    # Calculate number of years between first and last point
    months = len(portfolio)
    cagr = ((portfolio['value'].iloc[-1] / portfolio['value'].iloc[0]) ** (1 / (months / 12))) - 1
    sharpe = returns.mean() / returns.std() * np.sqrt(12)
    drawdown = (portfolio['value'] / portfolio['value'].cummax()) - 1
    max_drawdown = drawdown.min()
    portfolio['drawdown'] = drawdown

    return {
        "cagr": round(cagr * 100, 2),
        "sharpe": round(sharpe, 2),
        "max_drawdown": round(max_drawdown * 100, 2)
    }


# /echo route (POST)
@app.post("/echo")
async def echo(data: EchoData):
    return {"received": data.dict()}



def convert_dates_to_period(start: str, end: str) -> str:
    valid_periods = [
        ("1d", 1),
        ("5d", 5),
        ("1mo", 30),
        ("3mo", 90),
        ("6mo", 180),
        ("1y", 365),
        ("2y", 730),
        ("5y", 1825),
        ("10y", 3650),
    ]

    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(start, date_format)
    end_date = datetime.strptime(end, date_format)
    delta_days = (end_date - start_date).days

    for period, days in valid_periods:
        if delta_days <= days:
            return period
    return "max"  # fallback


def safe_download(tickers, start, end, retries=3, delay=1):
    for attempt in range(retries):
        try:
            print("Fetching from Yahoo")
            
            period = convert_dates_to_period(start, end)
            data = yf.download(tickers=tickers, start=start, end=end)["Close"]
            filename = f"prices_{tickers[0]}_{start}_{end}.csv".replace(":", "-")
            file_path = os.path.join(save_dir, filename)
            data.to_csv(file_path)
            print("Data yf downloaded")

            return data
        except Exception as e:
            wait_time = delay * (2 ** attempt) + random.random()
            print(f"[Retry {attempt + 1}] Error fetching data: {e} | Retrying in {round(wait_time, 2)}s...")
            time.sleep(wait_time)
    raise Exception(f"Failed to fetch data for {tickers} after {retries} retries.")


def ranking_logic(fundamentals_df, config):
    ranking_criteria = [r.strip() for r in config.ranking.split(',') if ':' in r]
    rankings = []

    for metric_order in ranking_criteria:
        metric, order = metric_order.split(':')
        ascending = True if order == 'asc' else False
        fundamentals_df[f'rank_{metric}'] = fundamentals_df[metric].rank(ascending=ascending)
        rankings.append(fundamentals_df[f'rank_{metric}'])

    if config.compranking == 'yes' and len(rankings) > 1:
        fundamentals_df['composite_rank'] = sum(rankings) / len(rankings)
    else:
        fundamentals_df['composite_rank'] = rankings[0]  # Use only first metric

    print("composite done")
    top_ranked_df = fundamentals_df.sort_values('composite_rank').head(config.portfolio_size)
    ranked_tickers = top_ranked_df['ticker'].tolist()

    return top_ranked_df, ranked_tickers

    
def fetch_rebalance_dates(start,end,config):
    start = pd.to_datetime(config.start_date)
    end = pd.to_datetime(config.end_date)
    rebalance_freq = {
        "monthly": relativedelta(months=1),
        "quarterly": relativedelta(months=3),
        "yearly": relativedelta(years=1),
    }.get(config.rebalance_frequency, relativedelta(months=1))

    rebalance_dates = []
    current = start
    while current < end:
        rebalance_dates.append(current)
        current += rebalance_freq
    rebalance_dates.append(end)

    return rebalance_dates


def fetch_fundamentals(year_cutoff,config):
    # Step 1: Fetch fundamentals based on user filters
        # Step 2: Subquery to get latest year for each company before or equal to start_year
        latest_fundamentals_subq = (
            select(
                fundamentals.c.company_id,
                func.max(fundamentals.c.year).label("max_year")
            )
            .where(fundamentals.c.year <= year_cutoff)
            .group_by(fundamentals.c.company_id)
            .alias("latest_f")
        )
        stmt = (
            select(
                companies.c.ticker,
                fundamentals.c.company_id,
                fundamentals.c.roce,
                fundamentals.c.pat,
                fundamentals.c.roe,
                fundamentals.c.pe,
                fundamentals.c.market_cap,
                fundamentals.c.year
            )
            .select_from(
                fundamentals
                .join(latest_fundamentals_subq,
                    and_(
                        fundamentals.c.company_id == latest_fundamentals_subq.c.company_id,
                        fundamentals.c.year == latest_fundamentals_subq.c.max_year
                    ))
                .join(companies, fundamentals.c.company_id == companies.c.id)
            )
            .where(
                and_(
                    fundamentals.c.roce >= config.roce,
                    fundamentals.c.pat >= config.pat,
                    fundamentals.c.market_cap.between(config.market_cap_min, config.market_cap_max)
                )
            )
        )

        with engine.connect() as conn:
            
            fundamentals_df = pd.read_sql(stmt, conn)
            print("Query returned:", len(fundamentals_df), "rows")


        if fundamentals_df.empty:
            raise Exception("No companies match the filter criteria.")

        print("Fetch Fundamentals")
        return fundamentals_df

def allocate_weights(top_ranked_df,tickers_this_period, config):
    
    if config.position_sizing == 'equal':
        weights = {ticker: 1 / len(ticker) for ticker in tickers_this_period}
    elif config.position_sizing == 'market_cap':
        caps = top_ranked_df.set_index('ticker').loc[tickers_this_period, 'market_cap']
        total = caps.sum()
        weights = {ticker: caps[ticker] / total for ticker in tickers_this_period}
    elif config.position_sizing in ['roce', 'roe']:
        vals = top_ranked_df.set_index('ticker').loc[tickers_this_period, config.position_sizing]
        total = vals.sum()
        weights = {ticker: vals[ticker] / total for ticker in tickers_this_period}
    else:
        weights = {ticker: 1 / len(ticker) for ticker in tickers_this_period}

    return weights

def exportconfig(run_id,config): 

    config_df = pd.DataFrame([{
    "run_id": run_id,
    "start_date": config.start_date,
    "end_date": config.end_date,
    "rebalance_frequency": config.rebalance_frequency,
    "portfolio_size": config.portfolio_size,
    "ranking": config.ranking,
    "compranking": config.compranking,
    "position_sizing": config.position_sizing,
    "initial_capital": config.initial_capital,
    "roce": config.roce,
    "pat": config.pat,
    "market_cap_min": config.market_cap_min,
    "market_cap_max": config.market_cap_max,
    "run_date": datetime.now()
    }])
    config_df.to_csv(f"data/exports/{run_id}_config.csv", index=False)


@app.post("/run-backtest")
@limiter.limit("5/minute")
def run_backtest(request: Request,config: BacktestConfig):
    try:
        run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") 
        exportconfig(run_id,config)
        start_year = pd.to_datetime(config.start_date).year
        end_year = pd.to_datetime(config.end_date).year
        rebalance_dates = fetch_rebalance_dates(start_year,end_year,config)
        print("-" * 50)
        print("Rebalance dates:", rebalance_dates)
        print("No of rebalances:", len(rebalance_dates))
        
        portfolio_history = []
        portfolio_composition_records=[]
        top_ranked_records=[]
        capital = config.initial_capital

        for i in range(len(rebalance_dates) - 1):
            print("Rebalance No:", i)
            year_cutoff = rebalance_dates[i].year
            period_start = rebalance_dates[i].strftime('%Y-%m-%d')
            period_end = rebalance_dates[i + 1].strftime('%Y-%m-%d')

            fundamentals_df = fetch_fundamentals(year_cutoff, config)
            top_ranked_df,tickers = ranking_logic(fundamentals_df, config)

            for _, row in top_ranked_df.iterrows():
                top_ranked_records.append({
                    "run_id": run_id,
                    "date": period_start,
                    "ticker": row["ticker"],
                    "composite_rank": row["composite_rank"],
                    "roce": row["roce"],
                    "roe": row["roe"],
                    "market_cap": row["market_cap"]
                })

            print(f"Period: {period_start} to {period_end}")
            print(f"Fundamentals columns: {len(fundamentals_df)}")
            print(f"Top-ranked tickers: {tickers}")
            

            try:
                price_data = safe_download(tickers, period_start, period_end)

            except Exception:
                continue

            if isinstance(price_data, pd.Series):
                price_data = price_data.to_frame()

            # Drop tickers with missing data in the period
            price_data = price_data.dropna(axis=1, how='any')
            if price_data.empty:
                continue

            print(f"Price data after dropna columns: {price_data.columns.tolist()}")
            print(f"Price data is empty? {price_data.empty}")

            weights=allocate_weights(top_ranked_df,price_data.columns.tolist(), config)

            # Get start and end prices
            try:
                start_prices = price_data.iloc[0]
                end_prices = price_data.iloc[-1]
            except Exception:
                continue

            # Calculate number of shares per ticker
            shares = {
                    ticker: (capital * weights[ticker]) / start_prices[ticker]
                    if start_prices[ticker] != 0 else 0
                    for ticker in weights
                }
            end_value = sum(shares[ticker] * end_prices[ticker] for ticker in weights)

            
            tickers_this_period = price_data.columns.tolist()
            for ticker in tickers_this_period:
                portfolio_composition_records.append({
                    "run_id": run_id,
                    "date": period_start,
                    "ticker": ticker,
                    "weight": weights[ticker],
                    "shares": shares[ticker],
                    "start_price": start_prices[ticker],
                    "end_price": end_prices[ticker],
                    "value": shares[ticker] * end_prices[ticker]
                })

            

            print("Capital Values")
            capital = end_value
            portfolio_history.append({
                "date": price_data.index[-1].strftime('%Y-%m-%d'),
                "value": round(end_value, 2)
            })

        portfoliocsv_df = pd.DataFrame(portfolio_composition_records)
        portfoliocsv_df.to_csv(f"data/exports/{run_id}_portfolio_composition.csv", index=False)

        top_companies_df = pd.DataFrame(top_ranked_records)
        top_companies_df.to_csv(f"data/exports/{run_id}_top_companies.csv", index=False)

        # Step 6: Metrics
        portfolio_df = pd.DataFrame(portfolio_history)
        metrics = calculate_metrics(portfolio_df)
        portfolio_df["drawdown"] = (portfolio_df["value"] / portfolio_df["value"].cummax()) - 1


        return {
            "run_id": run_id,
            "equity_curve": portfolio_df[["date", "value"]].to_dict(orient="records"),
            "drawdown_curve": portfolio_df[["date", "drawdown"]].round(4).to_dict(orient="records"),
            "metrics": metrics
        }

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export-backtest")
def export_backtest(run_id: str):
    try:
        print("run id", run_id)
        folder = "data/exports"
        files = [
            f"{folder}/{run_id}_portfolio_composition.csv",
            f"{folder}/{run_id}_top_companies.csv",
            f"{folder}/{run_id}_config.csv"
        ]

        # Create an in-memory ZIP file
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for filepath in files:
                filename = os.path.basename(filepath)
                zipf.write(filepath, arcname=filename)
        zip_buffer.seek(0)

        return StreamingResponse(
            zip_buffer,
            media_type="application/x-zip-compressed",
            headers={"Content-Disposition": f"attachment; filename={run_id}_backtest_export.zip"}
        )
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/compute-nifty")
@limiter.limit("5/minute")
def compute_nifty(request: Request, config: BacktestConfig):
    try:
        rebalance_dates = fetch_rebalance_dates(config.start_date, config.end_date, config)

        print("Nifty50 Rebalance Dates:", rebalance_dates)
        print(type(config.start_date), config.end_date)

        data = yf.download("^NSEI", start=config.start_date, end=config.end_date)["Close"]
        print(data.head())
        if isinstance(data, pd.DataFrame):
            data = data["^NSEI"] 

        result = []

        for date in rebalance_dates:
            try:
                # Exact match
                if date in data.index:
                    close_price = data.loc[date]
                else:
                    # Try previous available date
                    past_data = data[data.index < date]
                    if not past_data.empty:
                        close_price = past_data.iloc[-1]
                    else:
                        # No past data, assign 0
                        print(f"No previous data available for {date}, setting value to 0")
                        close_price = 0

                result.append({
                    "date": date.strftime('%Y-%m-%d'),
                    "value": round(close_price, 2) if isinstance(close_price, (int, float, np.number)) else 0
                })

                print(date,close_price)

            except Exception as e:
                print(f"Error on {date}: {e}")
                result.append({
                    "date": date.strftime('%Y-%m-%d'),
                    "value": 0
                })
                continue

        print(result)
        return result

    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
