from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, func
from pydantic import BaseModel
from dateutil.relativedelta import relativedelta
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import PlainTextResponse
import time
from sqlalchemy import Table, MetaData, select, and_, text
import random
from datetime import datetime
from dotenv import load_dotenv
import os
from sqlalchemy import inspect


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
            data = yf.download(tickers=tickers, period=period)["Close"]
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


@app.post("/run-backtest")
@limiter.limit("5/minute")
def run_backtest(request: Request,config: BacktestConfig):
    try:
        print(config)
        start_year = pd.to_datetime(config.start_date).year
        end_year = pd.to_datetime(config.end_date).year
        # Step 1: Fetch fundamentals based on user filters
        # Step 2: Subquery to get latest year for each company before or equal to start_year
        latest_fundamentals_subq = (
            select(
                fundamentals.c.company_id,
                func.max(fundamentals.c.year).label("max_year")
            )
            .where(fundamentals.c.year <= start_year)
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
        # Step 2: Rank based on provided criteria
        ranking_criteria = [r.strip() for r in config.ranking.split(',') if ':' in r]
        rankings = []

        print(ranking_criteria)

        for metric_order in ranking_criteria:
            metric, order = metric_order.split(':')
            ascending = True if order == 'asc' else False
            fundamentals_df[f'rank_{metric}'] = fundamentals_df[metric].rank(ascending=ascending)
            rankings.append(fundamentals_df[f'rank_{metric}'])

        print("ranking:", rankings)
        if config.compranking == 'yes' and len(rankings) > 1:
            fundamentals_df['composite_rank'] = sum(rankings) / len(rankings)
        else:
            fundamentals_df['composite_rank'] = rankings[0]  # Use only first metric

        print("composote done")
        top_ranked_df = fundamentals_df.sort_values('composite_rank').head(config.portfolio_size)
        ranked_tickers = top_ranked_df['ticker'].tolist()

        # Step 3: Download price data
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

        print("Rebalance dates", rebalance_dates)
        # Step 4: Portfolio allocation
        # Step 4: Backtest
        portfolio_history = []
        capital = config.initial_capital

        for i in range(len(rebalance_dates) - 1):
            period_start = rebalance_dates[i].strftime('%Y-%m-%d')
            period_end = rebalance_dates[i + 1].strftime('%Y-%m-%d')

            try:
                print("Type of tickers",ranked_tickers)
                price_data = safe_download(ranked_tickers, period_start, period_end)

            except Exception:
                continue

            if isinstance(price_data, pd.Series):
                price_data = price_data.to_frame()

            # Drop tickers with missing data in the period
            price_data = price_data.dropna(axis=1, how='any')
            if price_data.empty:
                continue

            # Rebuild weights for current rebalance
            tickers_this_period = price_data.columns.tolist()
            n = len(tickers_this_period)

            if config.position_sizing == 'equal':
                weights = {ticker: 1 / n for ticker in tickers_this_period}
            elif config.position_sizing == 'market_cap':
                caps = top_ranked_df.set_index('ticker').loc[tickers_this_period, 'market_cap']
                total = caps.sum()
                weights = {ticker: caps[ticker] / total for ticker in tickers_this_period}
            elif config.position_sizing in ['roce', 'roe']:
                vals = top_ranked_df.set_index('ticker').loc[tickers_this_period, config.position_sizing]
                total = vals.sum()
                weights = {ticker: vals[ticker] / total for ticker in tickers_this_period}
            else:
                weights = {ticker: 1 / n for ticker in tickers_this_period}

            # Get start and end prices
            try:
                start_prices = price_data.iloc[0]
                end_prices = price_data.iloc[-1]
            except Exception:
                continue

            # Calculate number of shares per ticker
            shares = {ticker: (capital * weights[ticker]) / start_prices[ticker] for ticker in tickers_this_period}
            end_value = sum(shares[ticker] * end_prices[ticker] for ticker in tickers_this_period)

            capital = end_value
            portfolio_history.append({
                "date": price_data.index[-1].strftime('%Y-%m-%d'),
                "value": round(end_value, 2)
            })

        # Step 6: Metrics
        portfolio_df = pd.DataFrame(portfolio_history)
        metrics = calculate_metrics(portfolio_df)
        portfolio_df["drawdown"] = (portfolio_df["value"] / portfolio_df["value"].cummax()) - 1


        return {
            "equity_curve": portfolio_df[["date", "value"]].to_dict(orient="records"),
            "drawdown_curve": portfolio_df[["date", "drawdown"]].round(4).to_dict(orient="records"),
            "metrics": metrics
        }


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/export-backtest")
def export_backtest():
    df = pd.read_sql("SELECT * FROM backtest_results", con=engine)
    csv = df.to_csv(index=False)
    return Response(content=csv, media_type="text/csv", headers={
        "Content-Disposition": "attachment; filename=backtest.csv"
    })
