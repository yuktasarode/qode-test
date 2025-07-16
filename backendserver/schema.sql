CREATE TABLE companies (
  id SERIAL PRIMARY KEY,
  ticker TEXT UNIQUE NOT NULL
);

CREATE TABLE fundamentals (
  id SERIAL PRIMARY KEY,
  company_id INTEGER REFERENCES companies(id),
  roce FLOAT,
  pat FLOAT,
  roe FLOAT,
  pe FLOAT,
  market_cap FLOAT,
  date DATE
);

CREATE TABLE prices (
  date DATE,
  ticker TEXT,
  close FLOAT
);
