CREATE TABLE companies (
  id SERIAL PRIMARY KEY,
  ticker TEXT UNIQUE NOT NULL
);

CREATE TABLE prices (
  id SERIAL PRIMARY KEY,
  company_id INTEGER NOT NULL REFERENCES companies(id),
  year INTEGER NOT NULL,
  price REAL NOT NULL,
  UNIQUE (company_id, year)
);

CREATE TABLE fundamentals (
  id SERIAL PRIMARY KEY,
  company_id INTEGER NOT NULL REFERENCES companies(id),
  year INTEGER NOT NULL,
  roe REAL,
  roce REAL,
  pat BIGINT,
  pe REAL,
  market_cap BIGINT,
  UNIQUE (company_id, year)
);
