# NIFTY Market Data Engine — v3.0

> **Data Pipeline Team** | Mathematical Finance Group Project  
> Historical NIFTY 50 Options + Spot Query API

---

## What This Is

This is a professional Python API that gives Pricing, Hedging, and Volatility Surface teams clean, filtered, spot-merged NIFTY options data — without ever touching a raw CSV file.

You write a query. You get a DataFrame. That's it.

---

## Quick Start

### 1. Requirements

```bash
pip install pandas
```

Python 3.8+ required.

### 2. Place the module

Copy `marketdatav2.py` into your project's `api/` folder:

```
your_project/
    api/
        __init__.py       ← create this (can be empty)
        marketdatav2.py   ← the engine
    your_notebook.ipynb
```

### 3. Import and initialise

```python
from api.marketdatav2 import NiftyMarketData

BASE_DIR = r"G:/SharedDrive/NiftyHistorical"   # path to shared dataset root
md = NiftyMarketData(base_dir=BASE_DIR)
```

### 4. Run your first query

```python
df = md.query_options(expiry="01FEB24", trade_date="01JAN24")
print(df.head())
```

---

## Dataset Structure Expected

```
base_dir/
    2024/
        2024JAN/          ← option files for January 2024
        2024FEB/
        ...
        2024DEC/
        2024Nifty/        ← spot index files
    2025/
        2025JAN/
        ...
        2025Nifty/
    2026/
        ...
```

**Option file naming:**
```
NIFTY-{EXPIRY}-{TRADEDATE}.csv
Example: NIFTY-01FEB24-01JAN24.csv
```

**Spot file naming:**
```
Nifty-{YEAR}{MONTH}.csv
Example: Nifty-2024JAN.csv
```

---

## API Reference

### Core Query

```python
df = md.query_options(
    expiry="01FEB24",         # Expiry date    — DDMMMYY format
    trade_date="01JAN24",     # Trading date   — DDMMMYY format
    strikes=[21500, 21700],   # Optional: specific strikes (list of int)
    option_type="C",          # Optional: "C" (Call) or "P" (Put)
    start="2024-01-01 10:00", # Optional: intraday start (inclusive)
    end="2024-01-01 11:00",   # Optional: intraday end (inclusive)
    min_volume=10,            # Optional: minimum volume filter
    raise_if_empty=False      # Optional: raise exception if empty
)
```

**Returns DataFrame with columns:**
`timestamp, expiry_date, days_to_expiry, strike, option_type,`  
`open_price, high_price, low_price, close_price, market_price,`  
`volume, open_interest, spot_price`

---

### Discovery Helpers

```python
# What expiries traded on this date?
expiries = md.list_expiries("01JAN24")

# What strikes exist for this expiry/date?
strikes = md.list_strikes("01FEB24", "01JAN24")

# What days had trading in this month?
days = md.list_trading_days(2024, "JAN")
```

---

### ATM Strike Grid

```python
atm, grid = md.get_atm_strikes(
    expiry="01FEB24",
    trade_date="01JAN24",
    n_strikes=10,   # strikes each side of ATM
    step=100        # strike spacing
)
# Returns: (21700, [21200, 21300, ..., 21700, ..., 22200])
```

---

### Volatility Surface Snapshot

```python
surface = md.surface_snapshot(
    trade_date="01JAN24",
    timestamp="2024-01-01 10:00",
    n_expiries=8,      # max expiries to include
    n_strikes=10,      # strikes each side of ATM per expiry
    step=100,
    option_type="C",   # "C", "P", or None (both)
    min_volume=0
)
```

Direct input for Black-Scholes IV fitting:
```python
S     = surface["spot_price"]
K     = surface["strike"]
T     = surface["days_to_expiry"] / 365.0
C_mkt = surface["market_price"]
```

---

### Multi-Day Time Series

```python
df_ts = md.query_time_series(
    expiry="01FEB24",
    trade_dates=["01JAN24", "02JAN24", "03JAN24"],
    strikes=[21700],
    option_type="C",
    snapshot_time="10:00",   # HH:MM — optional single-minute snapshot
    min_volume=0
)
```

---

## Date Format Reference

| Format | Example | Used For |
|--------|---------|----------|
| `DDMMMYY` | `01JAN24`, `27MAR25` | `expiry`, `trade_date` |
| `YYYY-MM-DD HH:MM` | `2024-01-01 10:00` | `start`, `end`, `timestamp` |

---

## Important Assumptions

| # | Assumption |
|---|------------|
| 1 | Bid/Ask quotes are **not available**. Market price is proxied by close price. |
| 2 | Many rows have **zero volume**. Use `min_volume ≥ 10` for IV calculations. |
| 3 | Spot price is joined using **nearest-timestamp matching**. |
| 4 | Dataset covers **2024 onwards**. 2025 and 2026 folders follow the same structure. |

---

## Error Types

| Exception | When raised |
|-----------|-------------|
| `FileNotAvailable` | CSV file not found on disk |
| `NoDataReturned` | Query returned 0 rows (when `raise_if_empty=True`) |
| `InvalidParameter` | Bad format for `option_type`, dates, or `strikes` |

All exceptions print a detailed message with suggested fixes.

---

## For the Pricing Team: Typical Workflow

```python
# Step 1: Initialise
md = NiftyMarketData(base_dir=BASE_DIR)

# Step 2: Discover what's available
expiries = md.list_expiries("01JAN24")

# Step 3: Get ATM-centred strikes
atm, grid = md.get_atm_strikes(expiries[0], "01JAN24", n_strikes=10)

# Step 4: Pull surface snapshot
surface = md.surface_snapshot(
    trade_date="01JAN24",
    timestamp="2024-01-01 10:00",
    n_expiries=8,
    option_type="C",
    min_volume=0
)

# Step 5: Pass to your IV solver
S, K, T, C_mkt = (
    surface["spot_price"],
    surface["strike"],
    surface["days_to_expiry"] / 365.0,
    surface["market_price"]
)
```

---

## Contact

**Data Pipeline Team** — responsible for maintaining this engine and the dataset.  
If a file is missing or a query fails unexpectedly, contact the Data Pipeline Team before modifying the raw data.

---

*Last updated: January 2026 | Version 3.0*