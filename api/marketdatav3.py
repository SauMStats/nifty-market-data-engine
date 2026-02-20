"""
===========================================================
NIFTY Market Data Engine — v3.0
===========================================================

Purpose:
--------
Professional query layer for historical NIFTY European
options data. Supports multiple years (2024, 2025, 2026+)
and provides clean, pricing-ready output to Pricing,
Hedging, and Volatility Surface teams.

Key Features:
-------------
✅ Multi-year dataset support (2024, 2025, 2026+)
✅ Lazy on-demand loading — no pre-loading of large files
✅ Automatic Spot Price merging via nearest-timestamp join
✅ Flexible query interface: expiry / strike / option type /
   time window / liquidity filter
✅ ATM strike grid generation
✅ Volatility surface snapshot builder (8 expiries × 20 strikes)
✅ Spot caching for repeated queries
✅ Descriptive error messages for missing files and bad inputs

Important Assumptions:
----------------------
1. Bid/Ask quotes are NOT available in this dataset.
2. Option market price is proxied using CLOSE price.
3. Many rows have zero traded volume — apply liquidity
   filters (min_volume) when computing implied volatility.
4. Spot price is matched using nearest-timestamp join
   (pd.merge_asof). Accuracy depends on spot file resolution.

Folder Convention (per year):
------------------------------
base_dir/
  {YEAR}/
    {YEAR}JAN/        ← monthly option folders
    {YEAR}FEB/
    ...
    {YEAR}DEC/
    {YEAR}Nifty/      ← spot price files

Option File Name:
  NIFTY-{EXPIRY}-{TRADEDATE}.csv
  Example: NIFTY-01FEB24-01JAN24.csv

Spot File Name:
  Nifty-{YEAR}{MONTH}.csv
  Example: Nifty-2024JAN.csv

===========================================================
Author : Data Pipeline Team
Version: 3.0
===========================================================
"""

import os
import pandas as pd
from datetime import datetime


# ─────────────────────────────────────────────────────────
# Custom Exceptions
# ─────────────────────────────────────────────────────────

class MarketDataError(Exception):
    """Base exception for all market data errors."""
    pass


class FileNotAvailable(MarketDataError):
    """Raised when a required CSV file does not exist on disk."""
    pass


class NoDataReturned(MarketDataError):
    """Raised when a query returns an empty DataFrame after filtering."""
    pass


class InvalidParameter(MarketDataError):
    """Raised when a query parameter has an invalid value or format."""
    pass


# ─────────────────────────────────────────────────────────
# Main Engine
# ─────────────────────────────────────────────────────────

class NiftyMarketData:
    """
    Market Data Query Engine for Historical NIFTY Options + Spot.

    Supports multiple calendar years. All query methods return
    pandas DataFrames in a standardized schema, with spot price
    merged automatically.

    Typical Usage
    -------------
    from api.marketdatav2 import NiftyMarketData

    md = NiftyMarketData(base_dir=r"G:/SharedDrive/NiftyHistorical")
    df = md.query_options(expiry="01FEB24", trade_date="01JAN24")
    """

    # Supported date format used throughout the engine
    _DATE_FMT = "%d%b%y"    # e.g. "01JAN24"

    def __init__(self, base_dir: str):
        """
        Initialise the engine.

        Parameters
        ----------
        base_dir : str
            Root folder containing one sub-folder per year.
            Example: r"G:/SharedDrive/NiftyHistorical"

        Directory layout expected:
            base_dir/
                2024/
                    2024JAN/ ... 2024DEC/
                    2024Nifty/
                2025/
                    2025JAN/ ... 2025DEC/
                    2025Nifty/
        """
        if not os.path.isdir(base_dir):
            raise FileNotAvailable(
                f"[INIT ERROR] base_dir does not exist or is not accessible:\n"
                f"  → {base_dir}\n"
                f"  Please verify the path and ensure the shared drive is mounted."
            )

        self.base_dir = base_dir

        # Spot data cache: {month_key: DataFrame}
        # month_key format: "2024JAN"
        self._spot_cache: dict = {}

        print(f"[NiftyMarketData] Engine initialised. Base directory: {base_dir}")

    # =========================================================
    # INTERNAL: Path Helpers
    # =========================================================

    def _year_dir(self, year: int) -> str:
        """Return path to the year-level folder (e.g. .../2024/)."""
        return os.path.join(self.base_dir, str(year))

    def _option_dir(self, year: int) -> str:
        """Return path to the options root (year folder)."""
        return os.path.join(self.base_dir, str(year))

    def _spot_dir(self, year: int) -> str:
        """Return path to the spot index folder for a given year."""
        return os.path.join(self.base_dir, str(year), f"{year}Nifty")

    def _parse_trade_date(self, trade_date: str):
        """
        Parse a trade_date string into a datetime.
        Raises InvalidParameter with a helpful message on failure.
        """
        try:
            return datetime.strptime(trade_date, self._DATE_FMT)
        except ValueError:
            raise InvalidParameter(
                f"[PARAMETER ERROR] trade_date '{trade_date}' is not a valid date.\n"
                f"  Required format: DDMMMYY  (e.g. '01JAN24', '15MAR25')"
            )

    def _parse_expiry(self, expiry: str):
        """
        Parse an expiry string into a datetime.
        Raises InvalidParameter with a helpful message on failure.
        """
        try:
            return datetime.strptime(expiry, self._DATE_FMT)
        except ValueError:
            raise InvalidParameter(
                f"[PARAMETER ERROR] expiry '{expiry}' is not a valid date.\n"
                f"  Required format: DDMMMYY  (e.g. '01FEB24', '27MAR25')"
            )

    # =========================================================
    # INTERNAL: Spot Loader + Cache
    # =========================================================

    def _load_spot_month(self, month_key: str) -> pd.DataFrame:
        """
        Load and cache NIFTY spot prices for one month.

        Parameters
        ----------
        month_key : str
            Format: "2024JAN", "2025MAR", etc.

        Returns
        -------
        DataFrame with columns: timestamp, spot_price
        """
        if month_key in self._spot_cache:
            return self._spot_cache[month_key]

        year = int(month_key[:4])
        spot_path = os.path.join(
            self._spot_dir(year), f"Nifty-{month_key}.csv"
        )

        if not os.path.exists(spot_path):
            raise FileNotAvailable(
                f"[FILE NOT FOUND] Spot file is missing for month '{month_key}'.\n"
                f"  Expected path: {spot_path}\n"
                f"  Please contact the Data Pipeline Team if this month's data "
                f"should be available."
            )

        df = pd.read_csv(spot_path)

        if "datetime" not in df.columns or "close" not in df.columns:
            raise MarketDataError(
                f"[FORMAT ERROR] Spot file for '{month_key}' does not contain "
                f"expected columns ('datetime', 'close').\n"
                f"  Found columns: {list(df.columns)}"
            )

        df["timestamp"] = pd.to_datetime(df["datetime"])
        df.rename(columns={"close": "spot_price"}, inplace=True)
        spot_df = df[["timestamp", "spot_price"]].sort_values("timestamp").reset_index(drop=True)

        self._spot_cache[month_key] = spot_df
        return spot_df

    # =========================================================
    # INTERNAL: Option File Loader
    # =========================================================

    def _load_option_file(self, expiry: str, trade_date: str):
        """
        Load one raw option CSV from disk.

        Returns
        -------
        (raw_df : DataFrame, month_key : str)
            raw_df    — the raw CSV as a DataFrame
            month_key — e.g. "2024JAN" (used for spot lookup)
        """
        trade_dt = self._parse_trade_date(trade_date)
        year = trade_dt.year
        month_key = trade_dt.strftime("%Y%b").upper()   # "2024JAN"

        filename = f"NIFTY-{expiry}-{trade_date}.csv"
        path = os.path.join(
            self._option_dir(year), month_key, filename
        )

        if not os.path.exists(path):
            # Give helpful guidance
            raise FileNotAvailable(
                f"[FILE NOT FOUND] Option file not found:\n"
                f"  Expiry    : {expiry}\n"
                f"  TradeDate : {trade_date}\n"
                f"  Expected  : {path}\n\n"
                f"  Possible causes:\n"
                f"    1. This expiry was not traded on {trade_date}.\n"
                f"       → Use md.list_expiries('{trade_date}') to see "
                f"what expiries are available.\n"
                f"    2. The date format may be wrong.\n"
                f"       → Required format: DDMMMYY (e.g. '01FEB24').\n"
                f"    3. Data for this period may not yet be loaded.\n"
                f"       → Contact the Data Pipeline Team."
            )

        df = pd.read_csv(path)
        return df, month_key

    # =========================================================
    # INTERNAL: Standardise Schema
    # =========================================================

    def _standardise(self, df: pd.DataFrame, expiry: str, trade_date: str) -> pd.DataFrame:
        """
        Convert a raw option CSV into the standardised schema.

        Output columns
        --------------
        timestamp, expiry_date, strike, option_type,
        open_price, high_price, low_price, close_price,
        market_price, volume, open_interest
        """
        expiry_dt = self._parse_expiry(expiry)
        trade_dt  = self._parse_trade_date(trade_date)

        # Reconstruct full timestamp from date + intraday time column
        df["timestamp"] = pd.to_datetime(
            trade_dt.strftime("%Y-%m-%d") + " " + df["datetime"].astype(str)
        )

        clean = pd.DataFrame()
        clean["timestamp"]       = df["timestamp"]
        clean["expiry_date"]     = expiry_dt.date()            # plain date; avoids midnight ambiguity
        clean["days_to_expiry"]  = (expiry_dt.date() - trade_dt.date()).days
        clean["strike"]          = df["strike_price"].astype(int)
        clean["option_type"]     = df["right"].replace({"CE": "C", "PE": "P"})
        clean["open_price"]      = df["open"]
        clean["high_price"]      = df["high"]
        clean["low_price"]       = df["low"]
        clean["close_price"]     = df["close"]
        clean["market_price"]    = clean["close_price"]        # pricing proxy
        clean["volume"]          = df["volume"].astype(int)
        clean["open_interest"]   = df["open_interest"].astype(int)

        return clean

    # =========================================================
    # INTERNAL: Spot Merge
    # =========================================================

    def _merge_spot(self, opt_df: pd.DataFrame, month_key: str) -> pd.DataFrame:
        """
        Left-join spot_price onto opt_df using nearest-timestamp matching.
        """
        spot_df = self._load_spot_month(month_key)

        merged = pd.merge_asof(
            opt_df.sort_values("timestamp"),
            spot_df.sort_values("timestamp"),
            on="timestamp",
            direction="nearest"
        )
        return merged

    # =========================================================
    # PUBLIC: Core Query Interface
    # =========================================================

    def query_options(
        self,
        expiry: str,
        trade_date: str,
        strikes: list = None,
        option_type: str = None,
        start: str = None,
        end: str = None,
        min_volume: int = 0,
        raise_if_empty: bool = False
    ) -> pd.DataFrame:
        """
        Primary query interface for the options data engine.

        Fetches all option records for the given expiry and trade date,
        then applies the requested filters. Spot price is merged
        automatically on every call.

        Parameters
        ----------
        expiry : str
            Expiry date in DDMMMYY format.
            Example: '01FEB24', '27MAR25'

        trade_date : str
            The date on which trading occurred, in DDMMMYY format.
            Example: '01JAN24', '15JAN25'

        strikes : list of int, optional
            Filter to specific strike prices.
            Example: [21000, 21500, 22000]
            Default: None → all strikes returned.

        option_type : str, optional
            'C' for Calls, 'P' for Puts.
            Default: None → both returned.

        start : str, optional
            Intraday start time filter (inclusive).
            Format: 'YYYY-MM-DD HH:MM'
            Example: '2024-01-01 09:30'

        end : str, optional
            Intraday end time filter (inclusive).
            Format: 'YYYY-MM-DD HH:MM'
            Example: '2024-01-01 15:30'

        min_volume : int, optional
            Minimum traded volume (contracts). Rows with volume
            strictly below this threshold are excluded.
            Default: 0 → no volume filtering.
            Recommended: 10 or higher for IV calculations.

        raise_if_empty : bool, optional
            If True, raises NoDataReturned when the result is empty.
            Default: False → returns empty DataFrame silently.

        Returns
        -------
        pandas.DataFrame
            Standardised option data with columns:
              timestamp, expiry_date, days_to_expiry, strike,
              option_type, open_price, high_price, low_price,
              close_price, market_price, volume, open_interest,
              spot_price

        Examples
        --------
        # Full option chain for Feb expiry, traded on 1 Jan 2024
        df = md.query_options('01FEB24', '01JAN24')

        # ATM calls only, 10:00–11:00, liquid contracts
        df = md.query_options(
            expiry='01FEB24', trade_date='01JAN24',
            strikes=[21500, 21600, 21700, 21800],
            option_type='C',
            start='2024-01-01 10:00',
            end='2024-01-01 11:00',
            min_volume=10
        )
        """
        # Validate option_type early
        if option_type is not None and option_type not in ("C", "P"):
            raise InvalidParameter(
                f"[PARAMETER ERROR] option_type must be 'C' (Call) or 'P' (Put).\n"
                f"  Received: '{option_type}'"
            )

        raw, month_key = self._load_option_file(expiry, trade_date)
        clean = self._standardise(raw, expiry, trade_date)

        # ── Apply Filters ──────────────────────────────────────
        if strikes is not None:
            if not isinstance(strikes, (list, tuple)):
                raise InvalidParameter(
                    "[PARAMETER ERROR] 'strikes' must be a list of integers.\n"
                    "  Example: strikes=[21000, 21500, 22000]"
                )
            invalid = [s for s in strikes if not isinstance(s, (int, float))]
            if invalid:
                raise InvalidParameter(
                    f"[PARAMETER ERROR] Non-numeric values in strikes list: {invalid}"
                )
            clean = clean[clean["strike"].isin(strikes)]

        if option_type is not None:
            clean = clean[clean["option_type"] == option_type]

        if start is not None:
            try:
                clean = clean[clean["timestamp"] >= pd.to_datetime(start)]
            except Exception:
                raise InvalidParameter(
                    f"[PARAMETER ERROR] 'start' could not be parsed as a datetime.\n"
                    f"  Received: '{start}'\n"
                    f"  Expected format: 'YYYY-MM-DD HH:MM'  (e.g. '2024-01-01 09:30')"
                )

        if end is not None:
            try:
                clean = clean[clean["timestamp"] <= pd.to_datetime(end)]
            except Exception:
                raise InvalidParameter(
                    f"[PARAMETER ERROR] 'end' could not be parsed as a datetime.\n"
                    f"  Received: '{end}'\n"
                    f"  Expected format: 'YYYY-MM-DD HH:MM'  (e.g. '2024-01-01 15:30')"
                )

        if min_volume > 0:
            clean = clean[clean["volume"] >= min_volume]

        # ── Merge Spot ─────────────────────────────────────────
        clean = self._merge_spot(clean, month_key)

        if clean.empty:
            msg = (
                f"[NO DATA] Query returned 0 rows.\n"
                f"  Expiry: {expiry}  |  Trade Date: {trade_date}\n"
                f"  Filters applied:\n"
                f"    strikes      = {strikes}\n"
                f"    option_type  = {option_type}\n"
                f"    time window  = [{start}, {end}]\n"
                f"    min_volume   = {min_volume}\n\n"
                f"  Suggestions:\n"
                f"    → Relax the min_volume filter (many rows have 0 volume).\n"
                f"    → Check available strikes: md.list_strikes('{expiry}', '{trade_date}')\n"
                f"    → Check trading hours: NIFTY trades 09:15–15:30 IST."
            )
            if raise_if_empty:
                raise NoDataReturned(msg)
            else:
                print(msg)

        return clean.reset_index(drop=True)

    # =========================================================
    # PUBLIC: Discovery Helpers
    # =========================================================

    def list_expiries(self, trade_date: str) -> list:
        """
        Return all expiries available for a given trade date.

        Scans the month folder on disk to determine which expiry
        files were actually recorded for this trade date. Use this
        before querying to confirm an expiry exists.

        Parameters
        ----------
        trade_date : str
            Trading date in DDMMMYY format (e.g. '01JAN24').

        Returns
        -------
        list of str
            Sorted list of expiry strings (e.g. ['01FEB24', '25JAN24', ...]).

        Example
        -------
        expiries = md.list_expiries('01JAN24')
        print(expiries)
        # ['01FEB24', '04JAN24', '11JAN24', '25JAN24', '27MAR25']
        """
        trade_dt  = self._parse_trade_date(trade_date)
        year      = trade_dt.year
        month_key = trade_dt.strftime("%Y%b").upper()
        folder    = os.path.join(self._option_dir(year), month_key)

        if not os.path.isdir(folder):
            raise FileNotAvailable(
                f"[FILE NOT FOUND] No data folder found for '{month_key}'.\n"
                f"  Expected: {folder}\n"
                f"  The dataset for this month may not yet be loaded."
            )

        files   = os.listdir(folder)
        expiries = sorted(
            set(
                f.split("-")[1]
                for f in files
                if f.endswith(".csv") and trade_date in f
            )
        )

        if not expiries:
            print(
                f"[INFO] No expiry files found for trade_date='{trade_date}'.\n"
                f"  All files in folder: {files[:5]} ..."
            )

        return expiries

    def list_strikes(self, expiry: str, trade_date: str) -> list:
        """
        Return all strikes available in a given expiry/trade-date file.

        Parameters
        ----------
        expiry     : str  e.g. '01FEB24'
        trade_date : str  e.g. '01JAN24'

        Returns
        -------
        list of int — sorted ascending.

        Example
        -------
        strikes = md.list_strikes('01FEB24', '01JAN24')
        """
        df = self.query_options(expiry, trade_date)
        return sorted(df["strike"].unique().tolist())

    def list_trading_days(self, year: int, month: str) -> list:
        """
        Return a sorted list of trade dates for which data files exist
        in a given year-month folder.

        Parameters
        ----------
        year  : int  e.g. 2024
        month : str  Three-letter abbreviation, uppercase (e.g. 'JAN', 'FEB')

        Returns
        -------
        list of str — trade dates in DDMMMYY format.

        Example
        -------
        days = md.list_trading_days(2024, 'JAN')
        # ['01JAN24', '02JAN24', '03JAN24', ...]
        """
        month_key = f"{year}{month.upper()}"
        folder    = os.path.join(self._option_dir(year), month_key)

        if not os.path.isdir(folder):
            raise FileNotAvailable(
                f"[FILE NOT FOUND] Folder not found: {folder}\n"
                f"  Check that year={year} and month='{month}' are correct."
            )

        files = os.listdir(folder)
        # File pattern: NIFTY-{EXPIRY}-{TRADEDATE}.csv
        trade_dates = sorted(
            set(
                f.split("-")[2].replace(".csv", "")
                for f in files
                if f.startswith("NIFTY") and f.endswith(".csv")
            )
        )
        return trade_dates

    # =========================================================
    # PUBLIC: ATM Strike Grid
    # =========================================================

    def get_atm_strikes(
        self,
        expiry: str,
        trade_date: str,
        n_strikes: int = 10,
        step: int = 100
    ) -> tuple:
        """
        Compute the ATM strike and return a symmetric grid around it.

        The ATM strike is computed as:
            ATM = round(S₀ / step) × step
        where S₀ is the spot price at the start of the trading session.

        Parameters
        ----------
        expiry     : str  e.g. '01FEB24'
        trade_date : str  e.g. '01JAN24'
        n_strikes  : int  Number of strikes each side of ATM (default 10).
                          Total grid size = 2 × n_strikes + 1.
        step       : int  Strike spacing in index points (default 100).

        Returns
        -------
        (atm : int, strike_grid : list of int)

        Example
        -------
        atm, grid = md.get_atm_strikes('01FEB24', '01JAN24', n_strikes=5, step=100)
        print(atm)    # 21700
        print(grid)   # [21200, 21300, 21400, 21500, 21600, 21700,
                      #  21800, 21900, 22000, 22100, 22200]
        """
        df = self.query_options(expiry, trade_date)
        S  = df["spot_price"].iloc[0]
        atm = int(round(S / step) * step)
        grid = [atm + i * step for i in range(-n_strikes, n_strikes + 1)]
        return atm, grid

    # =========================================================
    # PUBLIC: Multi-Day Query
    # =========================================================

    def query_time_series(
        self,
        expiry: str,
        trade_dates: list,
        strikes: list = None,
        option_type: str = None,
        snapshot_time: str = None,
        min_volume: int = 0
    ) -> pd.DataFrame:
        """
        Query the same expiry across multiple trade dates.

        Useful for studying how option prices evolve day-over-day
        as expiry approaches (term structure, time decay analysis).

        Parameters
        ----------
        expiry       : str         e.g. '01FEB24'
        trade_dates  : list of str e.g. ['01JAN24', '02JAN24', '03JAN24']
        strikes      : list of int, optional
        option_type  : str, optional  'C' or 'P'
        snapshot_time: str, optional  HH:MM — if given, filters to that
                       minute only on each date (e.g. '10:00').
        min_volume   : int, optional

        Returns
        -------
        pandas.DataFrame — all dates stacked, with trade_date column added.

        Example
        -------
        df_ts = md.query_time_series(
            expiry='01FEB24',
            trade_dates=['01JAN24', '02JAN24', '03JAN24'],
            option_type='C',
            snapshot_time='10:00'
        )
        """
        frames = []
        for td in trade_dates:
            try:
                # Derive the calendar date from trade_date for time filter
                td_dt  = self._parse_trade_date(td)
                start_ = None
                end_   = None
                if snapshot_time is not None:
                    ts_str = td_dt.strftime("%Y-%m-%d") + " " + snapshot_time
                    start_ = ts_str
                    end_   = ts_str

                df = self.query_options(
                    expiry=expiry,
                    trade_date=td,
                    strikes=strikes,
                    option_type=option_type,
                    start=start_,
                    end=end_,
                    min_volume=min_volume
                )
                df.insert(0, "trade_date", td)
                frames.append(df)

            except (FileNotAvailable, NoDataReturned) as e:
                print(f"[SKIP] trade_date='{td}': {str(e).splitlines()[0]}")

        if not frames:
            raise NoDataReturned(
                "[NO DATA] query_time_series returned no data across all trade dates.\n"
                "  Check that the expiry and trade_dates are correct."
            )

        return pd.concat(frames, ignore_index=True)

    # =========================================================
    # PUBLIC: Volatility Surface Snapshot
    # =========================================================

    def surface_snapshot(
        self,
        trade_date: str,
        timestamp: str,
        n_expiries: int = 8,
        n_strikes: int = 10,
        step: int = 100,
        option_type: str = None,
        min_volume: int = 0
    ) -> pd.DataFrame:
        """
        Build a full volatility surface input grid at a single point in time.

        This is the primary function for the Pricing and Volatility
        Surface teams. It automatically:
          1. Discovers available expiries for trade_date.
          2. Computes ATM and builds a symmetric strike grid for each expiry.
          3. Extracts market prices at the requested timestamp.
          4. Merges spot price.

        Parameters
        ----------
        trade_date  : str   e.g. '01JAN24'
        timestamp   : str   Exact minute snapshot. Format: 'YYYY-MM-DD HH:MM'
                            Example: '2024-01-01 10:00'
        n_expiries  : int   Maximum number of expiries to include (default 8).
        n_strikes   : int   Strikes each side of ATM per expiry (default 10).
                            Total = 2 × n_strikes + 1 per expiry.
        step        : int   Strike spacing (default 100 index points).
        option_type : str   'C', 'P', or None (both). Default: None.
        min_volume  : int   Minimum volume filter (default 0).

        Returns
        -------
        pandas.DataFrame
            Combined grid ready for implied volatility fitting.
            Columns: timestamp, expiry_date, days_to_expiry, strike,
                     option_type, market_price, spot_price, volume, ...

        Example
        -------
        surface = md.surface_snapshot(
            trade_date='01JAN24',
            timestamp='2024-01-01 10:00',
            n_expiries=8,
            n_strikes=10,
            option_type='C'
        )
        """
        expiries = self.list_expiries(trade_date)[:n_expiries]

        if not expiries:
            raise NoDataReturned(
                f"[NO DATA] No expiries found for trade_date='{trade_date}'.\n"
                f"  Use md.list_expiries('{trade_date}') to debug."
            )

        frames = []
        for expiry in expiries:
            try:
                atm, grid = self.get_atm_strikes(
                    expiry, trade_date,
                    n_strikes=n_strikes,
                    step=step
                )

                df = self.query_options(
                    expiry=expiry,
                    trade_date=trade_date,
                    strikes=grid,
                    option_type=option_type,
                    start=timestamp,
                    end=timestamp,
                    min_volume=min_volume
                )

                if not df.empty:
                    frames.append(df)

            except (FileNotAvailable, NoDataReturned):
                continue

        if not frames:
            raise NoDataReturned(
                f"[NO DATA] surface_snapshot returned no data.\n"
                f"  trade_date='{trade_date}', timestamp='{timestamp}'\n"
                f"  Try a different timestamp or reduce min_volume."
            )

        result = pd.concat(frames, ignore_index=True)
        result = result.sort_values(["expiry_date", "strike", "option_type"]).reset_index(drop=True)
        return result

    # =========================================================
    # PUBLIC: Cache Management
    # =========================================================

    def clear_spot_cache(self):
        """
        Clear the in-memory spot price cache.

        Call this if you need to free memory after processing many months,
        or if the underlying spot files have been updated on disk.
        """
        n = len(self._spot_cache)
        self._spot_cache.clear()
        print(f"[CACHE] Spot cache cleared. ({n} month(s) removed)")

    def cache_status(self) -> dict:
        """
        Return the months currently held in the spot price cache.

        Returns
        -------
        dict: {month_key: row_count}
        """
        return {k: len(v) for k, v in self._spot_cache.items()}