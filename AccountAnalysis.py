import argparse
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import yfinance as yf


STANDARD_SECTORS = [
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Basic Materials",
    "Real Estate",
    "Technology",
    "Utilities",
]

TICKER_ALIASES = {
    "ticker",
    "symbol",
    "stock",
    "security",
    "holding",
}

AMOUNT_ALIASES = {
    "amount",
    "value",
    "dollars",
    "market value",
    "position value",
    "investment",
    "invested",
    "usd",
}

TYPE_ALIASES = {
    "type",
    "asset type",
    "security type",
    "category",
}


def normalize_header(value: str) -> str:
    return " ".join(str(value).strip().lower().replace("_", " ").split())


def find_column(frame: pd.DataFrame, aliases: set[str]) -> str | None:
    normalized = {normalize_header(column): column for column in frame.columns}
    for alias in aliases:
        if alias in normalized:
            return normalized[alias]
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read an Excel portfolio, look through ETF holdings, and report "
            "overlap/repetition plus missing sectors."
        )
    )
    parser.add_argument("input_file", help="Path to the input Excel workbook")
    parser.add_argument(
        "--sheet",
        help="Sheet name to read. Defaults to the first sheet.",
    )
    parser.add_argument(
        "--output",
        help="Path to the output Excel workbook. Defaults next to the input file.",
    )
    parser.add_argument(
        "--top-holdings",
        type=int,
        default=25,
        help="How many ETF holdings to include per ETF from Yahoo Finance (default: 25).",
    )
    return parser.parse_args()


def read_portfolio(input_file: Path, sheet_name: str | None) -> pd.DataFrame:
    if input_file.suffix.lower() == ".csv":
        frame = pd.read_csv(input_file)
    else:
        frame = pd.read_excel(input_file, sheet_name=sheet_name)
    ticker_col = find_column(frame, TICKER_ALIASES)
    amount_col = find_column(frame, AMOUNT_ALIASES)
    type_col = find_column(frame, TYPE_ALIASES)

    if ticker_col is None or amount_col is None:
        raise ValueError(
            "Your Excel sheet needs a ticker column and an amount/value column. "
            "Examples: Ticker + Amount, or Symbol + Value."
        )

    portfolio = pd.DataFrame(
        {
            "Ticker": frame[ticker_col].astype(str).str.strip().str.upper(),
            "Amount": pd.to_numeric(frame[amount_col], errors="coerce"),
        }
    )

    if type_col is not None:
        portfolio["Declared Type"] = frame[type_col].astype(str).str.strip()
    else:
        portfolio["Declared Type"] = ""

    portfolio = portfolio[(portfolio["Ticker"] != "") & portfolio["Ticker"].notna()]
    portfolio = portfolio[portfolio["Amount"].notna()]
    portfolio = portfolio[portfolio["Amount"] > 0].reset_index(drop=True)

    if portfolio.empty:
        raise ValueError("No usable rows were found after cleaning the Excel data.")

    return portfolio


def percent_to_decimal(value) -> float:
    if value is None or pd.isna(value):
        return 0.0
    numeric = float(value)
    if numeric > 1:
        return numeric / 100.0
    return numeric


def clean_sector_name(raw_sector: str) -> str:
    if not raw_sector:
        return "Unknown"

    sector = str(raw_sector).strip()
    mapping = {
        "consumer cyclical": "Consumer Cyclical",
        "consumer defensive": "Consumer Defensive",
        "financial services": "Financial Services",
        "healthcare": "Healthcare",
        "industrials": "Industrials",
        "basic materials": "Basic Materials",
        "real estate": "Real Estate",
        "technology": "Technology",
        "utilities": "Utilities",
        "energy": "Energy",
        "communication services": "Communication Services",
    }
    return mapping.get(sector.lower(), sector)


class YahooLookup:
    def __init__(self, top_holdings_limit: int):
        self.top_holdings_limit = top_holdings_limit
        self.cache: dict[str, dict] = {}

    def get_security_data(self, symbol: str) -> dict:
        if symbol in self.cache:
            return self.cache[symbol]

        ticker = yf.Ticker(symbol)
        result = {
            "symbol": symbol,
            "name": symbol,
            "security_type": "UNKNOWN",
            "sector": "Unknown",
            "top_holdings": pd.DataFrame(columns=["Symbol", "Name", "Holding Percent"]),
            "sector_weightings": {},
            "error": "",
        }

        try:
            info = ticker.info
        except Exception as exc:
            info = {}
            result["error"] = f"info lookup failed: {exc}"

        if info:
            result["name"] = info.get("shortName") or info.get("longName") or symbol
            result["sector"] = clean_sector_name(info.get("sector"))

        try:
            funds_data = ticker.funds_data
            top_holdings = funds_data.top_holdings.reset_index()
            sector_weightings = {
                clean_sector_name(key): percent_to_decimal(value)
                for key, value in funds_data.sector_weightings.items()
            }

            if not top_holdings.empty:
                top_holdings["Holding Percent"] = top_holdings["Holding Percent"].apply(percent_to_decimal)
                top_holdings = top_holdings.sort_values("Holding Percent", ascending=False)
                result["top_holdings"] = top_holdings.head(self.top_holdings_limit).copy()
                result["sector_weightings"] = sector_weightings
                result["security_type"] = "ETF"
        except Exception as exc:
            if not result["error"]:
                result["error"] = f"fund lookup failed: {exc}"

        if result["security_type"] != "ETF":
            quote_type = str(info.get("quoteType", "")).upper()
            if quote_type in {"EQUITY", "MUTUALFUND"}:
                result["security_type"] = "STOCK" if quote_type == "EQUITY" else "FUND"
            elif result["sector"] != "Unknown":
                result["security_type"] = "STOCK"

        self.cache[symbol] = result
        return result


def analyze_portfolio(portfolio: pd.DataFrame, lookup: YahooLookup) -> dict:
    security_rows = []
    expanded_rows = []
    overlap_sources: dict[str, list[dict]] = defaultdict(list)
    sector_totals: dict[str, float] = defaultdict(float)

    for row in portfolio.to_dict(orient="records"):
        ticker = row["Ticker"]
        amount = float(row["Amount"])
        declared_type = row.get("Declared Type", "")

        security = lookup.get_security_data(ticker)
        security_rows.append(
            {
                "Ticker": ticker,
                "Amount": amount,
                "Declared Type": declared_type,
                "Detected Type": security["security_type"],
                "Name": security["name"],
                "Sector": security["sector"],
                "Lookup Error": security["error"],
            }
        )

        if security["security_type"] == "ETF":
            top_holdings = security["top_holdings"]
            if not top_holdings.empty:
                for holding in top_holdings.to_dict(orient="records"):
                    underlying_ticker = str(holding["Symbol"]).strip().upper()
                    holding_weight = float(holding["Holding Percent"])
                    holding_amount = amount * holding_weight
                    overlap_sources[underlying_ticker].append(
                        {
                            "Source": ticker,
                            "Source Type": "ETF",
                            "Lookthrough Amount": holding_amount,
                        }
                    )
                    expanded_rows.append(
                        {
                            "Portfolio Holding": ticker,
                            "Portfolio Amount": amount,
                            "Exposure Type": "ETF Holding",
                            "Underlying Ticker": underlying_ticker,
                            "Underlying Name": holding["Name"],
                            "Weight In ETF": holding_weight,
                            "Lookthrough Amount": holding_amount,
                        }
                    )

            for sector, weight in security["sector_weightings"].items():
                sector_totals[sector] += amount * weight
        else:
            overlap_sources[ticker].append(
                {
                    "Source": ticker,
                    "Source Type": "Direct",
                    "Lookthrough Amount": amount,
                }
            )
            sector_totals[security["sector"]] += amount
            expanded_rows.append(
                {
                    "Portfolio Holding": ticker,
                    "Portfolio Amount": amount,
                    "Exposure Type": "Direct Holding",
                    "Underlying Ticker": ticker,
                    "Underlying Name": security["name"],
                    "Weight In ETF": 1.0,
                    "Lookthrough Amount": amount,
                }
            )

    overlap_rows = []
    for underlying, sources in overlap_sources.items():
        total = sum(item["Lookthrough Amount"] for item in sources)
        direct_amount = sum(item["Lookthrough Amount"] for item in sources if item["Source Type"] == "Direct")
        etf_amount = total - direct_amount
        distinct_sources = sorted({item["Source"] for item in sources})
        appears_multiple_times = len(distinct_sources) > 1

        if appears_multiple_times:
            overlap_rows.append(
                {
                    "Underlying Ticker": underlying,
                    "Total Lookthrough Amount": total,
                    "Direct Amount": direct_amount,
                    "ETF Amount": etf_amount,
                    "Source Count": len(distinct_sources),
                    "Sources": ", ".join(distinct_sources),
                }
            )

    overlap_frame = pd.DataFrame(overlap_rows).sort_values(
        "Total Lookthrough Amount",
        ascending=False,
    ) if overlap_rows else pd.DataFrame(
        columns=[
            "Underlying Ticker",
            "Total Lookthrough Amount",
            "Direct Amount",
            "ETF Amount",
            "Source Count",
            "Sources",
        ]
    )

    sector_frame = pd.DataFrame(
        [
            {
                "Sector": sector,
                "Estimated Exposure": amount,
            }
            for sector, amount in sector_totals.items()
        ]
    )

    if sector_frame.empty:
        sector_frame = pd.DataFrame(columns=["Sector", "Estimated Exposure", "Portfolio Weight"])
    else:
        total_sector_amount = sector_frame["Estimated Exposure"].sum()
        sector_frame["Portfolio Weight"] = sector_frame["Estimated Exposure"] / total_sector_amount
        sector_frame = sector_frame.sort_values("Estimated Exposure", ascending=False)

    missing_sectors = [sector for sector in STANDARD_SECTORS if sector not in set(sector_frame["Sector"])]
    missing_frame = pd.DataFrame({"Missing Sector": missing_sectors})

    summary_frame = build_summary(
        portfolio=portfolio,
        security_frame=pd.DataFrame(security_rows),
        overlap_frame=overlap_frame,
        sector_frame=sector_frame,
        missing_frame=missing_frame,
    )

    return {
        "summary": summary_frame,
        "portfolio_cleaned": portfolio,
        "security_details": pd.DataFrame(security_rows),
        "expanded_holdings": pd.DataFrame(expanded_rows),
        "overlap": overlap_frame,
        "sector_exposure": sector_frame,
        "missing_sectors": missing_frame,
    }


def build_summary(
    portfolio: pd.DataFrame,
    security_frame: pd.DataFrame,
    overlap_frame: pd.DataFrame,
    sector_frame: pd.DataFrame,
    missing_frame: pd.DataFrame,
) -> pd.DataFrame:
    total_input_amount = float(portfolio["Amount"].sum())
    direct_holdings = int((security_frame["Detected Type"] != "ETF").sum())
    etf_holdings = int((security_frame["Detected Type"] == "ETF").sum())
    overlap_count = int(len(overlap_frame))
    largest_overlap = (
        overlap_frame.iloc[0]["Underlying Ticker"]
        if not overlap_frame.empty
        else "None"
    )
    largest_sector = (
        sector_frame.iloc[0]["Sector"]
        if not sector_frame.empty
        else "None"
    )

    return pd.DataFrame(
        [
            {"Metric": "Total input dollars", "Value": total_input_amount},
            {"Metric": "Portfolio rows", "Value": len(portfolio)},
            {"Metric": "Direct stock/fund rows", "Value": direct_holdings},
            {"Metric": "ETF rows", "Value": etf_holdings},
            {"Metric": "Repeated underlying stocks", "Value": overlap_count},
            {"Metric": "Largest repeated stock", "Value": largest_overlap},
            {"Metric": "Largest sector", "Value": largest_sector},
            {"Metric": "Missing standard sectors", "Value": len(missing_frame)},
        ]
    )


def write_report(output_path: Path, results: dict) -> tuple[Path, str]:
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name, frame in results.items():
                frame.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        return output_path, "excel"
    except ImportError:
        output_dir = output_path.with_suffix("")
        output_dir.mkdir(parents=True, exist_ok=True)
        for sheet_name, frame in results.items():
            frame.to_csv(output_dir / f"{sheet_name}.csv", index=False)
        return output_dir, "csv"


def print_console_summary(results: dict, output_path: Path, output_kind: str) -> None:
    summary = results["summary"]
    overlap = results["overlap"]
    sector_exposure = results["sector_exposure"]
    missing_sectors = results["missing_sectors"]

    print("\nPortfolio analysis complete.")
    if output_kind == "excel":
        print(f"Output workbook: {output_path}")
    else:
        print(f"Output files: {output_path}")
        print("Excel writer was unavailable, so the report was saved as CSV files instead.")
    print()

    for row in summary.to_dict(orient="records"):
        print(f"{row['Metric']}: {row['Value']}")

    print("\nTop repeated stocks:")
    if overlap.empty:
        print("  None found.")
    else:
        for row in overlap.head(10).to_dict(orient="records"):
            print(
                f"  {row['Underlying Ticker']}: total=${row['Total Lookthrough Amount']:,.2f}, "
                f"direct=${row['Direct Amount']:,.2f}, "
                f"via ETFs=${row['ETF Amount']:,.2f}, sources={row['Sources']}"
            )

    print("\nSector exposure:")
    if sector_exposure.empty:
        print("  No sector data found.")
    else:
        for row in sector_exposure.head(10).to_dict(orient="records"):
            print(
                f"  {row['Sector']}: ${row['Estimated Exposure']:,.2f} "
                f"({row['Portfolio Weight']:.1%})"
            )

    print("\nMissing sectors:")
    if missing_sectors.empty:
        print("  None of the standard sectors are missing.")
    else:
        print("  " + ", ".join(missing_sectors["Missing Sector"].tolist()))


def main() -> int:
    args = parse_args()
    input_path = Path(args.input_file).expanduser().resolve()

    if not input_path.exists():
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        return 1

    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else input_path.with_name(f"{input_path.stem}_portfolio_analysis.xlsx")
    )

    try:
        portfolio = read_portfolio(input_path, args.sheet)
        lookup = YahooLookup(top_holdings_limit=args.top_holdings)
        results = analyze_portfolio(portfolio, lookup)
        final_output_path, output_kind = write_report(output_path, results)
        print_console_summary(results, final_output_path, output_kind)
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
