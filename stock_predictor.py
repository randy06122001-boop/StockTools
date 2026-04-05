import argparse
import sys

import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.linear_model import LinearRegression


def download_history(ticker: str, period: str) -> pd.DataFrame:
    data = yf.download(ticker, period=period, interval="1d", auto_adjust=True, progress=False)
    if data.empty:
        raise ValueError(
            f"No price data found for ticker '{ticker}'. "
            "Try the market-specific symbol, for example Samsung is usually 005930.KS."
        )
    return data


def extract_close_series(history: pd.DataFrame) -> pd.Series:
    close_data = history["Close"]

    if isinstance(close_data, pd.DataFrame):
        if close_data.shape[1] != 1:
            raise ValueError(
                f"Expected one close-price column for a single ticker, but found {close_data.shape[1]}."
            )
        close_series = close_data.iloc[:, 0]
    else:
        close_series = close_data

    close_series = close_series.dropna()
    if close_series.empty:
        raise ValueError("No usable closing-price values were returned.")

    return close_series


def build_features(close_series: pd.Series, lookback: int) -> tuple[np.ndarray, np.ndarray]:
    frame = pd.DataFrame({"Close": close_series}).copy()

    for i in range(1, lookback + 1):
        frame[f"lag_{i}"] = frame["Close"].shift(i)

    frame["target"] = frame["Close"]
    frame = frame.dropna()

    feature_columns = [f"lag_{i}" for i in range(1, lookback + 1)]
    x = frame[feature_columns].to_numpy()
    y = frame["target"].to_numpy()
    return x, y


def predict_next_close(close_series: pd.Series, lookback: int) -> tuple[float, float]:
    if len(close_series) <= lookback:
        raise ValueError(
            f"Not enough price history to build a model with lookback={lookback}. "
            f"Need more than {lookback} trading days."
        )

    x, y = build_features(close_series, lookback)
    model = LinearRegression()
    model.fit(x, y)

    latest_window = np.array([close_series.iloc[-lookback:][::-1].to_list()])
    prediction = float(model.predict(latest_window)[0])

    # A quick in-sample score so the script can show whether the model is
    # at least loosely fitting the recent historical pattern.
    score = float(model.score(x, y))
    return prediction, score


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Predict the next trading day's closing price using a simple linear regression model."
    )
    parser.add_argument("ticker", help="Ticker symbol, e.g. AAPL or 005930.KS for Samsung Electronics")
    parser.add_argument(
        "--period",
        default="1y",
        help="Amount of recent daily history to use from Yahoo Finance (default: 1y)",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=5,
        help="Number of prior closing prices used as model inputs (default: 5)",
    )
    args = parser.parse_args()

    try:
        history = download_history(args.ticker, args.period)
        closes = extract_close_series(history)
        prediction, score = predict_next_close(closes, args.lookback)

        latest_close = float(closes.iloc[-1])
        latest_date = closes.index[-1].strftime("%Y-%m-%d")

        print(f"Ticker: {args.ticker}")
        print(f"Latest close ({latest_date}): {latest_close:.2f}")
        print(f"Predicted next close: {prediction:.2f}")
        print(f"Model R^2 on recent data: {score:.3f}")
        print()
        print("This is a simple educational model based only on recent closing prices.")
        print("It is not reliable enough for real trading decisions.")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
