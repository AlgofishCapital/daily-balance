import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from enum import Enum


class Session(Enum):
    ASIA = "Asia", [(23, 24), (0, 5)]  # 23:00 - 04:59
    LONDON = "London", [(6, 10)]  # 06:00 - 09:59
    NY_AM = "NY_AM", [(11, 15)]  # 11:00 - 14:59
    NY_PM = "NY_PM", [(20, 23)]  # 20:00 - 22:59
    OTHER = "Other", []

    def __init__(self, session_name, hours_list):
        self.session_name = session_name  # Name of the session
        self.hours_list = hours_list  # List of tuples containing start and end hours


def date_to_milliseconds(date_str):
    """Convert UTC date string to milliseconds since Epoch."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)


def get_historical_klines(symbol, interval, start_str, end_str=None):
    """
    Get Historical Klines from Binance.
    https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data

    :param symbol: Name of the symbol pair e.g., 'BTCUSDT'.
    :param interval: Binance Kline interval e.g., '1h'.
    :param start_str: Start date string in 'YYYY-MM-DD' format.
    :param end_str: End date string in 'YYYY-MM-DD' format.
    :return: list of klines.
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    output_data = []
    limit = 1500  # Maximum limit for Binance API

    # Mapping of interval to milliseconds
    timeframe = {
        "1m": 60 * 1000,
        "3m": 3 * 60 * 1000,
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 1000 * 60,
        "1h": 60 * 60 * 1000,
        "2h": 2 * 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
        "6h": 6 * 60 * 60 * 1000,
        "8h": 8 * 60 * 60 * 1000,
        "12h": 12 * 60 * 60 * 1000,
        "1d": 24 * 60 * 60 * 1000,
        "3d": 3 * 24 * 60 * 60 * 1000,
        "1w": 7 * 24 * 60 * 60 * 1000,
        "1M": 30 * 24 * 60 * 60 * 1000,
    }

    # Convert date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)
    if end_str:
        end_ts = date_to_milliseconds(end_str) + timeframe[interval] - 1
    else:
        end_ts = int(datetime.now().timestamp() * 1000)

    idx = 0
    while True:
        temp_params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": limit,
        }

        response = requests.get(url, params=temp_params)
        data = response.json()

        if not data:
            break

        output_data.extend(data)
        last_open_time = data[-1][0]
        start_ts = last_open_time + timeframe[interval]
        if start_ts >= end_ts:
            break

        idx += 1
        # To respect API rate limits
        time.sleep(0.5)

    return output_data


def assign_sessions(df) -> list[pd.DataFrame]:
    """Assign trading sessions to each row based on 'Open time'."""

    def get_session(row):
        hour = row["Open time"].hour
        for session in Session:
            for start_hour, end_hour in session.hours_list:
                # Handle hour ranges that span over midnight
                if start_hour > end_hour:
                    if hour >= start_hour or hour < end_hour:
                        return session
                else:
                    if start_hour <= hour < end_hour:
                        return session
        return Session.OTHER

    df["Session"] = df.apply(get_session, axis=1)
    return df


def mark_highs_lows_taken(df, target_sessions, target_timeframe) -> list[pd.DataFrame]:
    """
    For each session, determine whether its highs and lows are taken
    during the specified timeframe.

    :param df: DataFrame containing candlestick data with sessions assigned.
    :param target_sessions: List of target sessions to analyze.
    :param target_timeframe: Tuple containing start and end time of the target timeframe in 'HH:MM' format (UTC) inclusive of both times.
    :return: DataFrame with results for each day and session.
    """
    start_time, end_time = target_timeframe
    # Convert start_time and end_time to time objects
    start_hour, start_minute = map(int, start_time.split(":"))
    end_hour, end_minute = map(int, end_time.split(":"))

    # Ensure that end_time is after start_time
    if (end_hour, end_minute) <= (start_hour, start_minute):
        raise ValueError("End time must be after start time.")

    # Prepare results list
    results = []

    # Get unique dates
    unique_dates = df["date"].unique()

    for current_date in unique_dates:
        day_data = df[df["date"] == current_date]

        # Data within the specified timeframe
        timeframe_data = day_data[
            (
                day_data["Open time"].dt.time
                >= datetime.strptime(start_time, "%H:%M").time()
            )
            & (
                day_data["Open time"].dt.time
                <= datetime.strptime(end_time, "%H:%M").time()
            )
        ]

        # If timeframe spans over midnight
        if start_hour > end_hour or (
            start_hour == end_hour and start_minute > end_minute
        ):
            timeframe_data = pd.concat(
                [
                    day_data[
                        day_data["Open time"].dt.time
                        >= datetime.strptime(start_time, "%H:%M").time()
                    ],
                    day_data[
                        day_data["Open time"].dt.time
                        <= datetime.strptime(end_time, "%H:%M").time()
                    ],
                ]
            )

        # Get sessions in the day
        sessions_in_day = day_data["Session"].unique()

        for session in sessions_in_day:
            if (
                session not in target_sessions
                or day_data[day_data["Session"] == session].empty
            ):
                continue

            session_data = day_data[day_data["Session"] == session]

            # Get high and low of the session
            session_high = session_data["High"].max()
            session_low = session_data["Low"].min()

            # Check if session highs/lows are taken during the timeframe
            high_taken = False
            low_taken = False

            if not timeframe_data.empty:
                timeframe_high = timeframe_data["High"].max()
                timeframe_low = timeframe_data["Low"].min()

                if timeframe_high >= session_high:
                    high_taken = True
                if timeframe_low <= session_low:
                    low_taken = True

            results.append(
                {
                    "date": current_date,
                    "Session": session,
                    "Session High": session_high,
                    "Session Low": session_low,
                    "High Taken": high_taken,
                    "Low Taken": low_taken,
                }
            )

    # Create results DataFrame
    results_df = pd.DataFrame(results)
    return results_df


def combine_results(df, target_sessions: list[Session]):
    """
    For each day, combine results for all target sessions.

    Args:
    :param df: DataFrame containing target session data with highs and lows marked.
    :param target_sessions: List of target sessions to analyze.
    """
    # Create seperate columns to store target session results
    for session in target_sessions:
        df[f"{session.session_name} High"] = df.apply(
            lambda row: row["Session High"] if row["Session"] == session else None,
            axis=1,
        )
        df[f"{session.session_name} High Taken"] = df.apply(
            lambda row: row["High Taken"] if row["Session"] == session else None, axis=1
        )
        df[f"{session.session_name} Low"] = df.apply(
            lambda row: row["Session Low"] if row["Session"] == session else None,
            axis=1,
        )
        df[f"{session.session_name} Low Taken"] = df.apply(
            lambda row: row["Low Taken"] if row["Session"] == session else None, axis=1
        )

    # Group by date and aggregate results
    combined_results = (
        df.groupby("date")
        .agg(
            {
                "Session High": "max",
                "Session Low": "min",
                "High Taken": "all",
                "Low Taken": "all",
            }
        )
        .reset_index()
    )

    # Add duplicated columns for each target session
    for session in target_sessions:
        combined_results[f"{session.session_name} High"] = (
            df.groupby("date")[f"{session.session_name} High"].max().values
        )
        combined_results[f"{session.session_name} High Taken"] = (
            df.groupby("date")[f"{session.session_name} High Taken"].all().values
        )
        combined_results[f"{session.session_name} Low"] = (
            df.groupby("date")[f"{session.session_name} Low"].min().values
        )
        combined_results[f"{session.session_name} Low Taken"] = (
            df.groupby("date")[f"{session.session_name} Low Taken"].all().values
        )

    # A full balance is achieved when both the High and Lows are Taken
    combined_results["Full Balance"] = (
        combined_results["High Taken"] & combined_results["Low Taken"]
    )

    # Rename columns
    combined_results.rename(
        columns={
            "Session High": "Target High",
            "Session Low": "Target Low",
            "High Taken": "Target High Taken",
            "Low Taken": "Target Low Taken",
        },
        inplace=True,
    )

    return combined_results


def main(
    symbol: str,
    query_start: str,
    query_end: str,
    target_sessions: list[Session],
    target_timeframe: tuple[str, str],
):
    """_summary_

    Args:
    :param symbol: Name of the symbol pair e.g., 'BTCUSDT'.
    :param query_start: Start date string in 'YYYY-MM-DD' format.
    :param query_end: End date string in 'YYYY-MM-DD' format.
    :param target_sessions: List of target sessions to analyze.
    :param target_timeframe: Tuple containing start and end time of the target timeframe in 'HH:MM' format (UTC) inclusive of both times.
    :returm: DataFrame with results for each day.
    """
    interval = "1h"

    klines = get_historical_klines(symbol, interval, query_start, query_end)

    # Define column names
    columns = [
        "Open time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Close time",
        "Quote asset volume",
        "Number of trades",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
        "Ignore",
    ]

    # Create DataFrame
    df = pd.DataFrame(klines, columns=columns)

    # Convert numeric columns to appropriate data types
    numeric_columns = [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Quote asset volume",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
    ]
    df[numeric_columns] = df[numeric_columns].astype(float)

    # Drop unused columns
    df.drop(
        columns=[
            "Quote asset volume",
            "Number of trades",
            "Taker buy base asset volume",
            "Taker buy quote asset volume",
            "Ignore",
        ],
        inplace=True,
    )

    # Convert time columns to datetime
    df["Open time"] = pd.to_datetime(df["Open time"], unit="ms", utc=True)
    df["Close time"] = pd.to_datetime(df["Close time"], unit="ms", utc=True)

    # Add 'date' field (date part of 'Open time')
    df["date"] = df["Open time"].dt.date

    # Assign trading sessions
    df = assign_sessions(df)

    # Mark highs and lows for target sessions
    sessions_df = mark_highs_lows_taken(df, target_sessions, target_timeframe)

    # Combine session data per day
    daily_df = combine_results(sessions_df, target_sessions)

    # Output the results
    print(daily_df)

    # Optionally, save to CSV
    # results_df.to_csv('session_high_low_taken.csv', index=False)


if __name__ == "__main__":
    symbol = "BTCUSDT"  # Symbol pair from Binance see https://fapi.binance.com/fapi/v1/ticker/price

    start_str = "2024-01-01"  # Query data from
    end_str = "2024-09-16"  # Query data until

    target_sessions = [Session.ASIA, Session.LONDON]
    target_timeframe = (
        "11:00",
        "23:00",
    )  # Specify the timeframe, this should be outside of the target sessions range

    main(symbol, start_str, end_str, target_sessions, target_timeframe)
