import requests
import pandas as pd
from datetime import datetime, timedelta
import time as t
from enum import Enum


class Session(Enum):
    # Session Times in UTC
    ASIA = (
        "Asia",
        [("23:00", "03:59")],
    )  # Asia session spans over midnight, at 23:00 UTC, the new day starts
    LONDON = "London", [("06:00", "08:59")]
    NY_AM = "NY_AM", [("11:00", "13:59")]
    NY_PM = "NY_PM", [("20:00", "21:59")]
    OTHER = "Other", []

    def __init__(self, session_name, time_ranges):
        self.session_name = session_name  # Name of the session
        self.time_ranges = time_ranges  # List of tuples containing start and end times in 'HH:MM' format


def date_to_milliseconds(date_str):
    """Convert UTC date string to milliseconds since Epoch."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)


def get_historical_klines(
    symbol: str, interval: str, start_str: str, end_str: str = None
) -> list[list[str]]:
    """
    Get Historical Klines from Binance.
    https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data

    :param symbol: Name of the symbol pair e.g., 'BTCUSDT'.
    :param interval: Binance Kline interval e.g., '15m'.
    :param start_str: Start date string in 'YYYY-MM-DD' format (UTC).
    :param end_str: End date string in 'YYYY-MM-DD' format (UTC).
    :return: list of klines.
    """
    url = "https://fapi.binance.com/fapi/v1/klines"
    limit = 1500  # Maximum limit for Binance API

    # Mapping of interval to milliseconds
    timeframe = {
        "1m": 60 * 1000,
        "3m": 3 * 60 * 1000,
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
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
        end_ts = int(datetime.utcnow().timestamp() * 1000)

    idx = 0
    output_data = []
    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": limit,
        }

        response = requests.get(url, params=params)

        # Check for valid response
        if response.status_code != 200:
            raise ValueError(f"Failed to fetch data: {response.text}")

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
        t.sleep(0.5)

    return output_data


def etl_ohlcv(
    symbol: str, interval: str, query_start: str, query_end: str
) -> pd.DataFrame:
    """
    Extracts historical kline data for a given symbol and interval,
    Transforms it into a pandas DataFrame, enriched with additional derived information.
    Loads it into the kitchen sink

    :param symbol: Name of the symbol pair e.g., 'BTCUSDT'.
    :param interval: Binance Kline interval e.g., '15m'.
    :param query_start: Start date string in 'YYYY-MM-DD' format (UTC).
    :param query_end: End date string in 'YYYY-MM-DD' format (UTC).
    :return: pd.DataFrame: A DataFrame containing the transformed kline data with the following columns:
        - Open time (datetime): The opening time of the kline in UTC.
        - Close time (datetime): The closing time of the kline in UTC.
        - Open (float): The opening price.
        - High (float): The highest price.
        - Low (float): The lowest price.
        - Close (float): The closing price.
        - Volume (float): The trading volume.
        - Date (date): The date part of the 'Open time' in UTC.
        - Trading Date: The trading date, see Session
        - Trading Weekday (int): The weekday of the 'Trading Date'.
        - Session (Session): The trading Session of the kline.
    """
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

    # Convert timestamps to datetime objects
    df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
    df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")

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

    # Enrich DataFrame with additional data fields
    enriched_df = enrich_data(df)

    return enriched_df


def enrich_data(df) -> pd.DataFrame:
    """
    Enrich the DataFrame with additional data fields.
    - Date (date): The date part of the 'Open time'.
    - Trading Date: The trading date, see Session.
    - Trading Weekday (int): The weekday of the 'Trading Date', see Session.
    - Session (Session): The trading Session of the kline.
    """

    def get_session(row):
        time_of_day = row["Open time"].time()
        for session in Session:
            for start_str, end_str in session.time_ranges:
                start_time = datetime.strptime(start_str, "%H:%M").time()
                end_time = datetime.strptime(end_str, "%H:%M").time()

                # Handle time ranges that span over midnight
                if start_time > end_time:
                    if time_of_day >= start_time or time_of_day <= end_time:
                        return session
                else:
                    if start_time <= time_of_day <= end_time:
                        return session
        return Session.OTHER

    # Assign Date, Trading Date, Trading Weekday, and Session
    df["Date"] = df["Open time"].dt.date
    df["Session"] = df.apply(get_session, axis=1)

    # Adjust Trading Date for sessions that span over midnight
    df["Trading Date"] = df.apply(
        lambda row: row["Date"]
        if row["Session"] != Session.ASIA or row["Open time"].hour < 23
        else row["Date"] + timedelta(days=1),
        axis=1,
    )
    df["Trading Weekday"] = df["Trading Date"].apply(lambda date: date.weekday())

    return df


def mark_highs_lows_taken(df, target_sessions, target_timeframe) -> pd.DataFrame:
    """
    For each session in target_sessions, determine whether its highs and lows are taken
    during the specified timeframe (target_timeframe).

    :param df: DataFrame containing candlestick data with sessions assigned.
    :param target_sessions: List of Sessions to analyse as part of target_timeframe
    :param target_timeframe: Tuple containing start and end time of the target timeframe in 'HH:MM' format (UTC) inclusive of both times.
    :return: pd.DataFrame: A DataFrame with results for each day and session with the following columns:
        - Date (date): The Trading Date of the session.
        - Session (Session): The session of the day.
        - Session High (float): The highest price of the session.
        - Session Low (float): The lowest price of the session.
        - High Taken: Whether the session high is taken during the target timeframe.
        - Low Taken: Whether the session low is taken during the target timeframe.

    """
    start_time_str, end_time_str = target_timeframe
    # Convert start_time and end_time to time objects
    start_time = datetime.strptime(start_time_str, "%H:%M").time()
    end_time = datetime.strptime(end_time_str, "%H:%M").time()

    # Prepare results list
    results = []

    # Get unique dates
    unique_dates = df["Trading Date"].unique()

    for current_date in unique_dates:
        day_data = df[df["Trading Date"] == current_date]

        # Data within the specified timeframe
        timeframe_data = day_data[
            (day_data["Open time"].dt.time >= start_time)
            & (day_data["Open time"].dt.time <= end_time)
        ]

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
    # Create separate columns to store target session results
    for session in target_sessions:
        df[f"{session.session_name} High"] = df.apply(
            lambda row: row["Session High"] if row["Session"] == session else None,
            axis=1,
        )
        df[f"{session.session_name} High Taken"] = df.apply(
            lambda row: row["High Taken"] if row["Session"] == session else None,
            axis=1,
        )
        df[f"{session.session_name} Low"] = df.apply(
            lambda row: row["Session Low"] if row["Session"] == session else None,
            axis=1,
        )
        df[f"{session.session_name} Low Taken"] = df.apply(
            lambda row: row["Low Taken"] if row["Session"] == session else None,
            axis=1,
        )

    # Group by date and aggregate results
    combined_results = df.groupby("date").first().reset_index()

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
    output_csv: bool = False,
):
    """
    Main function to analyze and process OHLCV data for a given symbol within a specified date range and target sessions.
    Target sessions are the list of sessions where the highs and lows are expected to be taken during the Target Timeframe.

    Args:
    :param symbol: Name of the symbol pair e.g., 'BTCUSDT'.
    :param query_start: Backtesting Start date string in 'YYYY-MM-DD' format (UTC).
    :param query_end: Backtesting End date string in 'YYYY-MM-DD' format (UTC).
    :param target_sessions: List of target sessions to analyze.
    :param target_timeframe: Tuple containing start and end time of the target timeframe in 'HH:MM' format (UTC) inclusive of both times.
    :return: DataFrame with results for each day.
    """
    interval = "15m"  # Interval is '15m'

    input_df = etl_ohlcv(symbol, interval, query_start, query_end)

    # Converts [interval] data into sessions data, with highs and lows for each
    sessions_df = mark_highs_lows_taken(input_df, target_sessions, target_timeframe)

    # Combine session data per day
    results_df = combine_results(sessions_df, target_sessions)

    # Output the results
    print(results_df)

    # Optionally, save to CSV
    if output_csv:
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"daily_balance-{current_time}.csv"
        results_df.to_csv(file_name, index=False)


if __name__ == "__main__":
    symbol = "BTCUSDT"  # Symbol pair from Binance

    start_str = "2023-09-01"  # Query data from (UTC)
    end_str = "2023-09-16"  # Query data until (UTC)

    target_sessions = [Session.ASIA, Session.LONDON]
    target_timeframe = (
        "11:00",
        "23:00",
    )  # Specify the timeframe in UTC

    main(symbol, start_str, end_str, target_sessions, target_timeframe)
