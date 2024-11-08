import pandas as pd
from datetime import datetime

from daily_balance.daily_balance import (
    Session,
    enrich_data,
    mark_highs_lows_taken,
)


def test_enrich_data():
    data = {
        "Open time": [  # Initial Time in UTC
            datetime(2023, 1, 1, 23, 0),  # Asia
            datetime(2023, 1, 2, 6, 0),  # London
            datetime(2023, 1, 2, 11, 0),  # NY AM
            datetime(2023, 1, 2, 20, 0),  # NY PM
            datetime(2023, 1, 2, 23, 0),  # Asia
            datetime(2023, 1, 2, 19, 0),  # NA
        ],
        "High": [1, 2, 3, 4, 5, 6],
        "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
        "Close": [0.8, 1.8, 2.8, 3.8, 4.8, 5.8],
        "Volume": [100, 100, 100, 100, 100, 100],
    }
    df = pd.DataFrame(data)

    # Enrich data
    enriched_df = enrich_data(df)

    # Assertions
    assert "Date" in enriched_df.columns
    assert "Session" in enriched_df.columns
    assert "Trading Date" in enriched_df.columns
    assert "Trading Weekday" in enriched_df.columns

    # Check session assignment
    assert enriched_df.iloc[0]["Session"] == Session.ASIA
    assert enriched_df.iloc[1]["Session"] == Session.LONDON
    assert enriched_df.iloc[2]["Session"] == Session.NY_AM
    assert enriched_df.iloc[3]["Session"] == Session.NY_PM
    assert enriched_df.iloc[5]["Session"] == Session.OTHER

    # Check weekday values
    assert enriched_df.iloc[0]["Trading Weekday"] == 0  # Monday
    assert enriched_df.iloc[1]["Trading Weekday"] == 0
    assert enriched_df.iloc[4]["Trading Weekday"] == 1  # Tuesday


def test_mark_highs_lows_taken():
    data = {
        "Open time": [  # Initial Time in UTC
            datetime(2023, 1, 1, 23, 0),  # Asia
            datetime(2023, 1, 2, 0, 0),  # Asia
            datetime(2023, 1, 2, 6, 0),  # London
            datetime(2023, 1, 2, 7, 0),  # London
            datetime(2023, 1, 2, 11, 0),  # NY AM
            datetime(2023, 1, 2, 12, 0),  # NY AM
            datetime(2023, 1, 2, 16, 0),  # Other
            datetime(2023, 1, 2, 17, 0),  # Other
            datetime(2023, 1, 2, 20, 0),  # NY PM
            datetime(2023, 1, 2, 21, 0),  # NY PM
        ],
        "High": [1, 2, 3, 4, 5, 6, 7, 6, 5, 4],
        "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 5.5, 4.5, 3.5],
        "Close": [0.8, 1.8, 2.8, 3.8, 4.8, 5.8],
        "Volume": [100, 100, 100, 100, 100, 100],
    }
    df = pd.DataFrame(data)

    # Enrich data
    enriched_df = enrich_data(df)

    # Define target sessions and timeframe
    target_sessions = [Session.ASIA, Session.LONDON]
    target_timeframe = ("11:00", "23:00")

    # Mark highs and lows taken
    results_df = mark_highs_lows_taken(enriched_df, target_sessions, target_timeframe)

    # Assertions
    assert "date" in results_df.columns
    assert "Session" in results_df.columns
    assert "Session High" in results_df.columns
    assert "Session Low" in results_df.columns
    assert "High Taken" in results_df.columns
    assert "Low Taken" in results_df.columns

    # Check results for specific sessions
    asia_result = results_df[results_df["Session"] == Session.ASIA]
    london_result = results_df[results_df["Session"] == Session.LONDON]

    assert not asia_result.empty
    assert not london_result.empty

    assert asia_result.iloc[0]["Session High"] == 1
    assert asia_result.iloc[0]["Session Low"] == 0.5
    assert asia_result.iloc[0]["High Taken"] is False
    assert asia_result.iloc[0]["Low Taken"] is False

    assert london_result.iloc[0]["Session High"] == 2
    assert london_result.iloc[0]["Session Low"] == 1.5
    assert london_result.iloc[0]["High Taken"] is True
    assert london_result.iloc[0]["Low Taken"] is True
