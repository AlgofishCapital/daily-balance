import pandas as pd
from datetime import datetime, timedelta

from daily_balance.daily_balance import (
    Session,
    assign_sessions,
    determine_h1_trend_per_row,
)


def test_assign_sessions():
    data = {
        "Open time": [
            datetime(2023, 1, 1, 0, 0),  # Asia
            datetime(2023, 1, 1, 6, 0),  # London
            datetime(2023, 1, 1, 11, 0),  # NY AM
            datetime(2023, 1, 1, 20, 0),  # NY PM
            datetime(2023, 1, 1, 23, 0),  # Asia
            datetime(2023, 1, 1, 19, 0),  # NA
        ],
        "High": [1, 2, 3, 4, 5, 6],
        "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
        "Close": [0.8, 1.8, 2.8, 3.8, 4.8, 5.8],
        "Volume": [100, 100, 100, 100, 100, 100],
    }
    df = pd.DataFrame(data)

    # Assign sessions
    df = assign_sessions(df)

    # Check the assigned sessions
    assert df["Session"].iloc[0] == Session.ASIA
    assert df["Session"].iloc[1] == Session.LONDON
    assert df["Session"].iloc[2] == Session.NY_AM
    assert df["Session"].iloc[3] == Session.NY_PM
    assert df["Session"].iloc[4] == Session.ASIA
    assert df["Session"].iloc[5] == Session.OTHER
