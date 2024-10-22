import pandas as pd
from datetime import datetime, timedelta

from daily_balance.daily_balance import (
    Session,
    enrich_data,
)


# def test_assign_sessions():
#     data = {
#         "Open time": [
#             datetime(2023, 1, 1, 0, 0),  # Asia
#             datetime(2023, 1, 1, 6, 0),  # London
#             datetime(2023, 1, 1, 11, 0),  # NY AM
#             datetime(2023, 1, 1, 20, 0),  # NY PM
#             datetime(2023, 1, 1, 23, 0),  # Asia
#             datetime(2023, 1, 1, 19, 0),  # NA
#         ],
#         "High": [1, 2, 3, 4, 5, 6],
#         "Low": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
#         "Close": [0.8, 1.8, 2.8, 3.8, 4.8, 5.8],
#         "Volume": [100, 100, 100, 100, 100, 100],
#     }
#     df = pd.DataFrame(data)

#     # Assign sessions
#     df = assign_sessions(df)

#     # Check the assigned sessions
#     assert df["Session"].iloc[0] == Session.ASIA
#     assert df["Session"].iloc[1] == Session.LONDON
#     assert df["Session"].iloc[2] == Session.NY_AM
#     assert df["Session"].iloc[3] == Session.NY_PM
#     assert df["Session"].iloc[4] == Session.ASIA
#     assert df["Session"].iloc[5] == Session.OTHER


def test_enrich_data():
    data = {
        "Open time": [  # Initial Time in UTC
            datetime(2023, 1, 1, 23, 0),  # Asia
            datetime(2023, 1, 2, 4, 0),  # London
            datetime(2023, 1, 2, 10, 0),  # NY AM
            datetime(2023, 1, 2, 16, 0),  # NY PM
            datetime(2023, 1, 2, 22, 0),  # Asia
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

    # Check the enriched data
    assert enriched_df["Open time"].iloc[0] == pd.Timestamp(
        "2023-01-01 18:00:00-0500", tz="America/New_York"
    )
    assert enriched_df["Open time"].iloc[1] == pd.Timestamp(
        "2023-01-01 23:00:00-0500", tz="America/New_York"
    )
    assert enriched_df["Open time"].iloc[2] == pd.Timestamp(
        "2023-01-02 05:00:00-0500", tz="America/New_York"
    )
    assert enriched_df["Open time"].iloc[3] == pd.Timestamp(
        "2023-01-02 11:00:00-0500", tz="America/New_York"
    )
    assert enriched_df["Open time"].iloc[4] == pd.Timestamp(
        "2023-01-02 17:00:00-0500", tz="America/New_York"
    )
    assert enriched_df["Open time"].iloc[5] == pd.Timestamp(
        "2023-01-02 14:00:00-0500", tz="America/New_York"
    )

    assert enriched_df["Session"].iloc[0] == Session.ASIA
    assert enriched_df["Session"].iloc[1] == Session.LONDON
    assert enriched_df["Session"].iloc[2] == Session.NY_AM
    assert enriched_df["Session"].iloc[3] == Session.NY_PM
    assert enriched_df["Session"].iloc[4] == Session.ASIA
    assert enriched_df["Session"].iloc[5] == Session.OTHER

    assert enriched_df["Weekday"].iloc[0] == 6  # Sunday
    assert enriched_df["Weekday"].iloc[1] == 6  # Sunday
    assert enriched_df["Weekday"].iloc[2] == 6  # Sunday
    assert enriched_df["Weekday"].iloc[3] == 6  # Sunday
    assert enriched_df["Weekday"].iloc[4] == 6  # Sunday
    assert enriched_df["Weekday"].iloc[5] == 6  # Sunday
