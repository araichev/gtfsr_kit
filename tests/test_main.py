import datetime as dt

import gtfs_kit as gk
import pandas as pd
import pytest
from google.transit.gtfs_realtime_pb2 import FeedMessage

from .context import DATA_DIR, GTFSR_DIR, GTFS_PATH, date
from gtfsr_kit import (
    DATETIME_FORMAT,
    build_augmented_stop_times,
    combine_delays,
    dict_to_feed,
    extract_delays,
    feed_to_dict,
    get_timestamp_str,
    interpolate_delays,
    read_feed,
    str_to_timestamp,
    timestamp_to_str,
    write_feed,
)


JSON_PATH = DATA_DIR / "tripUpdates_short.json"
PB_PATH = DATA_DIR / "tripUpdates.pb"

JSON_FEED = read_feed(JSON_PATH, from_json=True)
PB_FEED = read_feed(PB_PATH)
GTFS_FEED = gk.read_feed(GTFS_PATH, dist_units="km")
GTFSR_FEEDS = [read_feed(path, from_json=True) for path in sorted(GTFSR_DIR.iterdir())]

DELAY_COLUMNS = [
    "route_id",
    "trip_id",
    "stop_sequence",
    "stop_id",
    "arrival_delay",
    "departure_delay",
]


def test_read_feed():
    # protobuf input
    feed = read_feed(PB_PATH)
    assert isinstance(feed, FeedMessage)

    # json input
    feed = read_feed(JSON_PATH, from_json=True)
    assert isinstance(feed, FeedMessage)

    # both fixtures should contain the same logical message type
    assert feed.DESCRIPTOR.full_name == "transit_realtime.FeedMessage"


def test_write_feed(tmp_path):
    # protobuf round-trip
    pb_out = tmp_path / "tripUpdates.pb"
    write_feed(PB_FEED, pb_out, to_json=False)
    pb_roundtrip = read_feed(pb_out, from_json=False)
    assert pb_roundtrip == PB_FEED

    # json round-trip
    json_out = tmp_path / "tripUpdates.json"
    write_feed(JSON_FEED, json_out, to_json=True)
    json_roundtrip = read_feed(json_out, from_json=True)
    assert json_roundtrip == JSON_FEED


def test_feed_to_dict():
    data = feed_to_dict(JSON_FEED)
    assert isinstance(data, dict)
    assert set(data) == {"header", "entity"}

    empty_data = feed_to_dict(FeedMessage())
    assert isinstance(empty_data, dict)
    assert set(empty_data).issubset({"header", "entity"})

def test_dict_to_feed():
    # round-trip populated feed
    rebuilt = dict_to_feed(feed_to_dict(JSON_FEED))
    assert rebuilt == JSON_FEED

    # round-trip empty feed
    empty = FeedMessage()
    rebuilt_empty = dict_to_feed(feed_to_dict(empty))
    assert rebuilt_empty == empty


def test_timestamp_to_str():
    # default format
    t = 69
    s = timestamp_to_str(t)
    assert isinstance(s, str)
    assert s == "1970-01-01T00:01:09"

    # custom format
    custom = timestamp_to_str(t, "%Y%m%d%H%M%S")
    assert custom == "19700101000109"


def test_str_to_timestamp():
    # default format
    s = "1970-01-01T00:01:09"
    assert str_to_timestamp(s) == 69

    # custom format
    s_custom = "19700101000109"
    assert str_to_timestamp(s_custom, "%Y%m%d%H%M%S") == 69

    # return type should be int
    assert isinstance(str_to_timestamp(s), int)


def test_get_timestamp_str():
    # empty feed timestamp defaults to epoch
    empty_value = get_timestamp_str(FeedMessage())
    assert isinstance(empty_value, str)
    assert empty_value == "1970-01-01T00:00:00"

    # populated feed should still produce a string parseable by the helper
    populated_value = get_timestamp_str(JSON_FEED)
    assert isinstance(populated_value, str)
    parsed = dt.datetime.strptime(populated_value, DATETIME_FORMAT)
    assert isinstance(parsed, dt.datetime)


def test_extract_delays():
    # empty feed
    empty_delays = extract_delays(FeedMessage())
    assert isinstance(empty_delays, pd.DataFrame)
    assert empty_delays.empty
    assert empty_delays.columns.tolist() == DELAY_COLUMNS

    # populated feed
    delays = extract_delays(JSON_FEED)
    assert isinstance(delays, pd.DataFrame)
    assert delays.columns.tolist() == DELAY_COLUMNS

    # sorted by route/trip/stop_sequence
    expected = delays.sort_values(
        ["route_id", "trip_id", "stop_sequence"], kind="stable"
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(delays, expected, check_dtype=False)

    # no row should have both delay values missing
    assert not (delays["arrival_delay"].isna() & delays["departure_delay"].isna()).all()


def test_combine_delays():
    delays = extract_delays(JSON_FEED)

    # empty input
    empty = combine_delays([])
    assert isinstance(empty, pd.DataFrame)
    assert empty.empty
    assert empty.columns.tolist() == DELAY_COLUMNS

    # duplicate identical inputs should collapse to one copy
    combined = combine_delays([delays, delays])
    assert isinstance(combined, pd.DataFrame)
    assert combined.columns.tolist() == DELAY_COLUMNS
    pd.testing.assert_frame_equal(combined, delays, check_dtype=False)

    # later non-null values should win within the same key
    patched = delays.copy()
    first_idx = patched.index[0]
    patched.loc[first_idx, "arrival_delay"] = 123.0
    combined_patched = combine_delays([delays, patched])
    assert combined_patched.loc[0, "arrival_delay"] == 123.0


def test_build_augmented_stop_times():
    stop_times = gk.get_stop_times(GTFS_FEED, date)
    expected_columns = stop_times.columns.tolist() + ["arrival_delay", "departure_delay"]

    # no realtime feeds
    without_rt = build_augmented_stop_times([], GTFS_FEED, date)
    assert isinstance(without_rt, pd.DataFrame)
    assert without_rt.columns.tolist() == expected_columns
    assert without_rt.shape[0] == stop_times.shape[0]
    assert without_rt["arrival_delay"].isna().all()
    assert without_rt["departure_delay"].isna().all()

    # with realtime feeds
    with_rt = build_augmented_stop_times(GTFSR_FEEDS, GTFS_FEED, date)
    assert isinstance(with_rt, pd.DataFrame)
    assert with_rt.columns.tolist() == expected_columns
    assert with_rt.shape[0] == stop_times.shape[0]

    # output should be sorted by trip_id/stop_sequence
    expected = with_rt.sort_values(["trip_id", "stop_sequence"], kind="stable").reset_index(drop=True)
    pd.testing.assert_frame_equal(with_rt, expected, check_dtype=False)


def test_interpolate_delays():
    augmented = build_augmented_stop_times(GTFSR_FEEDS, GTFS_FEED, date)

    # single-column interpolation
    one_col = interpolate_delays(
        augmented,
        dist_threshold=1,
        delay_cols=["arrival_delay"],
    )
    assert isinstance(one_col, pd.DataFrame)
    assert one_col.columns.tolist() == augmented.columns.tolist()
    assert one_col.shape == augmented.shape

    for _, group in one_col.groupby("trip_id", dropna=False):
        count = group["arrival_delay"].count()
        assert count in (0, len(group))

    # two-column interpolation
    both_cols = interpolate_delays(
        augmented,
        dist_threshold=1,
        delay_cols=["arrival_delay", "departure_delay"],
    )
    assert isinstance(both_cols, pd.DataFrame)
    assert both_cols.columns.tolist() == augmented.columns.tolist()
    assert both_cols.shape == augmented.shape

    for _, group in both_cols.groupby("trip_id", dropna=False):
        for col in ["arrival_delay", "departure_delay"]:
            count = group[col].count()
            assert count in (0, len(group))

    # invalid delay column should raise
    with pytest.raises(ValueError, match="missing delay columns"):
        interpolate_delays(
            augmented,
            dist_threshold=1,
            delay_cols=["not_a_real_delay_col"],
        )