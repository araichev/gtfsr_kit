from __future__ import annotations

import datetime as dt
from os import PathLike
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import gtfs_kit as gk
import numpy as np
import pandas as pd
from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
DELAY_COLUMNS = ["arrival_delay", "departure_delay"]
DELAY_KEY_COLUMNS = ["route_id", "trip_id", "stop_sequence"]

Pathish = str | PathLike[str] | Path
FeedMessage = gtfs_realtime_pb2.FeedMessage


def read_feed(path: Pathish, *, from_json: bool = False) -> FeedMessage:
    """
    Read a GTFS-Realtime feed from the given path and return it as a FeedMessage.

    If ``from_json``, then parse the file as protobuf JSON text.
    Otherwise, parse it as serialized protobuf bytes.
    """
    path = Path(path)
    feed = gtfs_realtime_pb2.FeedMessage()

    if from_json:
        with path.open("r", encoding="utf-8") as src:
            feed = json_format.Parse(src.read(), feed)
    else:
        with path.open("rb") as src:
            feed.ParseFromString(src.read())

    return feed


def write_feed(feed: FeedMessage, path: Pathish, *, to_json: bool = False) -> None:
    """
    Write the given GTFS-Realtime feed to the given path.

    If ``to_json``, then write the feed as protobuf JSON text.
    Otherwise, write it as serialized protobuf bytes.
    """
    path = Path(path)

    if to_json:
        with path.open("w", encoding="utf-8") as tgt:
            tgt.write(json_format.MessageToJson(feed))
    else:
        with path.open("wb") as tgt:
            tgt.write(feed.SerializeToString())


def feed_to_dict(feed: FeedMessage) -> dict[str, Any]:
    """
    Convert the given GTFS-Realtime feed to a dictionary.

    The conversion uses the standard protobuf JSON mapping, so the result
    matches the structure you would get by serializing the message to JSON
    and then parsing that JSON into ordinary Python objects.
    """
    return json_format.MessageToDict(feed)


def dict_to_feed(feed_dict: dict[str, Any]) -> FeedMessage:
    """
    Convert the given dictionary to a GTFS-Realtime feed.

    The input is interpreted according to the standard protobuf JSON mapping.
    """
    return json_format.ParseDict(feed_dict, gtfs_realtime_pb2.FeedMessage())


def timestamp_to_str(t: int, datetime_format: str = DATETIME_FORMAT) -> str:
    """
    Format a POSIX timestamp as a UTC datetime string.

    The timestamp is interpreted in UTC, not in local time.
    The default format is ``YYYY-MM-DDTHH:MM:SS``.
    """
    return dt.datetime.fromtimestamp(int(t), tz=dt.timezone.utc).strftime(datetime_format)


def str_to_timestamp(t: str, datetime_format: str = DATETIME_FORMAT) -> int:
    """
    Parse a UTC datetime string and return its POSIX timestamp in seconds.

    The input string is interpreted in UTC.
    The return value is an integer number of whole seconds since the Unix epoch.
    """
    parsed = dt.datetime.strptime(t, datetime_format).replace(tzinfo=dt.timezone.utc)
    return int(parsed.timestamp())


def get_timestamp_str(feed: FeedMessage, datetime_format: str = DATETIME_FORMAT) -> str:
    """
    Return the feed header timestamp as a UTC datetime string.

    An empty feed has a timestamp of zero, so in that case the result
    corresponds to the Unix epoch in UTC.
    """
    return timestamp_to_str(int(feed.header.timestamp), datetime_format)


def _empty_delays_frame() -> pd.DataFrame:
    """
    Return an empty delay table with the columns

    - ``'route_id'``
    - ``'trip_id'``
    - ``'stop_sequence'``
    - ``'stop_id'``
    - ``'arrival_delay'``
    - ``'departure_delay'``.

    This helper is used so that callers receive a predictable schema even
    when no trip updates or no usable delay values are present.
    """
    return pd.DataFrame(
        {
            "route_id": pd.Series(dtype="string"),
            "trip_id": pd.Series(dtype="string"),
            "stop_sequence": pd.Series(dtype="Int64"),
            "stop_id": pd.Series(dtype="string"),
            "arrival_delay": pd.Series(dtype="Float64"),
            "departure_delay": pd.Series(dtype="Float64"),
        }
    )


def _require_columns(frame: pd.DataFrame, required: list[str], *, name: str) -> None:
    """
    Raise ``ValueError`` if the given DataFrame is missing any of the given
    required columns.

    The ``name`` argument is used to make the error message more specific.
    """
    missing = [col for col in required if col not in frame.columns]
    if missing:
        missing_fmt = ", ".join(repr(col) for col in missing)
        raise ValueError(f"{name} is missing required columns: {missing_fmt}")


def _cast_delay_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Return a delay table with validated columns and normalized dtypes.

    The given DataFrame must contain the columns


    - ``'route_id'``
    - ``'trip_id'``
    - ``'stop_sequence'``
    - ``'stop_id'``
    - ``'arrival_delay'``
    - ``'departure_delay'``.

    The result has the same columns, with Pandas nullable dtypes so that
    missing values remain explicit without forcing the table onto plain
    object dtype.
    """
    _require_columns(
        frame,
        ["route_id", "trip_id", "stop_sequence", "stop_id", *DELAY_COLUMNS],
        name="delay frame",
    )
    return frame.assign(
        route_id=pd.col("route_id").astype("string"),
        trip_id=pd.col("trip_id").astype("string"),
        stop_sequence=pd.col("stop_sequence").astype("Int64"),
        stop_id=pd.col("stop_id").astype("string"),
        arrival_delay=pd.col("arrival_delay").astype("Float64"),
        departure_delay=pd.col("departure_delay").astype("Float64"),
    )


def _cast_stop_times_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Return a stop-times table with validated key columns and normalized dtypes.

    The given DataFrame must contain at least the columns

    - ``'trip_id'``
    - ``'stop_id'``
    - ``'stop_sequence'``.

    The result has the same columns as the input.
    Only those required key columns are normalized here,
    since they are used later for joins and sorting.
    """
    _require_columns(frame, ["trip_id", "stop_id", "stop_sequence"], name="stop_times")
    return frame.assign(
        trip_id=pd.col("trip_id").astype("string"),
        stop_id=pd.col("stop_id").astype("string"),
        stop_sequence=pd.col("stop_sequence").astype("Int64"),
    )


def _last_non_null(series: pd.Series) -> Any:
    """
    Return the last non-null value in the given Series.
    If the series contains no non-null values, return ``pd.NA``.
    """
    result = pd.NA
    non_null = series.dropna()

    if not non_null.empty:
        result = non_null.iloc[-1]

    return result


def _get_optional_string(message: Any, field_name: str) -> str | pd._libs.missing.NAType:
    """
    Return a protobuf scalar field as a string, or ``pd.NA`` if it is absent.
    """
    result = pd.NA

    if message.HasField(field_name):
        result = str(getattr(message, field_name))

    return result


def _get_optional_int(message: Any, field_name: str) -> int | pd._libs.missing.NAType:
    """
    Return a protobuf scalar field as an integer, or ``pd.NA`` if it is absent.
    """
    result = pd.NA

    if message.HasField(field_name):
        result = int(getattr(message, field_name))

    return result


def _get_optional_delay(update: Any, field_name: str) -> float | pd._libs.missing.NAType:
    """
    Return an arrival or departure delay as a float, or ``pd.NA`` if it is absent.

    The named field is expected to be a stop-time event such as ``arrival``
    or ``departure``.
    A missing event, or an event without a ``delay`` value, is treated as missing data.
    """
    result = pd.NA

    if update.HasField(field_name):
        event = getattr(update, field_name)
        if event.HasField("delay"):
            result = float(event.delay)

    return result


def extract_delays(feed: FeedMessage) -> pd.DataFrame:
    """
    Extract trip-update delays from a GTFS-Realtime feed.

    The returned DataFrame has one row per stop-time update and contains
    the columns

    - ``'route_id'``
    - ``'trip_id'``
    - ``'stop_sequence'``
    - ``'stop_id'``
    - ``'arrival_delay'``: in seconds
    - ``'departure_delay'``: in seconds

    Missing arrival or departure delays are represented with pandas nullable values.
    If the feed has no trip updates, then the result is an empty typed
    DataFrame with the same columns.
    """
    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        route_id = str(trip_update.trip.route_id)
        trip_id = str(trip_update.trip.trip_id)

        for stop_update in trip_update.stop_time_update:
            rows.append(
                {
                    "route_id": route_id,
                    "trip_id": trip_id,
                    "stop_sequence": _get_optional_int(stop_update, "stop_sequence"),
                    "stop_id": _get_optional_string(stop_update, "stop_id"),
                    "arrival_delay": _get_optional_delay(stop_update, "arrival"),
                    "departure_delay": _get_optional_delay(stop_update, "departure"),
                }
            )

    result = _empty_delays_frame()

    if rows:
        result = (
            pd.DataFrame.from_records(rows)
            .pipe(_cast_delay_frame)
            .sort_values(["route_id", "trip_id", "stop_sequence"], kind="stable")
            .reset_index(drop=True)
        )

    return result


def combine_delays(delays_list: Iterable[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine the given delay tables produced by :func:`extract_delays`.

    Each input DataFrame must contain the columns


    - ``'route_id'``
    - ``'trip_id'``
    - ``'stop_sequence'``
    - ``'stop_id'``
    - ``'arrival_delay'``
    - ``'departure_delay'``.

    Rows are keyed by ``route_id``, ``trip_id``, and ``stop_sequence``.
    When the same key appears more than once, the last non-null value seen
    for each delay column is used, preserving the order of the input frames.
    That means later frames override earlier ones only where they supply
    usable delay values.

    Return a DataFrame with the columns above.

    Rows in which both delay columns are null are dropped before combining.
    If no usable data remains, the result is an empty typed DataFrame.
    """
    frames = []
    result = _empty_delays_frame()

    for feed_order, frame in enumerate(delays_list):
        if frame.empty:
            continue
        frames.append(_cast_delay_frame(frame.copy()).assign(_feed_order=feed_order))

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        combined = combined.loc[
            ~(pd.col("arrival_delay").isna() & pd.col("departure_delay").isna())
        ]

        if not combined.empty:
            result = (
                combined.sort_values(
                    ["route_id", "trip_id", "stop_sequence", "_feed_order"],
                    kind="stable",
                )
                .groupby(DELAY_KEY_COLUMNS, as_index=False, dropna=False)
                .agg(
                    stop_id=("stop_id", _last_non_null),
                    arrival_delay=("arrival_delay", _last_non_null),
                    departure_delay=("departure_delay", _last_non_null),
                )
                .pipe(_cast_delay_frame)
                .sort_values(["route_id", "trip_id", "stop_sequence"], kind="stable")
                .reset_index(drop=True)
            )

    return result


def _get_agency_timezone(gtfs_feed: Any) -> ZoneInfo:
    """
    Return the GTFS agency timezone, falling back to UTC if necessary.

    The function looks for ``agency_timezone`` in the feed's agency table.
    Missing tables, missing columns, null values, and invalid timezone names
    all result in a UTC fallback.
    """
    result = ZoneInfo("UTC")
    agency = getattr(gtfs_feed, "agency", None)

    if agency is not None and not getattr(agency, "empty", True):
        if "agency_timezone" in agency.columns:
            values = agency["agency_timezone"].dropna()
            if not values.empty:
                try:
                    result = ZoneInfo(str(values.iloc[0]))
                except (ZoneInfoNotFoundError, ValueError):
                    result = ZoneInfo("UTC")

    return result


def _service_window_timestamps(
    stop_times: pd.DataFrame,
    date: str,
    agency_tz: ZoneInfo,
    *,
    fuzz_seconds: int = 20 * 60,
) -> tuple[int, int]:
    """
    Compute the UTC timestamp window for a GTFS service date.

    ``stop_times`` may contain many columns, but this helper only inspects
    ``departure_time`` if it is present.
    The date is a GTFS service date in ``YYYYMMDD`` format.
    The window starts at local midnight on that date in
    the agency timezone and extends through the latest scheduled departure
    plus ``fuzz_seconds``.

    The return value is a pair ``(start_ts, end_ts)`` in Unix seconds.
    """
    service_start = dt.datetime.strptime(date, "%Y%m%d").replace(tzinfo=agency_tz)

    max_departure_seconds = 0
    if "departure_time" in stop_times.columns:
        departures = stop_times["departure_time"].dropna()
        if not departures.empty:
            max_departure_seconds = max(gk.timestr_to_seconds(str(value)) for value in departures)

    service_end = service_start + dt.timedelta(
        seconds=max_departure_seconds + fuzz_seconds
    )

    return int(service_start.timestamp()), int(service_end.timestamp())


def build_augmented_stop_times(
    gtfsr_feeds: Iterable[FeedMessage],
    gtfs_feed: Any,
    date: str,
) -> pd.DataFrame:
    """
    Build stop times for the given date and augment them with realtime delays.

    The scheduled stop times come from the static GTFS feed through
    ``gtfs_kit.get_stop_times``.
    That stop-times table is expected to contain at least
    the columns ``'trip_id'``, ``'stop_id'``, and ``'stop_sequence'``.
    It may contain additional GTFS stop-times columns, such as ``'arrival_time'``,
    ``'shape_dist_traveled'``, etc.

    Realtime feeds are filtered to those whose header timestamps fall within
    the service window for the given date.
    Delay information is then extracted, combined, and merged onto
    the stop-times table by columns 'trip_id', 'stop_id', and 'stop_sequence'.

    The returned DataFrame contains all columns produced by
    ``gtfs_kit.get_stop_times(gtfs_feed, date)``, plus two extra columns
    ``'arrival_delay'`` and ``'departure_delay'`` (measured in seconds).
    If no matching realtime delay data is available,
    those two columns are present but entirely null.
    """
    stop_times = gk.get_stop_times(gtfs_feed, date).copy()
    stop_times = _cast_stop_times_frame(stop_times)
    result = stop_times.assign(
        arrival_delay=pd.Series(np.nan, index=stop_times.index, dtype="Float64"),
        departure_delay=pd.Series(np.nan, index=stop_times.index, dtype="Float64"),
    )

    if not stop_times.empty:
        agency_tz = _get_agency_timezone(gtfs_feed)
        start_ts, end_ts = _service_window_timestamps(stop_times, date, agency_tz)

        delay_frames = [
            extract_delays(feed)
            for feed in gtfsr_feeds
            if start_ts <= int(feed.header.timestamp) <= end_ts
        ]
        delays = combine_delays(delay_frames)

        if not delays.empty:
            delays_for_merge = delays.drop(columns=["route_id"], errors="ignore")
            result = (
                stop_times.merge(
                    delays_for_merge,
                    how="left",
                    on=["trip_id", "stop_id", "stop_sequence"],
                    validate="m:1",
                )
                .assign(
                    arrival_delay=pd.col("arrival_delay").astype("Float64"),
                    departure_delay=pd.col("departure_delay").astype("Float64"),
                )
            )

    result = (
        result.sort_values(["trip_id", "stop_sequence"], kind="stable")
        .reset_index(drop=True)
    )

    return result


def _interpolate_trip_delays(
    group: pd.DataFrame,
    *,
    dist_threshold: float,
    delay_cols: list[str],
) -> pd.DataFrame:
    """
    Interpolate delay columns for one trip when distance data is usable.

    The given group is expected to contain the column ``'shape_dist_traveled'``
    and the delay columns named in ``delay_cols``.
    It is also expected to already be sorted in stop order, usually by
    ``'stop_sequence'``.

    The returned DataFrame has the same columns as ``group``.
    If the ``'shape_dist_traveled'`` column contains missing values or is not monotonic,
    then the group is returned unchanged.

    For each requested delay column, the first and last stops may be seeded
    from the nearest observed delay value, unless that observed value is
    farther away than ``dist_threshold``, in which case the endpoint delay
    is set to zero.
    Intermediate values are then linearly interpolated by
    ``'shape_dist_traveled'``.
    If repeated shape distances occur, then the last observed delay at each
    distance is used.
    """
    can_interpolate = (
        not group["shape_dist_traveled"].isna().any()
        and not np.any(
            np.diff(group["shape_dist_traveled"].to_numpy(dtype=float, na_value=np.nan)) < 0
        )
    )

    if not can_interpolate:
        result = group
    else:
        distances = group["shape_dist_traveled"].to_numpy(dtype=float, na_value=np.nan)
        result = group.copy()

        for col in delay_cols:
            observed_mask = result[col].notna().to_numpy()
            if not observed_mask.any():
                continue

            observed_positions = np.flatnonzero(observed_mask)
            first_observed = observed_positions[0]
            last_observed = observed_positions[-1]

            first_value = float(result.iloc[first_observed][col])
            last_value = float(result.iloc[last_observed][col])

            result.iloc[0, result.columns.get_loc(col)] = (
                0.0
                if abs(distances[0] - distances[first_observed]) > dist_threshold
                else first_value
            )
            result.iloc[-1, result.columns.get_loc(col)] = (
                0.0
                if abs(distances[-1] - distances[last_observed]) > dist_threshold
                else last_value
            )

            known_positions = np.flatnonzero(result[col].notna().to_numpy())
            xp = distances[known_positions]
            fp = result.iloc[known_positions][col].astype(float).to_numpy()

            xp_frame = (
                pd.DataFrame({"dist": xp, "value": fp})
                .groupby("dist", as_index=False, sort=True)
                .last()
            )
            xp = xp_frame["dist"].to_numpy(dtype=float)
            fp = xp_frame["value"].to_numpy(dtype=float)

            if xp.size == 1:
                interpolated = np.full(result.shape[0], fp[0], dtype=float)
                result[col] = pd.Series(interpolated, index=result.index, dtype="Float64")
            elif xp.size > 1:
                interpolated = np.interp(distances, xp, fp)
                result[col] = pd.Series(interpolated, index=result.index, dtype="Float64")

    return result


def interpolate_delays(
    augmented_stop_times: pd.DataFrame,
    dist_threshold: float,
    delay_threshold: int = 3600,
    delay_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Interpolate trip delays by shape distance after removing implausible values.

    ``augmented_stop_times`` must contain the column ``'trip_id'``.
    If interpolation is actually to occur, it must also contain the column
    ``'shape_dist_traveled'``.
    The delay columns are given by ``delay_cols`` and default to
    ``'arrival_delay'`` and ``'departure_delay'``.
    If the ``'stop_sequence'`` column is present, then it is used to sort rows
    within trips before interpolation.

    The returned DataFrame has the same columns as ``augmented_stop_times``.
    The requested delay columns are cleaned, interpolated trip by trip where
    possible, rounded to whole seconds, and stored as nullable floating-point
    values.

    Delay values whose absolute value exceeds ``delay_threshold`` are first
    set to null.
    If the ``'shape_dist_traveled'`` column is missing or contains no
    non-null values, then the cleaned table is returned without interpolation.
    """
    f = augmented_stop_times.copy()

    if "trip_id" not in f.columns:
        raise ValueError("augmented_stop_times must contain a 'trip_id' column")

    if delay_cols is None:
        delay_cols = DELAY_COLUMNS.copy()

    missing_delay_cols = [col for col in delay_cols if col not in f.columns]
    if missing_delay_cols:
        missing_fmt = ", ".join(repr(col) for col in missing_delay_cols)
        raise ValueError(f"augmented_stop_times is missing delay columns: {missing_fmt}")

    f = f.assign(**{col: pd.col(col).astype("Float64") for col in delay_cols})

    for col in delay_cols:
        f.loc[abs(pd.col(col)) > delay_threshold, col] = pd.NA

    result = f

    if "shape_dist_traveled" in f.columns:
        f = f.assign(shape_dist_traveled=pd.col("shape_dist_traveled").astype("Float64"))

        if f["shape_dist_traveled"].notna().any():
            sort_cols = ["trip_id"]
            if "stop_sequence" in f.columns:
                sort_cols.append("stop_sequence")

            f = f.sort_values(sort_cols, kind="stable").reset_index(drop=True)

            pieces = []
            for _, group in f.groupby("trip_id", sort=False, dropna=False):
                pieces.append(
                    _interpolate_trip_delays(
                        group,
                        dist_threshold=dist_threshold,
                        delay_cols=delay_cols,
                    )
                )

            if pieces:
                result = pd.concat(pieces, ignore_index=True)
                result[delay_cols] = result[delay_cols].round(0).astype("Float64")

    return result