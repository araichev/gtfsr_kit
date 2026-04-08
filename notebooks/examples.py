import marimo

__generated_with = "0.22.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import pathlib as pl

    import gtfs_kit as gk

    sys.path.append("../")

    import gtfsr_kit as grk

    # magic command not supported in marimo; please file an issue to add support
    # %load_ext autoreload
    # '%autoreload 2' command supported automatically in marimo

    DATA = pl.Path("data")
    import os

    os.listdir(DATA)
    return DATA, gk, grk


@app.cell
def _(DATA, grk):
    # Read a feed

    path = DATA / "tripUpdates.pb"
    feed = grk.read_feed(path)
    return (feed,)


@app.cell
def _(feed, grk):
    print(feed.header.timestamp)
    print(grk.get_timestamp_str(feed))

    print(feed.entity[0])
    print(grk.feed_to_dict(feed)["entity"][0])
    return


@app.cell
def _(feed, grk):
    # Extract delays

    grk.extract_delays(feed)
    return


@app.cell
def _(DATA, gk, grk):
    # Build augmented stop times

    gtfsr_dir = DATA / "test_gtfsr"
    gtfsr_feeds = [grk.read_feed(path, from_json=True) for path in gtfsr_dir.iterdir()]
    gtfs_feed = gk.read_feed(DATA / "test_gtfs.zip", dist_units="km")  # Good from 20160519
    date = "20160519"

    ast = grk.build_augmented_stop_times(gtfsr_feeds, gtfs_feed, date)

    trip_id = "14005028723-20160512154122_v40.34"
    ast[ast["trip_id"] == trip_id].T
    return ast, trip_id


@app.cell
def _(ast, grk, trip_id):
    # Interpolate delays

    f = grk.interpolate_delays(ast, dist_threshold=1)
    f[f["trip_id"] == trip_id].T
    return


if __name__ == "__main__":
    app.run()
