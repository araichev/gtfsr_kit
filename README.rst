GTFSR Kit
*********
.. image:: https://github.com/araichev/gtfsr_kit/actions/workflows/test.yml/badge.svg

GTFSR Kit is a tiny Python 3.10+ library to process `General Transit Feed Specification Realtime (GTFSR) data <https://developers.google.com/transit/gtfs-realtime/reference>`_.
It does some simple things like read and write Protocol Buffer or JSON feed files.
It also does some complex things like extract and combine delays into Pandas DataFrames.


Installation
============
Install from PyPI, e.g. via ``uv add gtfsr_kit``.


Examples
========
See the Marimo notebook at ``notebooks/examples.py``.


Notes
======
- Development status is Alpha
- This project uses `semantic versioning <http://semver.org/>`_
- Thanks to `MRCagney <http://www.mrcagney.com/>`_ for funding this project


Authors
========
- Alex Raichev  (2016-06)

Changelog
=========

8.0.0, 2026-04-08
-----------------
- Rewrote GTFS-Realtime feed I/O helpers for clearer control flow and more
  consistent UTC timestamp handling.
- Updated the module for Pandas 3 style where appropriate, including use of
  ``pd.col()`` in DataFrame transformations.
- Simplified local typing by removing variable annotations while keeping
  function parameter type hints.
- Improved delay extraction and combination logic, including deterministic
  handling of duplicate trip-stop delay records.
- Hardened service-window and timezone handling for realtime feed matching.
- Refined delay interpolation logic and validation for required columns.
- Improved docstrings.
- Improved tests.
- Updated dependencies.

7.0.1, 2022-07-20
-----------------
- Updated dependencies, removing all but one version caps, and tested on Python 3.10.


7.0.0, 2020-12-15
-----------------
- Upgraded to Python 3.8.
- Breaking change: renamed some functions.
- Changed the package name.


6.1.0, 2018-07-11
------------------
- Added ``delay_cols`` keyword argument to ``interpolate_delays``


6.0.2, 2018-04-18
------------------
- Handled edge case in ``build_augmented_stop_times`` for real this time!


6.0.1, 2018-04-18
------------------
- Handled edge case in ``build_augmented_stop_times``


6.0.0, 2018-04-18
------------------
- Renamed function ``dictify`` to ``feed_to_dict`` and added the inverse function ``dict_to_feed``


5.0.1, 2018-04-17
------------------
- Fixed setup.py


5.0.0, 2018-04-17
------------------
- Finally handled Protocol Buffer feed files, thanks to version 0.0.5 of `the Python gtfs-realtime-bindings <https://github.com/google/gtfs-realtime-bindings/tree/master/python>`_
- Switched to using Google FeedMessage objects natively
- Simplified code


4.0.0, 2016-07-13
------------------
- Changed the signature of ``main.collect_feeds``


3.0.1, 2016-07-12
------------------
- Bugfixed ``ipynb/examples.ipynb``


3.0.0, 2016-07-12
------------------
- Changed to a small data set for tests
- Upgraded to gtfstk==5.0.0
- Removed ``time_it`` decorators from functions
- Changed signature of ``main.build_augmented_stop_times`` to be more versatile


2.0.0, 2016-07-08
------------------
- Refactored heaps
- Added automated tests
- Added Sphinx docs
- Uploaded to pip


1.0.0, 2016-06-12
------------------
- First release