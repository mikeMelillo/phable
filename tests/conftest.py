from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from phable.kinds import NA, Grid, GridCol, Number, Ref

TS_NOW = datetime.now(ZoneInfo("America/New_York"))

EXPECTED_SCHEMA = pa.schema(
    [
        ("ts", pa.timestamp("us", tz="America/New_York")),
        ("id", pa.string()),
        ("dis", pa.string()),
        ("val_bool", pa.bool_()),
        ("val_str", pa.string()),
        ("val_num", pa.float64()),
        ("unit", pa.string()),
        ("na", pa.bool_()),
    ]
)


@pytest.fixture(scope="module")
def non_his_grid() -> Grid:
    meta = {"ver": "3.0"}
    cols = [
        GridCol("x"),
        GridCol("y"),
    ]
    rows = [{"x": 1}, {"y": 2}]

    return Grid(meta, cols, rows)


@pytest.fixture(scope="module")
def single_pt_his_grid() -> Grid:
    meta = {
        "ver": "3.0",
        "id": Ref("1234", "foo kW"),
        "hisStart": TS_NOW - timedelta(hours=1),
        "hisEnd": TS_NOW,
    }

    cols = [
        GridCol("ts"),
        GridCol(
            "val",
            {"id": Ref("point1", "Point 1 description"), "unit": "kW", "kind": "Number"},
        ),
    ]
    rows = [
        {
            "ts": TS_NOW - timedelta(seconds=60),
            "val": NA(),
        },
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "val": Number(72.2, "kW"),
        },
        {
            "ts": TS_NOW,
            "val": Number(76.3, "kW"),
        },
    ]

    return Grid(meta, cols, rows)


@pytest.fixture(scope="module")
def single_pt_his_table() -> pa.Table:
    data = [
        {
            "ts": TS_NOW - timedelta(seconds=60),
            "id": "@point1",
            "dis": None,
            "val_bool": None,
            "val_str": None,
            "val_num": None,
            "unit": None,
            "na": True,
        },
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "id": "@point1",
            "dis": None,
            "val_bool": None,
            "val_str": None,
            "val_num": 72.2,
            "unit": "kW",
            "na": False,
        },
        {
            "ts": TS_NOW,
            "id": "@point1",
            "dis": None,
            "val_bool": None,
            "val_str": None,
            "val_num": 76.3,
            "unit": "kW",
            "na": False,
        },
    ]

    return pa.Table.from_pylist(data, schema=EXPECTED_SCHEMA)


@pytest.fixture(scope="module")
def multi_pt_his_grid() -> Grid:
    meta = {
        "ver": "3.0",
        "id": Ref("1234", "foo kW"),
        "hisStart": TS_NOW - timedelta(hours=1),
        "hisEnd": TS_NOW,
    }

    cols = [
        GridCol("ts"),
        GridCol("v0", {"id": Ref("point1"), "unit": "kW", "kind": "Number"}),
        GridCol("v1", {"id": Ref("point2"), "kind": "Str"}),
        GridCol("v2", {"id": Ref("point3"), "kind": "Bool"}),
    ]
    rows = [
        {
            "ts": TS_NOW - timedelta(seconds=60),
            # v0 is None (missing from row)
            "v1": "available",
            "v2": True,
        },
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "v0": NA(),
            # v1 is None (missing from row)
            "v2": False,
        },
        {
            "ts": TS_NOW,
            "v0": Number(76.3, "kW"),
            "v1": NA(),
            # v2 is None (missing from row)
        },
    ]

    return Grid(meta, cols, rows)


@pytest.fixture(scope="module")
def multi_pt_his_table() -> pa.Table:
    data = [
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "id": "@point1",
            "dis": None,
            "val_bool": None,
            "val_str": None,
            "val_num": None,
            "unit": None,
            "na": True,
        },
        {
            "ts": TS_NOW,
            "id": "@point1",
            "dis": None,
            "val_bool": None,
            "val_str": None,
            "val_num": 76.3,
            "unit": "kW",
            "na": False,
        },
        {
            "ts": TS_NOW - timedelta(seconds=60),
            "id": "@point2",
            "dis": None,
            "val_bool": None,
            "val_str": "available",
            "val_num": None,
            "unit": None,
            "na": False,
        },
        {
            "ts": TS_NOW,
            "id": "@point2",
            "dis": None,
            "val_bool": None,
            "val_str": None,
            "val_num": None,
            "unit": None,
            "na": True,
        },
        {
            "ts": TS_NOW - timedelta(seconds=60),
            "id": "@point3",
            "dis": None,
            "val_bool": True,
            "val_str": None,
            "val_num": None,
            "unit": None,
            "na": False,
        },
        {
            "ts": TS_NOW - timedelta(seconds=30),
            "id": "@point3",
            "dis": None,
            "val_bool": False,
            "val_str": None,
            "val_num": None,
            "unit": None,
            "na": False,
        },
    ]

    return pa.Table.from_pylist(data, schema=EXPECTED_SCHEMA)
