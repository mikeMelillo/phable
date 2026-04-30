from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
import pytest

import phable.kinds as kinds
from phable.io.parquet_decoder import ParquetDecoder
from phable.io.parquet_encoder import ParquetEncoder


# -----------------------------------------------------------------------------
# To Parquet - tests for Kind to Parquet
# -----------------------------------------------------------------------------


def test_grid_to_parquet_roundtrip_empty():
    """Test roundtrip conversion of empty grid"""
    encoder = ParquetEncoder()
    decoder = ParquetDecoder()
    
    # Empty grid
    grid = kinds.Grid(meta={}, cols=[], rows=[])
    
    # Encode to parquet
    parquet_bytes = encoder.encode(grid)
    
    # Decode back
    decoded_grid = decoder.decode(parquet_bytes)
    
    # Should be equivalent
    assert isinstance(decoded_grid, kinds.Grid)
    assert decoded_grid.meta == grid.meta
    assert len(decoded_grid.cols) == len(grid.cols)
    assert len(decoded_grid.rows) == len(grid.rows)


def test_grid_to_parquet_roundtrip_simple_types():
    """Test roundtrip conversion with simple types"""
    encoder = ParquetEncoder()
    decoder = ParquetDecoder()
    
    # Create a grid with various simple types
    cols = [
        kinds.GridCol("id"),
        kinds.GridCol("navName"), 
        kinds.GridCol("active"),
        kinds.GridCol("curVal"),
        kinds.GridCol("created")
    ]
    
    rows = [
        {
            "id": "point1",
            "navName": "Point One",
            "active": True,
            "curVal": 95.5,
            "created": datetime(2024, 1, 15, 10, 30, 0)
        },
        {
            "id": "point2", 
            "navName": "Point Two",
            "active": False,
            "curVal": 87.2,
            "created": datetime(2024, 1, 16, 14, 15, 30)
        }
    ]
    
    grid = kinds.Grid(meta={"version": "1.0"}, cols=cols, rows=rows)
    
    # Encode to parquet
    parquet_bytes = encoder.encode(grid)
    
    # Decode back
    decoded_grid = decoder.decode(parquet_bytes)
    
    # Check basic structure
    assert isinstance(decoded_grid, kinds.Grid)
    assert decoded_grid.meta == grid.meta
    assert len(decoded_grid.cols) == len(grid.cols)
    assert len(decoded_grid.rows) == len(grid.rows)
    
    # Check column names
    for i, col in enumerate(decoded_grid.cols):
        assert col.name == grid.cols[i].name
    
    # Check first row values
    row0 = decoded_grid.rows[0]
    assert row0["id"] == "point1"
    assert row0["navName"] == "Point One"
    assert row0["active"] is True
    assert abs(row0["curVal"].val - 95.5) < 0.001  # Float comparison on Number.val
    assert row0["created"] == datetime(2024, 1, 15, 10, 30, 0)


def test_grid_to_parquet_with_markers_and_na():
    """Test handling of markers and NA values"""
    encoder = ParquetEncoder()
    decoder = ParquetDecoder()
    
    cols = [
        kinds.GridCol("status"),
        kinds.GridCol("curVal")
    ]
    
    rows = [
        {
            "status": kinds.Marker(),  # Marker present
            "curVal": 42.5
        },
        {
            "status": kinds.NA(),      # Not available
            "curVal": kinds.NA()
        },
        {
            "status": kinds.Remove(),  # Remove tag
            "curVal": 100.0
        }
    ]
    
    grid = kinds.Grid(meta={}, cols=cols, rows=rows)
    
    # Encode to parquet
    parquet_bytes = encoder.encode(grid)
    
    # Decode back
    decoded_grid = decoder.decode(parquet_bytes)
    
    # Check structure
    assert isinstance(decoded_grid, kinds.Grid)
    assert len(decoded_grid.rows) == 3
    
    # Check first row - marker should be preserved
    row0 = decoded_grid.rows[0]
    assert isinstance(row0["status"], kinds.Marker)
    assert isinstance(row0["curVal"], kinds.Number)  # Numbers are preserved as Number objects
    assert abs(row0["curVal"].val - 42.5) < 0.001
    
    # Check second row - NA curVals
    row1 = decoded_grid.rows[1]
    assert isinstance(row1["status"], kinds.NA)
    assert isinstance(row1["curVal"], kinds.NA)
    
    # Check third row - remove should be preserved
    row2 = decoded_grid.rows[2]
    assert isinstance(row2["status"], kinds.Remove)
    assert isinstance(row2["curVal"], kinds.Number)  # Numbers are preserved as Number objects
    assert abs(row2["curVal"].val - 100.0) < 0.001


def test_grid_to_parquet_with_refs_and_symbols():
    """Test handling of Ref and Symbol types"""
    encoder = ParquetEncoder()
    decoder = ParquetDecoder()
    
    cols = [
        kinds.GridCol("equipRef"),
        kinds.GridCol("symbolTag")
    ]
    
    rows = [
        {
            "equipRef": kinds.Ref("abc123", "Equipment 123"),
            "symbolTag": kinds.Symbol("cool_point")
        },
        {
            "equipRef": kinds.Ref("xyz789"),  # No display name
            "symbolTag": kinds.Symbol("another_symbol")
        }
    ]
    
    grid = kinds.Grid(meta={}, cols=cols, rows=rows)
    
    # Encode to parquet
    parquet_bytes = encoder.encode(grid)
    
    # Decode back
    decoded_grid = decoder.decode(parquet_bytes)
    
    # Check structure
    assert isinstance(decoded_grid, kinds.Grid)
    assert len(decoded_grid.rows) == 2
    
    # Check first row
    row0 = decoded_grid.rows[0]
    assert isinstance(row0["equipRef"], kinds.Ref)
    assert row0["equipRef"].val == "abc123"
    # Note: dis is only preserved if explicitly provided in GridCol metadata
    # For a first draft, we don't extract dis from Ref values automatically
    assert row0["equipRef"].dis is None  # dis is not preserved without explicit column metadata
    assert isinstance(row0["symbolTag"], kinds.Symbol)
    assert row0["symbolTag"].val == "cool_point"
    
    # Check second row
    row1 = decoded_grid.rows[1]
    assert isinstance(row1["equipRef"], kinds.Ref)
    assert row1["equipRef"].val == "xyz789"
    assert row1["equipRef"].dis is None
    assert isinstance(row1["symbolTag"], kinds.Symbol)
    assert row1["symbolTag"].val == "another_symbol"


def test_grid_to_parquet_with_coords_and_xstr():
    """Test handling of Coord and XStr types"""
    encoder = ParquetEncoder()
    decoder = ParquetDecoder()
    
    cols = [
        kinds.GridCol("geoCoord"),
        kinds.GridCol("description")
    ]
    
    rows = [
        {
            "geoCoord": kinds.Coord(Decimal("40.7128"), Decimal("-74.0060")),
            "description": kinds.XStr("Descr", "Main Headquarters")
        },
        {
            "geoCoord": kinds.Coord(Decimal("34.0522"), Decimal("-118.2437")),
            "description": kinds.XStr("Note", "West Coast Office")
        }
    ]
    
    grid = kinds.Grid(meta={}, cols=cols, rows=rows)
    
    # Encode to parquet
    parquet_bytes = encoder.encode(grid)
    
    # Decode back
    decoded_grid = decoder.decode(parquet_bytes)
    
    # Check structure
    assert isinstance(decoded_grid, kinds.Grid)
    assert len(decoded_grid.rows) == 2
    
    # Check first row
    row0 = decoded_grid.rows[0]
    assert isinstance(row0["geoCoord"], kinds.Coord)
    # Note: lat/lng are converted to floats during Parquet conversion
    # For a first draft, we accept float precision
    assert abs(float(row0["geoCoord"].lat) - 40.7128) < 0.0001
    assert abs(float(row0["geoCoord"].lng) - (-74.0060)) < 0.0001
    assert isinstance(row0["description"], kinds.XStr)
    assert row0["description"].type == "Descr"
    assert row0["description"].val == "Main Headquarters"
    
    # Check second row
    row1 = decoded_grid.rows[1]
    assert isinstance(row1["geoCoord"], kinds.Coord)
    assert abs(float(row1["geoCoord"].lat) - 34.0522) < 0.0001
    assert abs(float(row1["geoCoord"].lng) - (-118.2437)) < 0.0001
    assert isinstance(row1["description"], kinds.XStr)
    assert row1["description"].type == "Note"
    assert row1["description"].val == "West Coast Office"


def test_encoder_to_dict_consistency():
    """Test that to_dict returns consistent metadata"""
    encoder = ParquetEncoder()
    
    grid = kinds.Grid(
        meta={"test": "value"},
        cols=[kinds.GridCol("testCol")],
        rows=[{"testCol": "testValue"}]
    )
    
    # Get dict representation
    dict_result = encoder.to_dict(grid)
    
    # Should contain expected metadata
    assert "num_rows" in dict_result
    assert "num_columns" in dict_result
    assert "schema" in dict_result
    assert dict_result["num_rows"] == 1
    assert dict_result["num_columns"] == 1


def test_encoder_to_str_consistency():
    """Test that to_str returns consistent metadata"""
    encoder = ParquetEncoder()
    
    grid = kinds.Grid(
        meta={"test": "value"},
        cols=[kinds.GridCol("testCol")],
        rows=[{"testCol": "testValue"}]
    )
    
    # Get string representation
    str_result = encoder.to_str(grid)
    
    # Should be valid JSON
    import json
    metadata = json.loads(str_result)
    
    # Should contain expected metadata
    assert "num_rows" in metadata
    assert "num_columns" in metadata
    assert "schema" in metadata
    assert metadata["num_rows"] == 1
    assert metadata["num_columns"] == 1


def test_decoder_from_str_error_handling():
    """Test that from_str properly handles invalid input"""
    decoder = ParquetDecoder()
    
    # Test with actual metadata JSON (should error)
    metadata_json = '{"num_rows": 1, "num_columns": 1}'
    with pytest.raises(ValueError, match="expects binary Parquet data"):
        decoder.from_str(metadata_json)
    
    # Test with random string
    with pytest.raises(ValueError):
        decoder.from_str("not parquet data")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])