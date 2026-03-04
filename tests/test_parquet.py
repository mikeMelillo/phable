"""Tests for Parquet file I/O functionality."""

from __future__ import annotations

import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from phable.io.parquet_io import ParquetDecoder, ParquetEncoder
from phable.io.ph_io_factory import PH_IO_FACTORY
from phable.kinds import Grid, GridCol, Number, Ref


@pytest.fixture
def sample_his_grid():
    """Create a sample historical grid for testing."""
    tz = ZoneInfo("America/New_York")
    
    meta = {
        "ver": "3.0",
        "hisStart": datetime(2024, 1, 1, 12, 0, tzinfo=tz),
        "hisEnd": datetime(2024, 1, 1, 14, 0, tzinfo=tz),
    }

    cols = [
        GridCol("ts"),
        GridCol("temp", {"id": Ref("temp_sensor_1", "Temperature 1"), "unit": "°F"}),
        GridCol("status", {"id": Ref("status_sensor_1", "Status 1"), "kind": "Str"}),
    ]

    rows = [
        {
            "ts": datetime(2024, 1, 1, 12, 0, tzinfo=tz),
            "temp": Number(72.5, "°F"),
            "status": "active",
        },
        {
            "ts": datetime(2024, 1, 1, 13, 0, tzinfo=tz),
            "temp": Number(73.2, "°F"),
            "status": "active",
        },
    ]

    return Grid(meta=meta, cols=cols, rows=rows)


class TestParquetEncoder:
    """Tests for ParquetEncoder."""

    def test_encode_grid_to_bytes(self, sample_his_grid):
        """Test encoding a Grid to parquet bytes."""
        encoder = ParquetEncoder()
        parquet_bytes = encoder.encode(sample_his_grid)

        assert isinstance(parquet_bytes, bytes)
        assert len(parquet_bytes) > 0
        # Parquet files start with specific magic bytes
        assert parquet_bytes[:4] == b"PAR1"

    def test_encode_non_grid_raises_error(self):
        """Test encoding non-Grid raises TypeError."""
        encoder = ParquetEncoder()

        with pytest.raises(TypeError, match="Expected Grid"):
            encoder.encode("not a grid")

    def test_to_str_raises_not_implemented(self, sample_his_grid):
        """Test that to_str raises NotImplementedError."""
        encoder = ParquetEncoder()

        with pytest.raises(NotImplementedError, match="binary format"):
            encoder.to_str(sample_his_grid)

    def test_to_file_writes_parquet_file(self, sample_his_grid):
        """Test writing Grid to a parquet file."""
        encoder = ParquetEncoder()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.parquet"

            encoder.to_file(sample_his_grid, file_path)

            assert file_path.exists()
            assert file_path.stat().st_size > 0

    def test_to_file_with_string_path(self, sample_his_grid):
        """Test to_file works with string path."""
        encoder = ParquetEncoder()

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = str(Path(tmpdir) / "test.parquet")

            encoder.to_file(sample_his_grid, file_path)

            assert Path(file_path).exists()


class TestParquetDecoder:
    """Tests for ParquetDecoder."""

    def test_decode_bytes_to_grid(self, sample_his_grid):
        """Test decoding parquet bytes to Grid."""
        encoder = ParquetEncoder()
        parquet_bytes = encoder.encode(sample_his_grid)

        decoder = ParquetDecoder()
        grid = decoder.decode(parquet_bytes)

        assert isinstance(grid, Grid)
        assert "ver" in grid.meta
        assert len(grid.rows) == 4

    def test_from_str_raises_not_implemented(self):
        """Test that from_str raises NotImplementedError."""
        decoder = ParquetDecoder()

        with pytest.raises(NotImplementedError, match="binary format"):
            decoder.from_str("not parquet data")

    def test_from_file_reads_parquet_file(self, sample_his_grid):
        """Test reading Grid from parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.parquet"

            encoder = ParquetEncoder()
            encoder.to_file(sample_his_grid, file_path)

            decoder = ParquetDecoder()
            grid = decoder.from_file(file_path)

            assert isinstance(grid, Grid)
            assert len(grid.rows) == 4
            assert "ver" in grid.meta

    def test_from_file_nonexistent_raises_error(self):
        """Test from_file raises FileNotFoundError for missing file."""
        decoder = ParquetDecoder()

        with pytest.raises(FileNotFoundError):
            decoder.from_file("/nonexistent/path/file.parquet")

    def test_from_file_with_string_path(self, sample_his_grid):
        """Test from_file works with string path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = str(Path(tmpdir) / "test.parquet")

            encoder = ParquetEncoder()
            encoder.to_file(sample_his_grid, file_path)

            decoder = ParquetDecoder()
            grid = decoder.from_file(file_path)

            assert isinstance(grid, Grid)
            assert len(grid.rows) == 4

    def test_iter_rows(self, sample_his_grid):
        """Test iterating through rows in parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.parquet"

            encoder = ParquetEncoder()
            encoder.to_file(sample_his_grid, file_path)

            decoder = ParquetDecoder()
            rows = list(decoder.iter_rows(file_path))

            assert len(rows) == 4
            assert all(isinstance(row, dict) for row in rows)
            assert "id" in rows[0]
            assert "ts" in rows[0]

    def test_iter_rows_nonexistent_file_raises_error(self):
        """Test iter_rows raises FileNotFoundError for missing file."""
        decoder = ParquetDecoder()

        with pytest.raises(FileNotFoundError):
            list(decoder.iter_rows("/nonexistent/path/file.parquet"))


class TestGridParquetMethods:
    """Tests for Grid.to_parquet() and Grid.from_parquet() methods."""

    def test_grid_to_parquet_returns_bytes(self, sample_his_grid):
        """Test Grid.to_parquet() returns bytes."""
        parquet_bytes = sample_his_grid.to_parquet()

        assert isinstance(parquet_bytes, bytes)
        assert len(parquet_bytes) > 0
        assert parquet_bytes[:4] == b"PAR1"

    def test_grid_to_parquet_file(self, sample_his_grid):
        """Test Grid.to_parquet() writes to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.parquet"

            result = sample_his_grid.to_parquet(str(file_path))

            assert result is None
            assert file_path.exists()

    def test_grid_from_parquet_file(self, sample_his_grid):
        """Test Grid.from_parquet() reads from file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.parquet"

            sample_his_grid.to_parquet(str(file_path))
            loaded_grid = Grid.from_parquet(str(file_path))

            assert isinstance(loaded_grid, Grid)
            assert len(loaded_grid.rows) == 4
            assert "ver" in loaded_grid.meta

    def test_grid_from_parquet_nonexistent_file_raises_error(self):
        """Test Grid.from_parquet() raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            Grid.from_parquet("/nonexistent/path/file.parquet")

    def test_roundtrip_grid_to_from_parquet(self, sample_his_grid):
        """Test roundtrip encoding/decoding preserves grid data.
        
        Note: The roundtrip goes through pandas' long-format DataFrame structure,
        so the resulting Grid will have a different structure (long format) with
        more rows than the original grid.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test.parquet"

            # Convert to parquet and back
            sample_his_grid.to_parquet(str(file_path))
            loaded_grid = Grid.from_parquet(str(file_path))

            # The loaded grid is in long-format, so it will have more rows
            # Check that it's a valid Grid with the long-format structure
            assert isinstance(loaded_grid, Grid)
            assert "ver" in loaded_grid.meta
            assert len(loaded_grid.rows) > 0
            # Long-format should have id, ts, val_bool, val_str, val_num, na columns
            assert any(col.name == "id" for col in loaded_grid.cols)
            assert any(col.name == "ts" for col in loaded_grid.cols)


class TestParquetIOFactory:
    """Tests for I/O factory registration."""

    def test_parquet_registered_in_factory(self):
        """Test parquet is registered in PH_IO_FACTORY."""
        assert "parquet" in PH_IO_FACTORY
        assert "encoder" in PH_IO_FACTORY["parquet"]
        assert "decoder" in PH_IO_FACTORY["parquet"]
        assert "content_type" in PH_IO_FACTORY["parquet"]

    def test_factory_encoder_encodes_grid(self, sample_his_grid):
        """Test factory encoder works correctly."""
        encoder = PH_IO_FACTORY["parquet"]["encoder"]
        parquet_bytes = encoder.encode(sample_his_grid)

        assert isinstance(parquet_bytes, bytes)
        assert parquet_bytes[:4] == b"PAR1"

    def test_factory_decoder_decodes_grid(self, sample_his_grid):
        """Test factory decoder works correctly."""
        encoder = PH_IO_FACTORY["parquet"]["encoder"]
        decoder = PH_IO_FACTORY["parquet"]["decoder"]

        parquet_bytes = encoder.encode(sample_his_grid)
        grid = decoder.decode(parquet_bytes)

        assert isinstance(grid, Grid)
        assert len(grid.rows) == 4


class TestConversionScenarios:
    """Tests for various conversion scenarios."""

    def test_json_to_parquet_to_grid(self, sample_his_grid):
        """Test conversion chain: Grid -> JSON -> Parquet -> Grid.
        
        Note: The conversion goes through pandas' long-format DataFrame structure,
        so the resulting Grid will have a different structure.
        """
        from phable.io.json_encoder import JsonEncoder
        from phable.io.json_decoder import JsonDecoder

        # First convert Grid to JSON
        json_encoder = JsonEncoder()
        json_bytes = json_encoder.encode(sample_his_grid)

        # Then decode JSON back to Grid
        json_decoder = JsonDecoder()
        grid_from_json = json_decoder.decode(json_bytes)

        # Now convert to Parquet and back
        parquet_encoder = ParquetEncoder()
        parquet_bytes = parquet_encoder.encode(grid_from_json)

        parquet_decoder = ParquetDecoder()
        final_grid = parquet_decoder.decode(parquet_bytes)

        assert isinstance(final_grid, Grid)
        assert len(final_grid.rows) > 0
        # Verify the data is preserved in long-format
        assert any(col.name == "id" for col in final_grid.cols)
        assert any(col.name == "ts" for col in final_grid.cols)

    def test_parquet_with_various_data_types(self):
        """Test parquet encoding with various data types in grid.
        
        Note: The conversion goes through pandas' long-format DataFrame structure,
        so the resulting Grid will have more rows (one per point per timestamp).
        """
        tz = ZoneInfo("America/New_York")

        meta = {
            "ver": "3.0",
            "hisStart": datetime(2024, 1, 1, 12, 0, tzinfo=tz),
            "hisEnd": datetime(2024, 1, 1, 14, 0, tzinfo=tz),
        }

        cols = [
            GridCol("ts"),
            GridCol("bool_val", {"id": Ref("bool_point"), "kind": "Bool"}),
            GridCol("str_val", {"id": Ref("str_point"), "kind": "Str"}),
            GridCol("num_val", {"id": Ref("num_point"), "unit": "kW"}),
        ]

        rows = [
            {
                "ts": datetime(2024, 1, 1, 12, 0, tzinfo=tz),
                "bool_val": True,
                "str_val": "active",
                "num_val": Number(100, "kW"),
            },
            {
                "ts": datetime(2024, 1, 1, 13, 0, tzinfo=tz),
                "bool_val": False,
                "str_val": "inactive",
                "num_val": Number(0, "kW"),
            },
        ]

        grid = Grid(meta=meta, cols=cols, rows=rows)

        encoder = ParquetEncoder()
        parquet_bytes = encoder.encode(grid)

        decoder = ParquetDecoder()
        decoded_grid = decoder.decode(parquet_bytes)

        # The decoded grid is in long-format, so it should have more rows
        # (one for each point per timestamp)
        assert len(decoded_grid.rows) > len(rows)
        assert any(col.name == "id" for col in decoded_grid.cols)
        assert any(col.name == "ts" for col in decoded_grid.cols)
