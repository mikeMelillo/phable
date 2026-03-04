"""Parquet format encoder and decoder for Project Haystack data.

This module provides utilities for reading and writing Haystack Grids
to/from Parquet files using pandas DataFrames as an intermediary format.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from phable.io.ph_decoder import PhDecoder
from phable.io.ph_encoder import PhEncoder
from phable.kinds import Grid, PhKind


class ParquetEncoder(PhEncoder):
    """Encoder for writing Haystack Grids to Parquet format.
    
    Uses pandas DataFrames as an intermediary format. The Grid must be 
    convertible to a DataFrame using the `to_pandas()` or `to_polars()` method.
    """

    def encode(self, data: PhKind) -> bytes:
        """Encode a Grid to Parquet bytes.
        
        Parameters:
            data: A Grid object to encode.
            
        Returns:
            Parquet data as bytes.
            
        Raises:
            TypeError: If data is not a Grid.
            ValueError: If Grid cannot be converted to DataFrame.
        """
        if not isinstance(data, Grid):
            raise TypeError(f"Expected Grid, got {type(data).__name__}")
        
        try:
            df = data.to_pandas()
        except Exception as e:
            raise ValueError(f"Failed to convert Grid to pandas DataFrame: {e}")
        
        # Store metadata in the parquet file using pandas metadata
        return df.to_parquet()

    def to_str(self, data: PhKind) -> str:
        """Not supported for Parquet format.
        
        Parquet is a binary format and cannot be represented as a string.
        Use encode() to get bytes instead.
        
        Raises:
            NotImplementedError: Always, as Parquet is binary format.
        """
        raise NotImplementedError(
            "Parquet is a binary format. Use encode() to get bytes, "
            "or save to file with to_file()."
        )

    @staticmethod
    def to_file(grid: Grid, path: Path | str) -> None:
        """Write a Grid to a Parquet file.
        
        Parameters:
            grid: Grid to write.
            path: File path where Parquet file will be written.
            
        Raises:
            TypeError: If grid is not a Grid.
            ValueError: If Grid cannot be converted to DataFrame.
        """
        if not isinstance(grid, Grid):
            raise TypeError(f"Expected Grid, got {type(grid).__name__}")
        
        path = Path(path)
        try:
            df = grid.to_pandas()
            df.to_parquet(path)
        except Exception as e:
            raise ValueError(f"Failed to write Grid to Parquet file: {e}")


class ParquetDecoder(PhDecoder):
    """Decoder for reading Parquet files into Haystack Grids.
    
    Reads a Parquet file into a pandas DataFrame and converts it back
    to a Grid structure.
    """

    def decode(self, data: bytes) -> PhKind:
        """Decode Parquet bytes into a Grid.
        
        Parameters:
            data: Parquet data as bytes.
            
        Returns:
            A Grid object.
            
        Raises:
            ValueError: If data cannot be parsed as Parquet.
        """
        import io

        import pandas as pd

        try:
            df = pd.read_parquet(io.BytesIO(data))
            return _dataframe_to_grid(df)
        except Exception as e:
            raise ValueError(f"Failed to decode Parquet bytes: {e}")

    def from_str(self, data: str) -> PhKind:
        """Not supported for Parquet format.
        
        Parquet is a binary format and cannot be created from a string.
        Use from_file() to read from a file instead.
        
        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "Parquet is a binary format. Use from_file() to read from a file."
        )

    @staticmethod
    def from_file(path: Path | str) -> Grid:
        """Read a Parquet file into a Grid.
        
        Parameters:
            path: Path to Parquet file.
            
        Returns:
            A Grid object.
            
        Raises:
            FileNotFoundError: If file does not exist.
            ValueError: If file cannot be parsed as Parquet.
        """
        import pandas as pd

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        try:
            df = pd.read_parquet(path)
            return _dataframe_to_grid(df)
        except Exception as e:
            raise ValueError(f"Failed to read Parquet file: {e}")

    @staticmethod
    def iter_rows(path: Path | str) -> Any:
        """Iterate through rows in a Parquet file as dictionaries.
        
        Useful for processing large Parquet files row-by-row without
        loading the entire file into memory. Each yielded row is a dictionary
        that can be converted to a Ref, Dict, or other entity types.
        
        Parameters:
            path: Path to Parquet file.
            
        Returns:
            Iterator yielding rows as dictionaries.
            
        Raises:
            FileNotFoundError: If file does not exist.
            ValueError: If file cannot be read as Parquet.
            
        **Example:**
        
        ```python
        from phable.kinds import Ref
        
        for row_dict in ParquetDecoder.iter_rows("data.parquet"):
            # Convert dict to Ref or other entity types as needed
            ref = Ref(row_dict.get("id"))
            print(ref)
        ```
        """
        import pandas as pd

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        try:
            df = pd.read_parquet(path)
            for _, row in df.iterrows():
                yield row.to_dict()
        except Exception as e:
            raise ValueError(f"Failed to iterate Parquet file: {e}")


def _dataframe_to_grid(df: Any) -> Grid:
    """Convert a pandas DataFrame back to a Grid.
    
    This reconstructs the Grid structure from a DataFrame that was originally
    created from a Grid using to_pandas(). Handles the long-format DataFrame
    structure with id, ts, val_* columns.
    
    Parameters:
        df: pandas DataFrame with columns matching the long-format structure.
        
    Returns:
        A Grid object.
    """
    from phable.kinds import GridCol

    # Convert DataFrame to list of dictionaries (rows)
    rows = df.to_dict("records")

    # Extract grid metadata from DataFrame metadata if available
    grid_meta = {"ver": "3.0"}
    if hasattr(df, "attrs") and df.attrs:
        grid_meta.update(df.attrs)

    # Create Grid with inferred columns
    if rows:
        col_names = list(rows[0].keys())
        cols = [GridCol(name) for name in col_names]
    else:
        cols = []

    return Grid(
        meta=grid_meta,
        cols=cols,
        rows=rows,
    )
