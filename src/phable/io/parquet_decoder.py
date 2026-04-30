from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from phable.io.ph_decoder import PhDecoder
from phable.kinds import (
    Coord,
    Grid,
    NA,
    Number,
    PhKind,
    Ref,
    Remove,
    Symbol,
    Uri,
    XStr,
    Marker,
    GridCol
)


class ParquetDecoder(PhDecoder):
    def decode(self, data: bytes) -> PhKind:
        # Read Parquet from bytes
        import io
        buffer = io.BytesIO(data)
        table = pq.read_table(buffer)
        return self._table_to_grid(table)

    def from_str(self, data: str) -> PhKind:
        # For consistency, expect JSON metadata and raise error for actual data
        try:
            metadata = json.loads(data)
            if "num_rows" in metadata and "num_columns" in metadata:
                raise ValueError("ParquetDecoder.from_str expects binary Parquet data, not metadata JSON")
        except json.JSONDecodeError:
            pass  # Not JSON, treat as potential binary data
        
        # Try to decode as binary data
        try:
            return self.decode(data.encode('utf-8'))
        except Exception:
            raise ValueError("Unable to decode string as Parquet data")

    @staticmethod
    def from_json(data: dict[str, Any]) -> PhKind:
        # For consistency with other decoders
        raise NotImplementedError("ParquetDecoder.from_json not implemented - use decode() with binary data")

    @staticmethod
    def _table_to_grid(table: pa.Table) -> Grid:
        # Convert PyArrow Table back to Haystack Grid
        if table.num_rows == 0 and table.num_columns == 0:
            # Empty table
            return Grid(meta={}, cols=[], rows=[])

        # Extract grid-level metadata
        grid_meta = {}
        if table.schema.metadata:
            for key, value in table.schema.metadata.items():
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                if key_str.startswith("haystack.grid."):
                    meta_key = key_str[len("haystack.grid."):]
                    grid_meta[meta_key] = value_str

        # Build columns and rows
        cols = []
        rows = []

        # Process each column
        for i in range(table.num_columns):
            col_name = table.schema.field(i).name
            col_field = table.schema.field(i)
            
            # Extract column metadata from field metadata
            col_meta = {}
            if col_field.metadata:
                for key, value in col_field.metadata.items():
                    key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                    value_str = value.decode('utf-8') if isinstance(value, bytes) else value
                    if key_str.startswith("haystack."):
                        meta_key = key_str[len("haystack."):]
                        col_meta[meta_key] = value_str
            
            # Create GridCol
            cols.append(GridCol(col_name, col_meta if col_meta else None))

        # Convert each row
        for row_idx in range(table.num_rows):
            row_data = {}
            for col_idx in range(table.num_columns):
                col_name = table.schema.field(col_idx).name
                col_field = table.schema.field(col_idx)
                value = table[col_idx][row_idx].as_py()
                
                # Convert PyArrow value back to Haystack kind
                haystack_value = ParquetDecoder._convert_arrow_to_haystack(
                    value, col_field, row_idx, table
                )
                row_data[col_name] = haystack_value
            rows.append(row_data)

        return Grid(meta=grid_meta, cols=cols, rows=rows)

    @staticmethod
    def _convert_arrow_to_haystack(value: Any, field: pa.Field, row_idx: int, table: pa.Table) -> Any:
        # Convert PyArrow value back to Haystack kind
        if value is None:
            # Check if this should be NA or just None
            # For simplicity, treat all nulls as NA for now
            # In a more sophisticated implementation, we'd check metadata
            return NA()
        
        # Get type information
        field_type = field.type
        
        # Check for explicit kind in metadata
        kind_from_meta = None
        if field.metadata:
            for key, value_bytes in field.metadata.items():
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                if key_str == "haystack.kind":
                    kind_from_meta = value_bytes.decode('utf-8') if isinstance(value_bytes, bytes) else value_bytes
                    break
        
        # Handle based on PyArrow type
        if pa.types.is_integer(field_type) or pa.types.is_floating(field_type):
            # Number type - check if we have unit metadata
            unit = None
            if field.metadata:
                for key, value_bytes in field.metadata.items():
                    key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                    if key_str == "haystack.unit":
                        unit = value_bytes.decode('utf-8') if isinstance(value_bytes, bytes) else value_bytes
                        break
            return Number(float(value), unit)
            
        elif pa.types.is_boolean(field_type):
            # Could be Bool or Marker
            if kind_from_meta == "Marker":
                # For Marker, True means marker present
                return Marker() if value else None  # None will become NA in row processing
            else:
                return bool(value)
                
        elif pa.types.is_string(field_type):
            # Could be Str, Ref, Symbol, Uri, or just string
            # Or could be a special value like Marker/Remove stored as string
            # Check metadata for kind
            if kind_from_meta:
                kind_map = {
                    "Str": str,
                    "Ref": lambda v: Ref(v),
                    "Symbol": lambda v: Symbol(v),
                    "Uri": lambda v: Uri(v),
                }
                if kind_from_meta in kind_map:
                    # For Ref, also check for display name in metadata
                    if kind_from_meta == "Ref" and field.metadata:
                        dis = None
                        for key, value_bytes in field.metadata.items():
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            if key_str == "haystack.dis":
                                dis = value_bytes.decode('utf-8') if isinstance(value_bytes, bytes) else value_bytes
                                break
                        return Ref(value, dis)
                    return kind_map[kind_from_meta](value)
                # Handle special cases for values stored as strings
                elif kind_from_meta == "Marker":
                    # Marker values are stored as "True" or "None" (as strings)
                    if value == "True" or value is True:
                        return Marker()
                    elif value == "__REMOVE__":
                        return Remove()
                    else:
                        # Unknown marker value - default to NA
                        return NA()
                elif kind_from_meta == "Remove":
                    if value == "__REMOVE__":
                        return Remove()
                    else:
                        return str(value)
                # Handle special Remove case
                elif value == "__REMOVE__":
                    return Remove()
            # Default to string
            return str(value)
            
        elif pa.types.is_date32(field_type):
            return value  # Already a date object from .as_py()
            
        elif pa.types.is_time64(field_type):
            # value is already a time object from .as_py()
            return value
            
        elif pa.types.is_timestamp(field_type):
            # value is already a datetime object from .as_py()
            return value
            
        elif pa.types.is_struct(field_type):
            # Could be Coord or XStr
            if kind_from_meta == "Coord":
                return Coord(lat=value["lat"], lng=value["lng"])
            elif kind_from_meta == "XStr":
                return XStr(type=value["type"], val=value["val"])
            else:
                # Fallback - try to infer from field names
                if field_type.num_fields == 2:
                    field_names = [field_type.field(i).name for i in range(field_type.num_fields)]
                    if set(field_names) == {"lat", "lng"}:
                        return Coord(lat=value["lat"], lng=value["lng"])
                    elif set(field_names) == {"type", "val"}:
                        return XStr(type=value["type"], val=value["val"])
                # If we can't determine, return as dict
                return value
                
        else:
            # Fallback to string representation
            return str(value)