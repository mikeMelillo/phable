from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from phable.io.ph_encoder import PhEncoder
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
    date,
    datetime,
    time,
)


class ParquetEncoder(PhEncoder):
    def encode(self, data: PhKind) -> bytes:
        if not isinstance(data, Grid):
            raise ValueError("ParquetEncoder only supports Grid objects")
        return self._grid_to_parquet_bytes(data)

    def to_str(self, data: PhKind) -> str:
        # For consistency with other encoders, return a JSON representation
        # of the Parquet metadata (not the actual binary data)
        if not isinstance(data, Grid):
            raise ValueError("ParquetEncoder only supports Grid objects")
        table = self._grid_to_table(data)
        metadata = {
            "num_rows": table.num_rows,
            "num_columns": table.num_columns,
            "schema": table.schema.__str__(),
        }
        return json.dumps(metadata)

    @staticmethod
    def to_dict(data: PhKind) -> dict[str, Any]:
        # For consistency with other encoders, return Parquet metadata as dict
        if not isinstance(data, Grid):
            raise ValueError("ParquetEncoder only supports Grid objects")
        table = ParquetEncoder._grid_to_table(data)
        return {
            "num_rows": table.num_rows,
            "num_columns": table.num_columns,
            "schema": str(table.schema),
        }

    @staticmethod
    def _grid_to_table(grid: Grid) -> pa.Table:
        # Convert Grid to PyArrow Table with appropriate type mapping
        if not grid.cols:
            # Empty grid
            return pa.table({})

        # Determine column types and prepare data
        column_data = {}
        schema_fields = []

        for col in grid.cols:
            col_name = col.name
            # Extract column values
            values = [row.get(col_name) for row in grid.rows]

            # Convert to appropriate PyArrow array
            pa_array, final_meta = ParquetEncoder._convert_values_to_arrow_array(values, col_name, col.meta)
            column_data[col_name] = pa_array

            # Create field with potential metadata
            field_metadata = {}
            if col.meta:
                # Store column metadata as field metadata
                field_metadata.update({f"haystack.{k}": str(v) for k, v in col.meta.items()})
            
            # If we have explicit kind info in metadata, make sure it's preserved
            if col.meta and "kind" in col.meta:
                field_metadata["haystack.kind"] = col.meta["kind"]
            # If we inferred a kind during conversion, store it too
            elif "kind" in final_meta:
                field_metadata["haystack.kind"] = final_meta["kind"]
                 
            schema_fields.append(pa.field(col_name, pa_array.type, metadata=field_metadata or None))

        # Create schema
        schema = pa.schema(schema_fields)

        # Create table
        table = pa.Table.from_pydict(column_data, schema=schema)

        # Add grid-level metadata if present
        if grid.meta:
            existing_metadata = table.schema.metadata or {}
            haystack_metadata = {
                f"haystack.grid.{k}": str(v) for k, v in grid.meta.items()
            }
            combined_metadata = {**existing_metadata, **haystack_metadata}
            table = table.replace_schema_metadata(combined_metadata)

        return table

    @staticmethod
    def _convert_values_to_arrow_array(values: list[Any], col_name: str, col_meta: dict[str, Any] | None) -> tuple[pa.Array, dict[str, Any]]:
        # Convert list of Haystack values to PyArrow array
        # Returns the array and updated metadata
        if not values:
            # Empty column - infer type from metadata if possible, otherwise string
            if col_meta and "kind" in col_meta:
                kind = col_meta["kind"]
                return ParquetEncoder._kind_to_empty_array(kind), dict(col_meta) if col_meta else {}
            return pa.array([], type=pa.string()), dict(col_meta) if col_meta else {}

        # Find first non-null/non-NA value to determine type
        first_non_null = None
        for val in values:
            if val is not None and not isinstance(val, NA):
                first_non_null = val
                break

        if first_non_null is None:
            # All values are NA - default to string
            inferred_kind = "Str"
        else:
            # Infer kind from first non-null value
            inferred_kind = ParquetEncoder._haystack_value_to_kind(first_non_null)
            if inferred_kind is None:
                inferred_kind = "Str"

        # Update column metadata with inferred kind if we don't have explicit kind
        final_meta = dict(col_meta) if col_meta else {}
        if "kind" not in final_meta and inferred_kind:
            final_meta["kind"] = inferred_kind

        # Convert values based on types
        converted_values = []

        for val in values:
            if val is None:
                # Actual None value
                converted_values.append(None)
            elif isinstance(val, NA):
                # Haystack NA - convert to None/null
                converted_values.append(None)
            elif isinstance(val, Marker):
                # Haystack Marker - convert to True for boolean storage
                converted_values.append(True)
            elif isinstance(val, Remove):
                # Haystack Remove - convert to special marker
                converted_values.append("__REMOVE__")
            else:
                # Regular value
                converted_values.append(ParquetEncoder._convert_haystack_value(val))

        # Determine appropriate PyArrow type
        # For the type determination, we need to consider what we actually stored
        # Look at the first non-None converted value to determine the actual storage type
        first_stored_non_null = None
        for val in converted_values:
            if val is not None:
                first_stored_non_null = val
                break
                
        if first_stored_non_null is None:
            # All values are None - default to string
            arrow_type = pa.string()
        else:
            # If we have mixed types (e.g., bool and string), we need to use string
            # Check if all non-null values are of the same type
            non_null_types = set(type(v) for v in converted_values if v is not None)
            if len(non_null_types) > 1:
                # Mixed types - use string for all values
                arrow_type = pa.string()
                # Convert all values to strings
                converted_values = [str(v) if v is not None else None for v in converted_values]
            else:
                # Single type - use the appropriate PyArrow type
                arrow_type = ParquetEncoder._haystack_type_to_arrow_type(first_stored_non_null, final_meta)
            
        return pa.array(converted_values, type=arrow_type), final_meta

    @staticmethod
    def _convert_haystack_value(val: Any) -> Any:
        # Convert Haystack value to Python equivalent for PyArrow
        # Check bool FIRST because bool is a subclass of int in Python
        if isinstance(val, bool):
            return val
        elif isinstance(val, (int, float)):
            return float(val)
        elif isinstance(val, str):
            return val
        elif isinstance(val, (date, datetime)):
            return val
        elif isinstance(val, time):
            return val
        elif isinstance(val, Number):
            return float(val.val)
        elif isinstance(val, Ref):
            return str(val.val)
        elif isinstance(val, Symbol):
            return str(val.val)
        elif isinstance(val, Uri):
            return str(val.val)
        elif isinstance(val, Coord):
            # Will be handled as struct
            return {"lat": float(val.lat), "lng": float(val.lng)}
        elif isinstance(val, XStr):
            # Will be handled as struct
            return {"type": val.type, "val": val.val}
        elif isinstance(val, Marker):
            # For Marker, we'll store True to indicate presence
            return True
        elif isinstance(val, NA):
            # For NA, return None (will become null in PyArrow)
            return None
        elif isinstance(val, Remove):
            # For Remove, we'll store a special string or handle differently
            # For now, let's use a special value that we can detect later
            return "__REMOVE__"
        else:
            # Fallback to string representation
            return str(val)

    @staticmethod
    def _haystack_type_to_arrow_type(val: Any, col_meta: dict[str, Any] | None) -> pa.DataType:
        # Determine PyArrow type from Haystack value and/or metadata
        if col_meta and "kind" in col_meta:
            kind = col_meta["kind"]
            return ParquetEncoder._kind_to_arrow_type(kind)

        # Infer from value
        # Check bool FIRST because bool is a subclass of int in Python
        if isinstance(val, bool):
            return pa.bool_()
        elif isinstance(val, (int, float)):
            return pa.float64()
        elif isinstance(val, str):
            return pa.string()
        # Check datetime BEFORE date since datetime is a subclass of date
        elif isinstance(val, datetime):
            return pa.timestamp('us')
        elif isinstance(val, date):
            return pa.date32()
        elif isinstance(val, time):
            return pa.time64('us')
        elif isinstance(val, Number):
            return pa.float64()
        elif isinstance(val, Ref):
            return pa.string()
        elif isinstance(val, Symbol):
            return pa.string()
        elif isinstance(val, Uri):
            return pa.string()
        elif isinstance(val, Coord):
            return pa.struct([
                pa.field("lat", pa.float64()),
                pa.field("lng", pa.float64())
            ])
        elif isinstance(val, XStr):
            return pa.struct([
                pa.field("type", pa.string()),
                pa.field("val", pa.string())
            ])
        elif isinstance(val, Marker):
            return pa.bool_()
        else:
            return pa.string()

    @staticmethod
    def _kind_to_arrow_type(kind: str) -> pa.DataType:
        # Map Haystack kind strings to PyArrow types
        kind_map = {
            "Number": pa.float64(),
            "Str": pa.string(),
            "Bool": pa.bool_(),
            "Date": pa.date32(),
            "Time": pa.time64('us'),
            "DateTime": pa.timestamp('us'),
            "Ref": pa.string(),
            "Symbol": pa.string(),
            "Uri": pa.string(),
            "Marker": pa.bool_(),
            "Coord": pa.struct([
                pa.field("lat", pa.float64()),
                pa.field("lng", pa.float64())
            ]),
            "XStr": pa.struct([
                pa.field("type", pa.string()),
                pa.field("val", pa.string())
            ]),
        }
        return kind_map.get(kind, pa.string())

    @staticmethod
    def _haystack_value_to_kind(val: Any) -> str | None:
        # Determine Haystack kind string from a value
        if isinstance(val, bool):
            return "Bool"
        elif isinstance(val, (int, float)):
            return "Number"
        elif isinstance(val, str):
            return "Str"
        # Check datetime BEFORE date since datetime is a subclass of date
        elif isinstance(val, datetime):
            return "DateTime"
        elif isinstance(val, date):
            return "Date"
        elif isinstance(val, time):
            return "Time"
        elif isinstance(val, Number):
            return "Number"
        elif isinstance(val, Ref):
            return "Ref"
        elif isinstance(val, Symbol):
            return "Symbol"
        elif isinstance(val, Uri):
            return "Uri"
        elif isinstance(val, Coord):
            return "Coord"
        elif isinstance(val, XStr):
            return "XStr"
        elif isinstance(val, Marker):
            return "Marker"
        elif isinstance(val, NA):
            return "NA"
        elif isinstance(val, Remove):
            return "Remove"
        else:
            # Fallback to string representation
            return "Str"

    @staticmethod
    def _kind_to_empty_array(kind: str) -> pa.Array:
        # Create empty array of appropriate type
        arrow_type = ParquetEncoder._kind_to_arrow_type(kind)
        return pa.array([], type=arrow_type)

    @staticmethod
    def _grid_to_parquet_bytes(grid: Grid) -> bytes:
        # Convert Grid to Parquet bytes
        table = ParquetEncoder._grid_to_table(grid)
        # Write to buffer
        import io
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()