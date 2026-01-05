from __future__ import annotations

from typing import Any, Mapping, Self

from phable.kinds import Grid, GridCol


class GridBuilder:
    """Builder for constructing Project Haystack `Grid` objects.

    Provides a builder pattern with method chaining for adding columns, metadata,
    and rows before creating a `Grid`.
    """

    def __init__(self):
        self._meta = {"ver": "3.0"}
        self._cols = []
        self._rows = []

    _meta: dict[str, Any]
    _cols: list[GridCol]
    _rows: list[dict[str, Any]]

    @property
    def col_names(self) -> list[str]:
        """Column names.

        Returns:
            List of column names in the order they were added.
        """
        return [col.name for col in self._cols]

    def set_meta(self, meta: Mapping[str, Any]) -> Self:
        """Set or update grid-level metadata.

        Parameters:
            meta: Metadata dictionary to merge with existing grid metadata.

        Returns:
            Self for method chaining.
        """
        self._meta = self._meta | dict(meta)
        return self

    def add_col(self, name: str, meta: Mapping[str, Any] | None = None) -> Self:
        """Adds a column to the grid.

        Parameters:
            name: Column name following Haystack tag naming rules (lowercase start).
            meta: Optional metadata for the column (e.g., unit, display name).

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If column name is invalid or already exists.
        """
        if not _is_tag_name(name):
            raise ValueError(f"Invalid column name: {name}")

        # verify the column does not already exist
        for c in self._cols:
            if c.name == name:
                raise ValueError(f"Duplicate column name: {name}")

        col = GridCol(name, dict(meta) if meta is not None else None)

        self._cols.append(col)
        return self

    def set_col_meta(self, col_name: str, meta: Mapping[str, Any]) -> Self:
        """Set or update metadata for an existing column.

        Parameters:
            col_name: Name of the column to update.
            meta: Metadata to merge with existing column metadata.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If column does not exist.
        """
        col_found = False
        for i, c in enumerate(self._cols):
            if c.name == col_name:
                col_found = True
                existing_meta = c.meta or {}
                new_meta = existing_meta | dict(meta)
                self._cols[i] = GridCol(c.name, new_meta)
                break

        if not col_found:
            raise ValueError(f"Column not found: {col_name}")

        return self

    def add_row(self, row: Mapping[str, Any]) -> Self:
        """Adds a row of data to the grid.

        Parameters:
            row: Dictionary mapping column names to values.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If any row key does not match an existing column name.
        """
        col_names = self.col_names
        for key in row.keys():
            if key not in col_names:
                raise ValueError(f"Row key '{key}' does not match any column name")

        self._rows.append(dict(row))
        return self

    def build(self) -> Grid:
        """Builds a Grid from the accumulated columns, rows, and metadata.

        Returns:
            A constructed `Grid` instance.
        """
        return Grid(self._meta, self._cols, self._rows)


def _is_tag_name(n: str):
    if len(n) == 0 or n[0].isupper():
        return False
    for c in n:
        if not c.isalnum() and c != "_":
            return False
    return True
