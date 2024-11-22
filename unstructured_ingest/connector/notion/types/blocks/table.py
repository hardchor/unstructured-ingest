# https://developers.notion.com/reference/block#table
from dataclasses import dataclass, field
from typing import List, Optional

from htmlBuilder.tags import HtmlTag, Td, Th, Thead, Tr

from unstructured_ingest.connector.notion.interfaces import (
    BlockBase,
    FromJSONMixin,
)
from unstructured_ingest.connector.notion.types.rich_text import RichText


@dataclass
class Table(BlockBase):
    table_width: int
    has_column_header: bool
    has_row_header: bool

    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    def get_html(self) -> Optional[HtmlTag]:
        return None


@dataclass
class TableCell(FromJSONMixin):
    rich_texts: List[RichText]

    @classmethod
    def from_dict(cls, data: dict, client=None):
        return cls(rich_texts=[RichText.from_dict(rt, client=client) for rt in data.pop("rich_texts", [])])

    def get_html(self, is_header: bool) -> Optional[HtmlTag]:
        if is_header:
            return Th([], [rt.get_html() for rt in self.rich_texts])
        else:
            return Td([], [rt.get_html() for rt in self.rich_texts])


# https://developers.notion.com/reference/block#table-rows
@dataclass
class TableRow(BlockBase):
    is_header: bool = False
    cells: List[TableCell] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict, client=None):
        cells = data.get("cells", [])
        return cls(cells=[TableCell.from_dict({"rich_texts": c}, client=client) for c in cells])

    @staticmethod
    def can_have_children() -> bool:
        return False

    def get_html(self) -> Optional[HtmlTag]:
        if self.is_header:
            return Thead([], [Tr([], [cell.get_html(is_header=self.is_header) for cell in self.cells])])
        return Tr([], [cell.get_html(is_header=self.is_header) for cell in self.cells])
