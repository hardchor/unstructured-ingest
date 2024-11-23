# https://developers.notion.com/reference/property-object#rich-text
from dataclasses import dataclass, field
from typing import List, Optional

from htmlBuilder.tags import Div, HtmlTag, Span

from unstructured_ingest.connector.notion.interfaces import DBCellBase, DBPropertyBase
from unstructured_ingest.connector.notion.types.rich_text import (
    RichText as RichTextType,
)


@dataclass
class RichText(DBPropertyBase):
    id: str
    name: str
    description: Optional[str] = None
    type: str = "rich_text"
    rich_text: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class RichTextCell(DBCellBase):
    id: str
    rich_text: List[RichTextType]
    name: Optional[str] = None
    type: str = "rich_text"

    @classmethod
    def from_dict(cls, data: dict):
        # remove "\n" from the text as this breaks chunking
        for rt in data.get("rich_text", []):
            if rt.get("type", "") == "text":
                rt["text"]["content"] = rt["text"]["content"].replace("\n", "; ")
            rt["plain_text"] = rt["plain_text"].replace("\n", "; ")

        return cls(
            rich_text=[RichTextType.from_dict(rt) for rt in data.pop("rich_text", [])],
            **data,
        )

    def get_html(self) -> Optional[HtmlTag]:
        if not self.rich_text:
            return None
        spans = [Span([], rt.get_html()) for rt in self.rich_text]
        return Div([], spans)
