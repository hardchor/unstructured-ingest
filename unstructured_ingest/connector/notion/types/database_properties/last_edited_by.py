# https://developers.notion.com/reference/property-object#last-edited-by
from dataclasses import dataclass
from typing import Optional, Union

from htmlBuilder.tags import HtmlTag

from unstructured_ingest.connector.notion.interfaces import DBCellBase, DBPropertyBase
from unstructured_ingest.connector.notion.types.user import PartialUser, People, Bots


@dataclass
class LastEditedBy(DBPropertyBase):
    @classmethod
    def from_dict(cls, data: dict):
        return cls()

    def get_text(self) -> Optional[str]:
        return None


@dataclass
class LastEditedByCell(DBCellBase):
    id: str
    last_edited_by: Optional[Union[People, Bots, PartialUser]]
    type: str = "last_edited_by"

    name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        last_edited_by = data.pop("last_edited_by", None)
        if last_edited_by:
            if last_edited_by.get("type") == "person":
                last_edited_by=People.from_dict(last_edited_by)
            elif last_edited_by.get("type") == "bot":
                last_edited_by=Bots.from_dict(last_edited_by)
            elif last_edited_by.get("object") == "user":
                last_edited_by=PartialUser.from_dict(last_edited_by)
            else:
                raise ValueError(f"Invalid last_edited_by type {last_edited_by.get('type')}")

        return cls(last_edited_by=last_edited_by, **data)

    def get_html(self) -> Optional[HtmlTag]:
        return self.last_edited_by.get_html() if self.last_edited_by else None
