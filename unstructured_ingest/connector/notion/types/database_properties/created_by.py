# https://developers.notion.com/reference/property-object#created-by
from dataclasses import dataclass, field
from typing import Optional, Union

from htmlBuilder.tags import HtmlTag

from unstructured_ingest.connector.notion.interfaces import DBCellBase, DBPropertyBase
from unstructured_ingest.connector.notion.types.user import PartialUser, People, Bots


@dataclass
class CreatedBy(DBPropertyBase):
    id: str
    name: str
    description: Optional[str] = None
    type: str = "created_by"
    created_by: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class CreatedByCell(DBCellBase):
    id: str
    created_by: Optional[Union[People, Bots, PartialUser]]
    type: str = "created_by"
    name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        created_by = data.pop("created_by", None)
        if created_by:
            if created_by.get("type") == "person":
                created_by=People.from_dict(created_by)
            elif created_by.get("type") == "bot":
                created_by=Bots.from_dict(created_by)
            elif created_by.get("object") == "user":
                created_by=PartialUser.from_dict(created_by)
            else:
                raise ValueError(f"Invalid created_by type {created_by.get('type')}")

        return cls(created_by=created_by, **data)
        

    def get_html(self) -> Optional[HtmlTag]:
        return self.created_by.get_html() if self.created_by else None
