# https://developers.notion.com/reference/property-object#relation
from dataclasses import dataclass
from typing import Optional
from urllib.parse import unquote

from htmlBuilder.tags import Div, HtmlTag

from unstructured_ingest.connector.notion.interfaces import (
    DBCellBase,
    DBPropertyBase,
    FromJSONMixin,
)


@dataclass
class DualProperty(FromJSONMixin):
    synced_property_id: str
    synced_property_name: str

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class SingleProperty(FromJSONMixin):
    synced_property_id: Optional[str] = None
    synced_property_name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

@dataclass
class RelationProp(FromJSONMixin):
    database_id: str
    type: str
    dual_property: Optional[DualProperty] = None
    single_property: Optional[SingleProperty] = None

    @classmethod
    def from_dict(cls, data: dict):
        t = data.get("type")
        if t == "dual_property":
            dual_property = DualProperty.from_dict(data.pop(t))
            return cls(dual_property=dual_property, **data)
        elif t == "single_property":
            single_property = SingleProperty.from_dict(data.pop(t))
            return cls(single_property=single_property, **data)
        else:
            raise ValueError(f"{t} type not recognized")



@dataclass
class Relation(DBPropertyBase):
    id: str
    name: str
    relation: RelationProp
    description: Optional[str] = None
    type: str = "relation"

    @classmethod
    def from_dict(cls, data: dict):
        return cls(relation=RelationProp.from_dict(data.pop("relation")), **data)


@dataclass
class RelationCell(DBCellBase):
    id: str
    has_more: bool
    relation: list
    type: str = "relation"
    name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    def get_html(self) -> Optional[HtmlTag]:
        return Div([], unquote(self.id))
