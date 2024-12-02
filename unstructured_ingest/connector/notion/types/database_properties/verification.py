# https://developers.notion.com/reference/property-object#url
from dataclasses import dataclass, field
from typing import Optional, Union

from htmlBuilder.tags import Div, HtmlTag, Span

from unstructured_ingest.connector.notion.interfaces import (
    DBCellBase,
    DBPropertyBase,
    FromJSONMixin,
    GetHTMLMixin,
)
from unstructured_ingest.connector.notion.types.date import Date
from unstructured_ingest.connector.notion.types.user import PartialUser, People, Bots


@dataclass
class Verification(DBPropertyBase):
    id: str
    name: str
    description: Optional[str] = None
    type: str = "verification"
    verification: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class VerificationData(FromJSONMixin, GetHTMLMixin):
    state: Optional[str]
    verified_by: Optional[Union[People, Bots, PartialUser]]
    date: Optional[Date]

    @classmethod
    def from_dict(cls, data: dict):
        date = data.pop("date", None)
        verified_by = data.pop("verified_by", None)
        if verified_by:
            if verified_by.get("type") == "person":
                last_everified_byited_by=People.from_dict(verified_by)
            elif verified_by.get("type") == "bot":
                verified_by=Bots.from_dict(verified_by)
            elif verified_by.get("object") == "user":
                verified_by=PartialUser.from_dict(verified_by)
            else:
                raise ValueError(f"Invalid verified_by type {verified_by.get('type')}")
        
        return cls(
            verified_by=verified_by if verified_by else None,
            date=Date.from_dict(data=date) if date else None,
            **data,
        )

    def get_html(self) -> Optional[HtmlTag]:
        elements = []
        if state := self.state:
            elements.append(Span([], state))
        if (verified_by := self.verified_by) and (verified_by_html := verified_by.get_html()):
            elements.append(verified_by_html)
        if (date := self.date) and (date_html := date.get_html()):
            elements.append(date_html)
        if elements:
            return Div([], elements)
        return None


@dataclass
class VerificationCell(DBCellBase):
    id: str
    verification: Optional[VerificationData]
    name: Optional[str] = None
    type: str = "verification"

    @classmethod
    def from_dict(cls, data: dict):
        return cls(verification=VerificationData.from_dict(data.pop("verification")), **data)

    def get_html(self) -> Optional[HtmlTag]:
        elements = []
        if name := self.name:
            elements.append(Span([], name))
        if (verification := self.verification) and (verification_html := verification.get_html()):
            elements.append(verification_html)

        if elements:
            return Div([], elements)
        return None
