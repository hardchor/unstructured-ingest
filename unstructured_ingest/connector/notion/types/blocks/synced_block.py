# https://developers.notion.com/reference/block#synced-block
from dataclasses import dataclass, field
from typing import List, Optional

from htmlBuilder.tags import HtmlTag

from unstructured_ingest.connector.notion.interfaces import BlockBase


@dataclass
class OriginalSyncedBlock(BlockBase):
    synced_from: None = field(default=None)

    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        return cls(children=data["children"])

    def get_html(self) -> Optional[HtmlTag]:
        return None


@dataclass
class DuplicateSyncedBlock(BlockBase):
    type: str = "block_id"
    block_id: str

    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    def get_html(self) -> Optional[HtmlTag]:
        return None


class SyncBlock(BlockBase):
    @staticmethod
    def can_have_children() -> bool:
        return True

    @classmethod
    def from_dict(cls, data: dict):
        if not data.get("synced_from"):
            return OriginalSyncedBlock.from_dict(data)
        else:
            return DuplicateSyncedBlock.from_dict(data["synced_from"])

    def get_html(self) -> Optional[HtmlTag]:
        return None
