import enum
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse
from uuid import UUID

from htmlBuilder.attributes import Style, Type
from htmlBuilder.tags import (
    Body,
    Div,
    Head,
    Html,
    HtmlTag,
    Ol,
    Table,
    Tbody,
    Td,
    Th,
    Thead,
    Title,
    Tr,
    Ul,
)
from notion_client.errors import APIResponseError

import unstructured_ingest.connector.notion.types.blocks as notion_blocks
from unstructured_ingest.connector.notion.client import Client
from unstructured_ingest.connector.notion.interfaces import BlockBase
from unstructured_ingest.connector.notion.types.block import Block
from unstructured_ingest.connector.notion.types.page import Page
from unstructured_ingest.connector.notion.types.database import Database

HtmlElement = Tuple[BlockBase, HtmlTag]

@dataclass
class ProcessBlockResponse:
    html_element: HtmlElement
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)

def process_block(
    client: Client,
    logger: logging.Logger,
    parent_block: Block,
    start_level: int = 0,
 ) -> ProcessBlockResponse:
    block_id_uuid = UUID(parent_block.id)
    children_html_elements: List[HtmlElement] = []
    child_pages: List[str] = []
    child_databases: List[str] = []
    children: List[Tuple[int, Block]] = []

    parent_html = parent_block.get_html()
    if parent_block.has_children:
        if not parent_block.block.can_have_children():
            raise ValueError(f"Block type cannot have children: {type(parent_block.block)}")

        parent_html = parent_html or Div([], [])

        for child_blocks in client.blocks.children.iterate_list(  # type: ignore
            block_id=parent_block.id,
        ):
            for child_block in child_blocks:
                children.append((start_level + 1, child_block))
        logger.debug(f"adding {len(children)} children from parent: {parent_block}")
        for child_level, child_block in children:
            child_block_response = process_block(
                client=client,
                logger=logger,
                parent_block=child_block,
                start_level=child_level,
            )
            child_block_response_block, child_block_response_html = child_block_response.html_element

            # children_html_elements.append(child_block_response.html_element)
    
            logger.debug(f"processing child block: {child_block}")
            if isinstance(child_block.block, notion_blocks.ChildPage) and child_block.id != str(block_id_uuid):
                child_pages.append(child_block.id)
                continue
            elif isinstance(child_block.block, notion_blocks.ChildDatabase):
                child_databases.append(child_block.id)
                continue
            elif isinstance(child_block.block, notion_blocks.Table):
                table_response = build_table(client=client, table=child_block)
                children_html_elements.append((child_block.block, table_response.table_html))
                child_pages.extend(table_response.child_pages)
                child_databases.extend(table_response.child_databases)
                continue
            elif isinstance(child_block.block, notion_blocks.ColumnList):
                build_columned_list_response = build_columned_list(client=client, logger=logger, column_parent=child_block, level=child_level)
                child_pages.extend(build_columned_list_response.child_pages)
                child_databases.extend(build_columned_list_response.child_databases)
                children_html_elements.append((child_block.block, build_columned_list_response.columned_list_html))
                continue
            elif isinstance(child_block.block, notion_blocks.BulletedListItem):
                bulleted_list_response = build_bulleted_list_item(html=child_block_response_html)
                children_html_elements.append((child_block.block, bulleted_list_response.html))
            elif isinstance(child_block.block, notion_blocks.NumberedListItem):
                numbered_list_resp = build_numbered_list_item(html=child_block_response_html)
                children_html_elements.append((child_block.block, numbered_list_resp.html))
            else:
                child_pages.extend(child_block_response.child_pages)
                child_databases.extend(child_block_response.child_databases)
                children_html_elements.append(child_block_response.html_element)

        # Join list items
        joined_html_elements: List[HtmlElement] = []
        numbered_list_items = []
        bullet_list_items = []
        type_attr_ind = (start_level + 1) % len(numbered_list_types)
        list_style_ind = (start_level + 1) % len(bulleted_list_styles)
        for block, html in children_html_elements:
            if isinstance(block, notion_blocks.BulletedListItem):
                bullet_list_items.append(html)
                continue
            elif isinstance(block, notion_blocks.NumberedListItem):
                numbered_list_items.append(html)
                continue
            elif len(numbered_list_items) > 0:
                html = Ol([Type(numbered_list_types[type_attr_ind])], numbered_list_items)
                numbered_list_items = []
                joined_html_elements.append((block, html))
            elif len(bullet_list_items) > 0:
                html = Ul([Type(bulleted_list_styles[list_style_ind])], bullet_list_items)
                bullet_list_items = []
                joined_html_elements.append((block, html))
            elif html:
                joined_html_elements.append((block, html))
        
        if len(numbered_list_items) > 0:
            joined_html_elements.append((block, Ol([Type(numbered_list_types[type_attr_ind])], numbered_list_items)))
        if len(bullet_list_items) > 0:
            joined_html_elements.append((block, Ul([Type(bulleted_list_styles[list_style_ind])], bullet_list_items)))

        parent_html.inner_html.extend([html for block, html in joined_html_elements])
        

    return ProcessBlockResponse(
        html_element=(parent_block.block, parent_html),
        child_pages=child_pages,
        child_databases=child_databases,
    )



@dataclass
class TextExtractionResponse:
    text: Optional[str] = None
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)


@dataclass
class HtmlExtractionResponse:
    html: Optional[HtmlTag] = None
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)


def extract_page_html(
    client: Client,
    page_id: str,
    logger: logging.Logger,
) -> HtmlExtractionResponse:
    parent_block: Block = client.blocks.retrieve(block_id=page_id)  # type: ignore
    head = None
    if isinstance(parent_block.block, notion_blocks.ChildPage):
        head = Head([], Title([], parent_block.block.title))

    process_block_response = process_block(
        client=client,
        logger=logger,
        parent_block=parent_block,
        start_level=0,
    )
    _, body_child_html = process_block_response.html_element
    body = Body([], [body_child_html])
    all_elements = [body]
    if head:
        all_elements = [head] + all_elements
    full_html = Html([], all_elements)

    return HtmlExtractionResponse(
        full_html,
        child_pages=process_block_response.child_pages,
        child_databases=process_block_response.child_databases,
    )


def extract_database_html(
    client: Client,
    database_id: str,
    logger: logging.Logger,
) -> HtmlExtractionResponse:
    logger.debug(f"processing database id: {database_id}")
    database: Database = client.databases.retrieve(database_id=database_id)  # type: ignore
    head = None
    if database.title and database.title[0]:
        head = Head([], Title([], database.title[0].plain_text))

    property_keys = list(database.properties.keys())
    property_keys = sorted(property_keys)
    table_header_rows: List[Tr] = []
    table_body_rows: List[Tr] = []
    child_pages: List[str] = []
    child_databases: List[str] = []
    # Create header row
    table_header_rows.append(Tr([], [Th([], k) for k in property_keys]))

    pages_or_databases: List[Union[Page, Database]] = []
    for page_chunk in client.databases.iterate_query(database_id=database_id):  # type: ignore
        pages_or_databases.extend(page_chunk)

    logger.debug(f"creating {len(pages_or_databases)} rows")
    for page in pages_or_databases:
        if isinstance(page, Database):
            child_databases.append(page.id)
        if isinstance(page, Page):
            child_pages.append(page.id)
        properties = page.properties
        inner_html = [properties.get(k).get_html() for k in property_keys]  # type: ignore
        table_body_rows.append(
            Tr(
                [],
                [Td([], cell) for cell in [html if html else Div([], []) for html in inner_html]],
            ),
        )

    table_html = Table([], [Thead([], table_header_rows)] + [Tbody([], table_body_rows)])
    body_elements: List[HtmlTag] = [table_html]
    if database.title and database.title[0]:
        heading = notion_blocks.Heading.from_dict({"color": "black", "is_toggleable": False})
        heading.rich_text = database.title
        heading_html = heading.get_html()
        if heading_html:
            body_elements.insert(0, heading_html)
    body = Body([], body_elements)
    all_elements = [body]
    if head:
        all_elements = [head] + all_elements
    full_html = Html([], all_elements)

    return HtmlExtractionResponse(
        html=full_html,
        child_pages=child_pages,
        child_databases=child_databases,
    )


@dataclass
class ChildExtractionResponse:
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)


class QueueEntryType(enum.Enum):
    DATABASE = "database"
    PAGE = "page"


@dataclass
class QueueEntry:
    type: QueueEntryType
    id: UUID


def get_recursive_content_from_page(
    client: Client,
    page_id: str,
    logger: logging.Logger,
) -> ChildExtractionResponse:
    return get_recursive_content(
        client=client,
        init_entry=QueueEntry(type=QueueEntryType.PAGE, id=UUID(page_id)),
        logger=logger,
    )


def get_recursive_content_from_database(
    client: Client,
    database_id: str,
    logger: logging.Logger,
) -> ChildExtractionResponse:
    return get_recursive_content(
        client=client,
        init_entry=QueueEntry(type=QueueEntryType.DATABASE, id=UUID(database_id)),
        logger=logger,
    )


def get_recursive_content(
    client: Client,
    init_entry: QueueEntry,
    logger: logging.Logger,
) -> ChildExtractionResponse:
    parents: List[QueueEntry] = [init_entry]
    child_pages: List[str] = []
    child_dbs: List[str] = []
    processed: List[str] = []
    while len(parents) > 0:
        parent: QueueEntry = parents.pop()
        processed.append(str(parent.id))
        if parent.type == QueueEntryType.PAGE:
            logger.debug(f"getting child data from page: {parent.id}")
            page_children = []
            try:
                for children_block in client.blocks.children.iterate_list(  # type: ignore
                    block_id=str(parent.id),
                ):
                    page_children.extend(children_block)
            except APIResponseError as api_error:
                logger.error(f"failed to get page with id {parent.id}: {api_error}")
                if str(parent.id) in child_pages:
                    child_pages.remove(str(parent.id))
                continue
            if not page_children:
                continue

            # Extract child pages
            child_pages_from_page = [
                c for c in page_children if isinstance(c.block, notion_blocks.ChildPage)
            ]
            if child_pages_from_page:
                child_page_blocks: List[notion_blocks.ChildPage] = [
                    p.block
                    for p in child_pages_from_page
                    if isinstance(p.block, notion_blocks.ChildPage)
                ]
                logger.debug(
                    "found child pages from parent page {}: {}".format(
                        parent.id,
                        ", ".join([block.title for block in child_page_blocks]),
                    ),
                )
            new_pages = [p.id for p in child_pages_from_page if p.id not in processed]
            new_pages = list(set(new_pages))
            child_pages.extend(new_pages)
            parents.extend(
                [QueueEntry(type=QueueEntryType.PAGE, id=UUID(i)) for i in new_pages],
            )

            # Extract child databases
            child_dbs_from_page = [
                c for c in page_children if isinstance(c.block, notion_blocks.ChildDatabase)
            ]
            if child_dbs_from_page:
                child_db_blocks: List[notion_blocks.ChildDatabase] = [
                    c.block
                    for c in page_children
                    if isinstance(c.block, notion_blocks.ChildDatabase)
                ]
                logger.debug(
                    "found child database from parent page {}: {}".format(
                        parent.id,
                        ", ".join([block.title for block in child_db_blocks]),
                    ),
                )
            new_dbs = [db.id for db in child_dbs_from_page if db.id not in processed]
            new_dbs = list(set(new_dbs))
            child_dbs.extend(new_dbs)
            parents.extend(
                [QueueEntry(type=QueueEntryType.DATABASE, id=UUID(i)) for i in new_dbs],
            )

            linked_to_others: List[notion_blocks.LinkToPage] = [
                c.block for c in page_children if isinstance(c.block, notion_blocks.LinkToPage)
            ]
            for link in linked_to_others:
                if (page_id := link.page_id) and (
                    page_id not in processed and page_id not in child_pages
                ):
                    child_pages.append(page_id)
                    parents.append(QueueEntry(type=QueueEntryType.PAGE, id=UUID(page_id)))
                if (database_id := link.database_id) and (
                    database_id not in processed and database_id not in child_dbs
                ):
                    child_dbs.append(database_id)
                    parents.append(
                        QueueEntry(type=QueueEntryType.DATABASE, id=UUID(database_id)),
                    )

        elif parent.type == QueueEntryType.DATABASE:
            logger.debug(f"getting child data from database: {parent.id}")
            database_pages: List[Union[Page, Database]] = []
            try:
                for page_entries in client.databases.iterate_query(  # type: ignore
                    database_id=str(parent.id),
                ):
                    database_pages.extend(page_entries)
            except APIResponseError as api_error:
                logger.error(f"failed to get database with id {parent.id}: {api_error}")
                if str(parent.id) in child_dbs:
                    child_dbs.remove(str(parent.id))
                continue
            if not database_pages:
                continue

            child_pages_from_db = [
                p for p in database_pages if isinstance(p, Page)
            ]
            if child_pages_from_db:
                logger.debug(
                    "found child pages from parent database {}: {}".format(
                        parent.id,
                        ", ".join([p.url for p in child_pages_from_db]),
                    ),
                )
            new_pages = [p.id for p in child_pages_from_db if p.id not in processed]
            child_pages.extend(new_pages)
            parents.extend(
                [QueueEntry(type=QueueEntryType.PAGE, id=UUID(i)) for i in new_pages],
            )

            child_dbs_from_db = [
                p for p in database_pages if isinstance(p, Database)
            ]
            if child_dbs_from_db:
                logger.debug(
                    "found child database from parent database {}: {}".format(
                        parent.id,
                        ", ".join([db.url for db in child_dbs_from_db]),
                    ),
                )
            new_dbs = [db.id for db in child_dbs_from_db if db.id not in processed]
            child_dbs.extend(new_dbs)
            parents.extend(
                [QueueEntry(type=QueueEntryType.DATABASE, id=UUID(i)) for i in new_dbs],
            )

    return ChildExtractionResponse(
        child_pages=child_pages,
        child_databases=child_dbs,
    )


def is_valid_uuid(uuid_str: str) -> bool:
    try:
        UUID(uuid_str)
        return True
    except Exception:
        return False


def get_uuid_from_url(path: str) -> Optional[str]:
    strings = path.split("-")
    if len(strings) > 0 and is_valid_uuid(strings[-1]):
        return strings[-1]
    return None


def is_page_url(client: Client, url: str):
    parsed_url = urlparse(url)
    path = parsed_url.path.split("/")[-1]
    if parsed_url.netloc != "www.notion.so":
        return False
    page_uuid = get_uuid_from_url(path=path)
    if not page_uuid:
        return False
    check_resp = client.pages.retrieve_status(page_id=page_uuid)
    return check_resp == 200


def is_database_url(client: Client, url: str):
    parsed_url = urlparse(url)
    path = parsed_url.path.split("/")[-1]
    if parsed_url.netloc != "www.notion.so":
        return False
    database_uuid = get_uuid_from_url(path=path)
    if not database_uuid:
        return False
    check_resp = client.databases.retrieve_status(database_id=database_uuid)
    return check_resp == 200


@dataclass
class BuildTableResponse:
    table_html: HtmlTag
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)


def build_table(client: Client, table: Block) -> BuildTableResponse:
    if not isinstance(table.block, notion_blocks.Table):
        raise ValueError(f"block type not table: {type(table.block)}")
    rows: List[notion_blocks.TableRow] = []
    child_pages: List[str] = []
    child_databases: List[str] = []
    for row_chunk in client.blocks.children.iterate_list(  # type: ignore
        block_id=table.id,
    ):
        rows.extend(
            [row.block for row in row_chunk if isinstance(row.block, notion_blocks.TableRow)],
        )

    # Extract child databases and pages
    for row in rows:
        for c in row.cells:
            for rt in c.rich_texts:
                if mention := rt.mention:
                    if mention.type == "page" and (page := mention.page):
                        child_pages.append(page.id)
                    if mention.type == "database" and (database := mention.database):
                        child_databases.append(database.id)

    header: Optional[notion_blocks.TableRow] = None
    if table.block.has_column_header:
        header = rows.pop(0)
    table_html_rows = []
    if header:
        header.is_header = True
        table_html_rows.append(header.get_html())
    table_html_rows.extend([row.get_html() for row in rows])
    html_table = Table([], table_html_rows)

    return BuildTableResponse(
        table_html=html_table,
        child_pages=child_pages,
        child_databases=child_databases,
    )

@dataclass
class BuildColumnedListResponse:
    columned_list_html: HtmlTag
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)


def build_columned_list(client: Client, logger: logging.Logger, column_parent: Block, level: int = 0) -> BuildColumnedListResponse:
    if not isinstance(column_parent.block, notion_blocks.ColumnList):
        raise ValueError(f"block type not column list: {type(column_parent.block)}")
    columns: List[Block] = []
    child_pages: List[str] = []
    child_databases: List[str] = []
    for column_chunk in client.blocks.children.iterate_list(  # type: ignore
        block_id=column_parent.id,
    ):
        columns.extend(column_chunk)
    num_columns = len(columns)
    columns_content = []
    for column in columns:
        column_content_response = process_block(
            client=client,
            logger=logger,
            parent_block=column,
            start_level=level + 1,
        )
        _, column_content_html = column_content_response.html_element
        columns_content.append(
            Div(
                [Style(f"width:{100/num_columns}%; float: left")],
                [column_content_html],
            ),
        )

    return BuildColumnedListResponse(
        columned_list_html=Div([], columns_content),
        child_pages=child_pages,
        child_databases=child_databases,
    )


@dataclass
class BulletedListResponse:
    html: HtmlTag


bulleted_list_styles = ["circle", "square", "disc"]


def build_bulleted_list_item(
    html: HtmlTag,
) -> BulletedListResponse:
    html.attributes = [Style("margin-left: 10px")]

    return BulletedListResponse(
        html=html,
    )


@dataclass
class NumberedListResponse:
    html: HtmlTag


numbered_list_types = ["i", "a", "1"]


def build_numbered_list_item(
    html: HtmlTag,
) -> NumberedListResponse:
    html.attributes = [Style("margin-left: 10px")]

    return NumberedListResponse(
        html=html,
    )
