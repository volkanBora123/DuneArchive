import os
import json
from typing import List, Optional
from record import Record
from page import Page
from system_catalog import load_catalog

MAX_PAGES = 5

# --- Helper Functions ---
def get_db_filename(type_name: str) -> str:
    return f"{type_name}.db"

def get_record_size(field_types: List[str]) -> int:
    return len(Record([0]*len(field_types), field_types).serialize())

def get_page_size(field_types: List[str]) -> int:
    return Page.HEADER_SIZE + 10 * get_record_size(field_types)

def get_all_page_offsets(file, field_types: List[str]) -> List[int]:
    """Return a list of byte offsets for all pages in the file."""
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    page_size = get_page_size(field_types)
    return [i * page_size for i in range(file_size // page_size)]

def load_page_at_offset(file, offset: int, field_types: List[str], pk_index: int) -> Page:
    page_size = get_page_size(field_types)
    file.seek(offset)
    data = file.read(page_size)
    return Page.deserialize(data, field_types, pk_index)

def save_page_at_offset(file, page: Page, offset: int, field_types: List[str]):
    page_size = get_page_size(field_types)
    data = page.serialize()
    if len(data) != page_size:
        raise ValueError("Page size mismatch on save.")
    file.seek(offset)
    file.write(data)
    file.flush()

# --- Core Functions ---
def create_record(type_name: str, values: List[str]) -> str:
    catalog = load_catalog()
    type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
    if not type_entry:
        return "FAILURE"
    field_types = [f['type'] for f in type_entry['fields']]
    pk_index = type_entry['primary_key_index']
    db_filename = get_db_filename(type_name)
    record = Record(values, field_types)
    page_size = get_page_size(field_types)
    # Open or create the .db file
    if not os.path.exists(db_filename):
        with open(db_filename, 'wb') as f:
            pass
    with open(db_filename, 'r+b') as f:
        offsets = get_all_page_offsets(f, field_types)
        # Try to insert into existing pages
        for offset in offsets:
            page = load_page_at_offset(f, offset, field_types, pk_index)
            # Check for duplicate primary key
            if page.find_record(values[pk_index]) is not None:
                return "FAILURE"
            if page.record_count < Page.NUM_SLOTS:
                if page.insert_record(record):
                    save_page_at_offset(f, page, offset, field_types)
                    return "SUCCESS"
        # If all pages full, add a new page if under MAX_PAGES
        if len(offsets) < MAX_PAGES:
            new_page = Page(len(offsets), field_types, pk_index)
            if new_page.insert_record(record):
                f.seek(0, os.SEEK_END)
                save_page_at_offset(f, new_page, f.tell(), field_types)
                return "SUCCESS"
        return "FAILURE"

def search_record(type_name: str, pk_value: str) -> Optional[str]:
    catalog = load_catalog()
    type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
    if not type_entry:
        return None
    field_types = [f['type'] for f in type_entry['fields']]
    pk_index = type_entry['primary_key_index']
    db_filename = get_db_filename(type_name)
    if not os.path.exists(db_filename):
        return None
    with open(db_filename, 'rb') as f:
        offsets = get_all_page_offsets(f, field_types)
        for offset in offsets:
            page = load_page_at_offset(f, offset, field_types, pk_index)
            rec = page.find_record(pk_value)
            if rec is not None:
                # Format output as space-separated field values
                return ' '.join(str(v) for v in rec.values)
    return None

def delete_record(type_name: str, pk_value: str) -> str:
    catalog = load_catalog()
    type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
    if not type_entry:
        return "FAILURE"
    field_types = [f['type'] for f in type_entry['fields']]
    pk_index = type_entry['primary_key_index']
    db_filename = get_db_filename(type_name)
    if not os.path.exists(db_filename):
        return "FAILURE"
    with open(db_filename, 'r+b') as f:
        offsets = get_all_page_offsets(f, field_types)
        for offset in offsets:
            page = load_page_at_offset(f, offset, field_types, pk_index)
            if page.delete_record(pk_value):
                save_page_at_offset(f, page, offset, field_types)
                return "SUCCESS"
    return "FAILURE" 