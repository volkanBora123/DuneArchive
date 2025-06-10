import sys
import time
from typing import List, Any, Optional

class Record:
    """
    Represents a single fixed-length database record for the Dune Archive System.
    Each record contains a list of values and their corresponding field types.
    Records can be serialized to and deserialized from bytes for binary storage.
    """
    def __init__(self, values: List[Any], field_types: List[str], valid: bool = True):
        """
        Initialize a Record with field values and their types.
        Args:
            values: List of field values (order must match field_types)
            field_types: List of field types ("int" or "str")
            valid: True if record is active, False if deleted
        """
        self.values = values
        self.field_types = field_types
        self.valid = valid

    def serialize(self) -> bytes:
        """
        Convert the record to a fixed-length byte representation.
        Format: [validity_flag(1)] + [field1_bytes] + [field2_bytes] + ...
        Returns:
            bytes: Binary representation of the record
        """
        result = bytearray()
        result.append(0x01 if self.valid else 0x00)
        for value, ftype in zip(self.values, self.field_types):
            if ftype == "int":
                result += int(value).to_bytes(4, byteorder='big', signed=True)
            elif ftype == "str":
                encoded = str(value).encode('utf-8')
                if len(encoded) > 20:
                    encoded = encoded[:20]
                result += encoded.ljust(20, b'\x00')
            else:
                raise ValueError(f"Unsupported field type: {ftype}")
        return bytes(result)

    @classmethod
    def deserialize(cls, byte_data: bytes, field_types: List[str]):
        """
        Construct a Record from raw byte data.
        Args:
            byte_data: The bytes representing the record
            field_types: List of field types ("int" or "str")
        Returns:
            Record: A new Record instance
        """
        if not byte_data or len(byte_data) < 1:
            raise ValueError("Insufficient data for Record deserialization.")
        valid = byte_data[0] == 0x01
        values = []
        offset = 1
        for ftype in field_types:
            if ftype == "int":
                if offset + 4 > len(byte_data):
                    raise ValueError("Insufficient data for int field.")
                value = int.from_bytes(byte_data[offset:offset+4], byteorder='big', signed=True)
                values.append(value)
                offset += 4
            elif ftype == "str":
                if offset + 20 > len(byte_data):
                    raise ValueError("Insufficient data for str field.")
                raw = byte_data[offset:offset+20]
                value = raw.rstrip(b'\x00').decode('utf-8', errors='ignore')
                values.append(value)
                offset += 20
            else:
                raise ValueError(f"Unsupported field type: {ftype}")
        return cls(values, field_types, valid)

    def match_pk(self, pk_index: int, pk_value: Any) -> bool:
        """
        Check if the record's primary key matches the given value.
        Args:
            pk_index: Index of the primary key field
            pk_value: Value to compare against
        Returns:
            bool: True if primary key matches, False otherwise
        """
        try:
            typed_pk_value = type(self.values[pk_index])(pk_value)
        except Exception as e:
            return False

        result = self.values[pk_index] == typed_pk_value
        return result 

class Page:
    """
    Represents a single page in the Dune Archive System.
    Each page can store up to 10 fixed-length records and uses a bitmap
    to track which slots are in use.
    """
    NUM_SLOTS = 10
    BITMAP_SIZE = 2
    HEADER_SIZE = 4 + 1 + 2

    def __init__(self, page_id: int, field_types: List[str], pk_index: int):
        """
        Initialize a Page with given ID and field configuration.
        Args:
            page_id: Unique identifier for the page
            field_types: List of field types for records in this page
            pk_index: Index of the primary key field
        """
        self.page_id = page_id
        self.field_types = field_types
        self.pk_index = pk_index
        self.record_count = 0
        self.bitmap = [False] * self.NUM_SLOTS
        self.records: List[Optional[Record]] = [None] * self.NUM_SLOTS

    def insert_record(self, record: Record) -> bool:
        """
        Insert a record into the first available slot.
        Args:
            record: Record to insert
        Returns:
            bool: True if insertion successful, False if page is full
        """
        for i in range(self.NUM_SLOTS):
            if not self.bitmap[i]:
                self.records[i] = record
                self.bitmap[i] = True
                self.record_count += 1
                return True
        return False

    def delete_record(self, pk_value: Any) -> bool:
        """
        Mark a record as deleted using its primary key.
        Args:
            pk_value: Primary key value of record to delete
        Returns:
            bool: True if record found and deleted, False otherwise
        """
        for i in range(self.NUM_SLOTS):
            if self.bitmap[i] and self.records[i] is not None:
                if self.records[i].valid and self.records[i].match_pk(self.pk_index, pk_value):
                    self.records[i].valid = False
                    self.bitmap[i] = False
                    self.record_count -= 1
                    return True
        return False

    def find_record(self, pk_value: Any) -> Optional[Record]:
        """
        Find a record by its primary key value.
        Args:
            pk_value: Primary key value to search for
        Returns:
            Optional[Record]: Found record or None if not found
        """
        for record in self.records:
            if record and record.valid and record.match_pk(self.pk_index, pk_value):
                return record
        return None

    def serialize(self) -> bytes:
        """
        Convert the page to a fixed-length byte representation.
        Format: [page_id(4)] + [record_count(1)] + [bitmap(2)] + [record_slots...]
        Returns:
            bytes: Binary representation of the page
        """
        result = bytearray()
        result += self.page_id.to_bytes(4, byteorder='big', signed=False)
        result.append(self.record_count)
        bitmap_val = 0
        for i, used in enumerate(self.bitmap):
            if used:
                bitmap_val |= (1 << (9 - i))
        result += bitmap_val.to_bytes(2, byteorder='big')
        record_size = len(Record([0]*len(self.field_types), self.field_types).serialize())
        for i in range(self.NUM_SLOTS):
            if self.bitmap[i] and self.records[i] is not None:
                rec_bytes = self.records[i].serialize()
                if len(rec_bytes) != record_size:
                    raise ValueError("Record size mismatch in serialization.")
                result += rec_bytes
            else:
                result += bytes(record_size)
        return bytes(result)

    @classmethod
    def deserialize(cls, byte_data: bytes, field_types: List[str], pk_index: int):
        """
        Construct a Page from raw byte data.
        Args:
            byte_data: The bytes representing the page
            field_types: List of field types for records
            pk_index: Index of the primary key field
        Returns:
            Page: A new Page instance
        """
        if len(byte_data) < cls.HEADER_SIZE:
            raise ValueError("Insufficient data for Page header.")
        page_id = int.from_bytes(byte_data[0:4], byteorder='big', signed=False)
        record_count = byte_data[4]
        bitmap_val = int.from_bytes(byte_data[5:7], byteorder='big')
        bitmap = [(bitmap_val & (1 << (9 - i))) != 0 for i in range(cls.NUM_SLOTS)]
        records: List[Optional[Record]] = [None] * cls.NUM_SLOTS
        offset = cls.HEADER_SIZE
        record_size = len(Record([0]*len(field_types), field_types).serialize())
        for i in range(cls.NUM_SLOTS):
            slot_bytes = byte_data[offset:offset+record_size]
            if bitmap[i]:
                rec = Record.deserialize(slot_bytes, field_types)
                records[i] = rec
            else:
                records[i] = None
            offset += record_size
        page = cls(page_id, field_types, pk_index)
        page.record_count = record_count
        page.bitmap = bitmap
        page.records = records
        return page 

import json
import os
import re

CATALOG_FILE = 'system_catalog.json'

def is_valid_name(name, max_length):
    """
    Validate a name string.
    Args:
        name: Name to validate
        max_length: Maximum allowed length
    Returns:
        bool: True if name is valid, False otherwise
    """
    return (
        1 <= len(name) <= max_length and
        name.isalnum() and
        any(c.isalpha() for c in name)
    )

def is_valid_type(field_type):
    """
    Check if a field type is supported.
    Args:
        field_type: Type to check
    Returns:
        bool: True if type is supported, False otherwise
    """
    return field_type in {"int", "str"}

def type_exists(catalog, type_name):
    """
    Check if a type already exists in the catalog.
    Args:
        catalog: System catalog dictionary
        type_name: Name of type to check
    Returns:
        bool: True if type exists, False otherwise
    """
    return any(t['type_name'].lower() == type_name.lower() for t in catalog.get('types', []))

def load_catalog():
    """
    Load the system catalog from JSON file.
    Returns:
        dict: System catalog data
    """
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, 'r') as f:
            return json.load(f)
    else:
        return {"types": []}

def save_catalog(catalog):
    """
    Save the system catalog to JSON file.
    Args:
        catalog: System catalog data to save
    """
    with open(CATALOG_FILE, 'w') as f:
        json.dump(catalog, f, indent=2)

def create_type(command_str):
    """
    Create a new type in the system catalog.
    Args:
        command_str: Command string in format: create type <name> <field_count> <pk_index> <field1_name> <field1_type> ...
    Returns:
        str: "SUCCESS" or "FAILURE"
    """
    try:
        tokens = command_str.strip().split()
        if len(tokens) < 6 or tokens[0].lower() != 'create' or tokens[1].lower() != 'type':
            return "FAILURE"

        type_name = tokens[2]
        if not is_valid_name(type_name, 12):
            return "FAILURE"

        try:
            field_count = int(tokens[3])
            pk_index_raw = int(tokens[4])
            pk_index = pk_index_raw - 1
        except ValueError:
            return "FAILURE"

        if field_count < 1 or field_count > 6:
            return "FAILURE"
        if pk_index < 0 or pk_index >= field_count:
            return "FAILURE"

        expected_len = 5 + field_count * 2
        if len(tokens) != expected_len:
            return "FAILURE"

        fields = []
        field_names = set()
        for i in range(field_count):
            fname = tokens[5 + i * 2]
            ftype = tokens[5 + i * 2 + 1]
            if not is_valid_name(fname, 20):
                return "FAILURE"
            if fname in field_names:
                return "FAILURE"
            if not is_valid_type(ftype):
                return "FAILURE"
            fields.append({"name": fname, "type": ftype})
            field_names.add(fname)

        catalog = load_catalog()
        if type_exists(catalog, type_name):
            return "FAILURE"

        catalog["types"].append({
            "type_name": type_name,
            "fields": fields,
            "primary_key_index": pk_index
        })
        save_catalog(catalog)
        return "SUCCESS"
    except Exception as e:
        return "FAILURE"

MAX_PAGES = 50

def get_db_filename(type_name: str) -> str:
    """
    Get the database filename for a type.
    Args:
        type_name: Name of the type
    Returns:
        str: Database filename
    """
    return f"{type_name}.db"

def get_record_size(field_types: List[str]) -> int:
    """
    Calculate the size of a record in bytes.
    Args:
        field_types: List of field types
    Returns:
        int: Size in bytes
    """
    return len(Record([0]*len(field_types), field_types).serialize())

def get_page_size(field_types: List[str]) -> int:
    """
    Calculate the size of a page in bytes.
    Args:
        field_types: List of field types
    Returns:
        int: Size in bytes
    """
    return Page.HEADER_SIZE + 10 * get_record_size(field_types)

def get_all_page_offsets(file, field_types: List[str]) -> List[int]:
    """
    Get byte offsets for all pages in a file.
    Args:
        file: File object
        field_types: List of field types
    Returns:
        List[int]: List of page offsets
    """
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    page_size = get_page_size(field_types)
    return [i * page_size for i in range(file_size // page_size)]

def load_page_at_offset(file, offset: int, field_types: List[str], pk_index: int) -> Page:
    """
    Load a page from a specific offset in a file.
    Args:
        file: File object
        offset: Byte offset
        field_types: List of field types
        pk_index: Primary key index
    Returns:
        Page: Loaded page
    """
    page_size = get_page_size(field_types)
    file.seek(offset)
    data = file.read(page_size)
    return Page.deserialize(data, field_types, pk_index)

def save_page_at_offset(file, page: Page, offset: int, field_types: List[str]):
    """
    Save a page at a specific offset in a file.
    Args:
        file: File object
        page: Page to save
        offset: Byte offset
        field_types: List of field types
    """
    page_size = get_page_size(field_types)
    data = page.serialize()
    if len(data) != page_size:
        raise ValueError("Page size mismatch on save.")
    file.seek(offset)
    file.write(data)
    file.flush()

def create_record(type_name: str, values: list) -> str:
    """
    Create a new record in the database.
    Args:
        type_name: Name of the type
        values: List of field values
    Returns:
        str: "SUCCESS" or "FAILURE"
    """
    catalog = load_catalog()
    type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
    if not type_entry:
        return "FAILURE"
    field_types = [f['type'] for f in type_entry['fields']]
    pk_index = type_entry['primary_key_index']
    db_filename = get_db_filename(type_name)
    record = Record(values, field_types)
    page_size = get_page_size(field_types)
    if not os.path.exists(db_filename):
        with open(db_filename, 'wb') as f:
            pass
    with open(db_filename, 'r+b') as f:
        offsets = get_all_page_offsets(f, field_types)
        for offset in offsets:
            page = load_page_at_offset(f, offset, field_types, pk_index)
            for rec in page.records:
                if rec is not None and rec.match_pk(pk_index, values[pk_index]):
                    return "FAILURE"
        for offset in offsets:
            page = load_page_at_offset(f, offset, field_types, pk_index)
            if page.record_count < Page.NUM_SLOTS:
                if page.insert_record(record):
                    save_page_at_offset(f, page, offset, field_types)
                    return "SUCCESS"
        if len(offsets) < MAX_PAGES:
            new_page = Page(len(offsets), field_types, pk_index)
            if new_page.insert_record(record):
                f.seek(0, os.SEEK_END)
                save_page_at_offset(f, new_page, f.tell(), field_types)
                return "SUCCESS"
        return "FAILURE"

def search_record(type_name: str, pk_value: str) -> Optional[str]:
    """
    Search for a record by primary key.
    Args:
        type_name: Name of the type
        pk_value: Primary key value to search for
    Returns:
        Optional[str]: Space-separated field values if found, None otherwise
    """
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
                return ' '.join(str(v) for v in rec.values)
    return None

def delete_record(type_name: str, pk_value: str) -> str:
    """
    Delete a record by primary key.
    Args:
        type_name: Name of the type
        pk_value: Primary key value to delete
    Returns:
        str: "SUCCESS" or "FAILURE"
    """
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

def log_operation(command_str: str, status: str):
    """
    Log an operation to log.csv.
    Args:
        command_str: Command that was executed
        status: Result of the operation
    """
    ts = int(time.time())
    line = f"{ts}, {command_str.strip()}, {status.strip()}\n"
    with open("log.csv", "a") as f:
        f.write(line)
        f.flush()

def process_command(line: str, search_results: list):
    """
    Process a single command line.
    Args:
        line: Command line to process
        search_results: List to collect search results
    """
    line = line.strip()
    if not line:
        return
    lower = line.lower()
    tokens = line.split()
    try:
        if lower.startswith("create type"):
            if len(tokens) >= 5:
                try:
                    field_count = int(tokens[3])
                except Exception:
                    log_operation(line, "FAILURE")
                    return
                expected_len = 5 + field_count * 2
                if len(tokens) != expected_len:
                    log_operation(line, "FAILURE")
                    return
            status = create_type(line)
            if status == "FAILURE":
                pass
            else:
                pass
            log_operation(line, status)
        elif lower.startswith("create record"):
            if len(tokens) < 4:
                log_operation(line, "FAILURE")
                return
            type_name = tokens[2]
            values = tokens[3:]
            catalog = load_catalog()
            type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
            if not type_entry:
                log_operation(line, "FAILURE")
                return
            if len(values) != len(type_entry['fields']):
                log_operation(line, "FAILURE")
                return
            status = create_record(type_name, values)
            if status == "FAILURE":
                pass
            else:
                pass
            log_operation(line, status)
        elif lower.startswith("search record"):
            if len(tokens) != 4:
                log_operation(line, "FAILURE")
                return
            type_name = tokens[2]
            pk_value = tokens[3]
            result = search_record(type_name, pk_value)
            if result is None:
                log_operation(line, "FAILURE")
            else:
                search_results.append(result)
                log_operation(line, "SUCCESS")
        elif lower.startswith("delete record"):
            if len(tokens) != 4:
                log_operation(line, "FAILURE")
                return
            type_name = tokens[2]
            pk_value = tokens[3]
            status = delete_record(type_name, pk_value)
            if status == "FAILURE":
                pass
            else:
                pass
            log_operation(line, status)
        else:
            log_operation(line, "FAILURE")
    except Exception as e:
        log_operation(line, "FAILURE")

def main():
    """
    Main entry point for the Dune Archive System.
    Processes commands from an input file and writes search results to output.txt.
    """
    if len(sys.argv) != 2:
        print("Usage: python archive.py <input_file>")
        sys.exit(1)
    input_file = sys.argv[1]
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    search_results = []
    with open(input_file, 'r') as f:
        for line in f:
            process_command(line, search_results)
    if search_results:
        with open("output.txt", "w") as f:
            f.write('\n'.join(search_results))

if __name__ == "__main__":
    main() 