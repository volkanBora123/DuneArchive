import sys
import time
from typing import List, Any, Optional

class Record:
    """
    Represents a single fixed-length database record for the Dune Archive System.
    Supports serialization to and deserialization from bytes for binary storage.
    """
    def __init__(self, values: List[Any], field_types: List[str], valid: bool = True):
        """
        Initialize a Record.
        :param values: List of field values (order must match field_types)
        :param field_types: List of field types ("int" or "str")
        :param valid: True if record is active, False if deleted
        """
        self.values = values
        self.field_types = field_types
        self.valid = valid

    def serialize(self) -> bytes:
        """
        Convert the record to a fixed-length byte representation.
        :return: bytes
        """
        result = bytearray()
        # Validity flag
        result.append(0x01 if self.valid else 0x00)
        # Fields
        for value, ftype in zip(self.values, self.field_types):
            if ftype == "int":
                # 4 bytes, big-endian, signed
                result += int(value).to_bytes(4, byteorder='big', signed=True)
            elif ftype == "str":
                # 20 bytes, UTF-8, null-padded
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
        :param byte_data: The bytes representing the record
        :param field_types: List of field types ("int" or "str")
        :return: Record instance
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
                # Remove null padding and decode
                value = raw.rstrip(b'\x00').decode('utf-8', errors='ignore')
                values.append(value)
                offset += 20
            else:
                raise ValueError(f"Unsupported field type: {ftype}")
        return cls(values, field_types, valid)

    def match_pk(self, pk_index: int, pk_value: Any) -> bool:
        try:
            # Attempt type conversion
            typed_pk_value = type(self.values[pk_index])(pk_value)
        except Exception as e:
            print(f"[DEBUG] match_pk: type conversion failed for pk_value={pk_value}, error: {e}")
            return False

        result = self.values[pk_index] == typed_pk_value
        print(f"[DEBUG] match_pk: self.values[{pk_index}] = {self.values[pk_index]}, comparing to {typed_pk_value} -> {result}")
        return result 

class Page:
    """
    Represents a single page in the Dune Archive System.
    Stores up to 10 fixed-length records, tracks slot usage with a bitmap,
    and supports serialization/deserialization for binary storage.
    """
    NUM_SLOTS = 10
    BITMAP_SIZE = 2  # 10 bits used, 6 bits padding
    HEADER_SIZE = 4 + 1 + 2  # Page ID (4) + Record Count (1) + Bitmap (2)

    def __init__(self, page_id: int, field_types: List[str], pk_index: int):
        self.page_id = page_id
        self.field_types = field_types
        self.pk_index = pk_index
        self.record_count = 0
        self.bitmap = [False] * self.NUM_SLOTS  # True if slot used
        self.records: List[Optional[Record]] = [None] * self.NUM_SLOTS

    def insert_record(self, record: Record) -> bool:
        """
        Insert a record into the first available slot. Returns True on success, False if full.
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
        Delete the record with the given primary key value. Returns True if found and deleted, False otherwise.
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
        Find and return the record with the given primary key value, or None if not found or deleted.
        """
        for record in self.records:
            if record and record.valid and record.match_pk(self.pk_index, pk_value):
                print(f"[DEBUG] find_record: Found match with pk = {pk_value}")
                return record
        print(f"[DEBUG] find_record: No match found for pk = {pk_value}")
        return None

    def serialize(self) -> bytes:
        """
        Serialize the page to bytes: header, bitmap, and 10 record slots.
        """
        result = bytearray()
        # Page ID (4 bytes, unsigned int)
        result += self.page_id.to_bytes(4, byteorder='big', signed=False)
        # Record Count (1 byte)
        result.append(self.record_count)
        # Bitmap (2 bytes, 10 bits used)
        bitmap_val = 0
        for i, used in enumerate(self.bitmap):
            if used:
                bitmap_val |= (1 << (9 - i))  # Highest bit is slot 0
        result += bitmap_val.to_bytes(2, byteorder='big')
        # Record slots
        record_size = len(Record([0]*len(self.field_types), self.field_types).serialize())
        for i in range(self.NUM_SLOTS):
            if self.bitmap[i] and self.records[i] is not None:
                rec_bytes = self.records[i].serialize()
                if len(rec_bytes) != record_size:
                    raise ValueError("Record size mismatch in serialization.")
                result += rec_bytes
            else:
                result += bytes(record_size)  # Zero-filled slot
        return bytes(result)

    @classmethod
    def deserialize(cls, byte_data: bytes, field_types: List[str], pk_index: int):
        """
        Deserialize a Page from bytes, reconstructing header, bitmap, and records.
        """
        if len(byte_data) < cls.HEADER_SIZE:
            raise ValueError("Insufficient data for Page header.")
        # Page ID
        page_id = int.from_bytes(byte_data[0:4], byteorder='big', signed=False)
        # Record Count
        record_count = byte_data[4]
        # Bitmap
        bitmap_val = int.from_bytes(byte_data[5:7], byteorder='big')
        bitmap = [(bitmap_val & (1 << (9 - i))) != 0 for i in range(cls.NUM_SLOTS)]
        # Record slots
        records: List[Optional[Record]] = [None] * cls.NUM_SLOTS
        offset = cls.HEADER_SIZE
        record_size = len(Record([0]*len(field_types), field_types).serialize())
        for i in range(cls.NUM_SLOTS):
            slot_bytes = byte_data[offset:offset+record_size]
            if bitmap[i]:
                # Only try to deserialize if slot is marked used
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

# ----------------------
# Helper Validation Functions
# ----------------------
def is_valid_name(name, max_length):
    """
    Checks if a name is alphanumeric, contains at least one letter, is <= max_length, and has no underscores or symbols.
    """
    return (
        1 <= len(name) <= max_length and
        name.isalnum() and
        any(c.isalpha() for c in name)
    )

def is_valid_type(field_type):
    """
    Checks if the field type is either 'int' or 'str'.
    """
    return field_type in {"int", "str"}

def type_exists(catalog, type_name):
    """
    Checks if a type with the given name already exists in the catalog.
    """
    return any(t['type_name'].lower() == type_name.lower() for t in catalog.get('types', []))

# ----------------------
# Catalog File Operations
# ----------------------
def load_catalog():
    """
    Loads the system catalog from JSON file, or creates a new one if not present.
    """
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, 'r') as f:
            return json.load(f)
    else:
        return {"types": []}

def save_catalog(catalog):
    """
    Saves the system catalog to the JSON file.
    """
    with open(CATALOG_FILE, 'w') as f:
        json.dump(catalog, f, indent=2)

# ----------------------
# Main Type Creation Logic
# ----------------------

def create_type(command_str):
    """
    Parses and processes a 'create type' command in the format:
    create type <type_name> <field_count> <pk_index> <field1_name> <field1_type> ... <fieldN_name> <fieldN_type>
    Returns 'SUCCESS' or 'FAILURE'.
    """
    try:
        tokens = command_str.strip().split()
        if len(tokens) < 6 or tokens[0].lower() != 'create' or tokens[1].lower() != 'type':
            print("[DEBUG] create_type FAILURE: Invalid syntax (not enough tokens or wrong command start).")
            return "FAILURE"

        type_name = tokens[2]
        # Validate type_name
        if not is_valid_name(type_name, 12):
            print("[DEBUG] create_type FAILURE: Invalid type_name.")
            return "FAILURE"

        # Parse field_count and pk_index (adjust pk_index to 0-based)
        try:
            field_count = int(tokens[3])
            pk_index_raw = int(tokens[4])
            pk_index = pk_index_raw - 1  # Convert to 0-based index
        except ValueError:
            print("[DEBUG] create_type FAILURE: field_count or pk_index not integer.")
            return "FAILURE"

        if field_count < 1 or field_count > 6:
            print("[DEBUG] create_type FAILURE: field_count out of range.")
            return "FAILURE"
        if pk_index < 0 or pk_index >= field_count:
            print(f"[DEBUG] create_type FAILURE: pk_index {pk_index_raw} (0-based: {pk_index}) out of range for field_count {field_count}.")
            return "FAILURE"

        # There should be exactly field_count * 2 tokens after the first 5
        expected_len = 5 + field_count * 2
        if len(tokens) != expected_len:
            print("[DEBUG] create_type FAILURE: Incorrect number of field tokens.")
            return "FAILURE"

        fields = []
        field_names = set()
        for i in range(field_count):
            fname = tokens[5 + i * 2]
            ftype = tokens[5 + i * 2 + 1]
            if not is_valid_name(fname, 20):
                print(f"[DEBUG] create_type FAILURE: Invalid field name '{fname}'.")
                return "FAILURE"
            if fname in field_names:
                print(f"[DEBUG] create_type FAILURE: Duplicate field name '{fname}'.")
                return "FAILURE"
            if not is_valid_type(ftype):
                print(f"[DEBUG] create_type FAILURE: Invalid field type '{ftype}'.")
                return "FAILURE"
            fields.append({"name": fname, "type": ftype})
            field_names.add(fname)

        # Load catalog and check for duplicate type
        catalog = load_catalog()
        if type_exists(catalog, type_name):
            print(f"[DEBUG] create_type FAILURE: Type '{type_name}' already exists.")
            return "FAILURE"

        # Add new type
        catalog["types"].append({
            "type_name": type_name,
            "fields": fields,
            "primary_key_index": pk_index
        })
        save_catalog(catalog)
        print(f"[DEBUG] create_type SUCCESS: Type '{type_name}' created with pk_index {pk_index} (input was {pk_index_raw}).")
        return "SUCCESS"
    except Exception as e:
        print(f"[DEBUG] create_type FAILURE: Exception occurred: {e}")
        return "FAILURE"



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
def create_record(type_name: str, values: list) -> str:
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
        # Check for duplicate primary key in all pages (even if deleted)
        for offset in offsets:
            page = load_page_at_offset(f, offset, field_types, pk_index)
            for rec in page.records:
                if rec is not None and rec.match_pk(pk_index, values[pk_index]):
                    print(f"[DEBUG] create_record FAILURE: Duplicate primary key '{values[pk_index]}' found (even if deleted).")
                    return "FAILURE"
        # Try to insert into existing pages
        for offset in offsets:
            page = load_page_at_offset(f, offset, field_types, pk_index)
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


def log_operation(command_str: str, status: str):
    """
    Append a line to log.csv with Unix timestamp, command string, and status.
    Format: <timestamp>, <command_str>, <status>
    """
    ts = int(time.time())
    line = f"{ts}, {command_str.strip()}, {status.strip()}\n"
    with open("log.csv", "a") as f:
        f.write(line)
        f.flush()

def process_command(line: str, search_results: list):
    """
    Parse and execute a single command line, log the result, and collect output if needed.
    """
    line = line.strip()
    if not line:
        return
    print(f"[DEBUG] Processing command: {line}")
    lower = line.lower()
    tokens = line.split()
    try:
        if lower.startswith("create type"):
            # Validate field count matches number of field name/type pairs
            if len(tokens) >= 5:
                try:
                    field_count = int(tokens[3])
                except Exception:
                    print(f"[DEBUG] create_type FAILURE: field_count is not an integer.")
                    log_operation(line, "FAILURE")
                    return
                expected_len = 5 + field_count * 2
                if len(tokens) != expected_len:
                    print(f"[DEBUG] create_type FAILURE: Expected {field_count} fields, but got {((len(tokens)-5)//2)}.")
                    log_operation(line, "FAILURE")
                    return
            status = create_type(line)
            if status == "FAILURE":
                print(f"[DEBUG] create_type FAILURE: Invalid syntax, duplicate type, or field/type error.")
            else:
                print(f"[DEBUG] create_type status: {status}")
            log_operation(line, status)
        elif lower.startswith("create record"):
            if len(tokens) < 4:
                print(f"[DEBUG] create_record FAILURE: not enough tokens")
                log_operation(line, "FAILURE")
                return
            type_name = tokens[2]
            values = tokens[3:]
            # Validate value count matches field count
            from system_catalog import load_catalog
            catalog = load_catalog()
            type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
            if not type_entry:
                print(f"[DEBUG] create_record FAILURE: type '{type_name}' not found.")
                log_operation(line, "FAILURE")
                return
            field_count = len(type_entry['fields'])
            if len(values) != field_count:
                print(f"[DEBUG] create_record FAILURE: Expected {field_count} values, but got {len(values)}.")
                log_operation(line, "FAILURE")
                return
            status = create_record(type_name, values)
            if status == "FAILURE":
                print(f"[DEBUG] create_record FAILURE: duplicate primary key or all pages full.")
            else:
                print(f"[DEBUG] create_record status: {status}")
            log_operation(line, status)
        elif lower.startswith("search record"):
            if len(tokens) < 4:
                print(f"[DEBUG] search_record FAILURE: not enough tokens")
                log_operation(line, "FAILURE")
                return
            type_name = tokens[2]
            pk_value = tokens[3]
            result = search_record(type_name, pk_value)
            if result is not None:
                print(f"[DEBUG] search_record SUCCESS: {result}")
                search_results.append(result.rstrip('\n'))
                log_operation(line, "SUCCESS")
            else:
                from system_catalog import load_catalog
                catalog = load_catalog()
                type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
                if not type_entry:
                    print(f"[DEBUG] search_record FAILURE: type '{type_name}' not found.")
                else:
                    print(f"[DEBUG] search_record FAILURE: record with primary key '{pk_value}' not found.")
                log_operation(line, "FAILURE")
        elif lower.startswith("delete record"):
            if len(tokens) < 4:
                print(f"[DEBUG] delete_record FAILURE: not enough tokens")
                log_operation(line, "FAILURE")
                return
            type_name = tokens[2]
            pk_value = tokens[3]
            status = delete_record(type_name, pk_value)
            if status == "FAILURE":
                from system_catalog import load_catalog
                catalog = load_catalog()
                type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
                if not type_entry:
                    print(f"[DEBUG] delete_record FAILURE: type '{type_name}' not found.")
                else:
                    print(f"[DEBUG] delete_record FAILURE: record with primary key '{pk_value}' not found.")
            else:
                print(f"[DEBUG] delete_record status: {status}")
            log_operation(line, status)
        else:
            print(f"[DEBUG] Unknown command type. FAILURE.")
            log_operation(line, "FAILURE")
    except Exception as e:
        print(f"[DEBUG] Exception during command processing: {e}")
        log_operation(line, "FAILURE")

def main():
    # Check for exactly one command-line argument
    if len(sys.argv) != 2:
        print("Usage: python3 archive.py <input_file_path>")
        return

    input_file_path = sys.argv[1]

    # Clear the contents of output.txt before processing
    with open("output.txt", "w") as f:
        pass  # Truncate the file to zero length

    print(f"[DEBUG] Starting processing of input file: {input_file_path}")
    search_results = []
    # Read and process each command from the input file
    try:
        with open(input_file_path, 'r') as f:
            for line in f:
                if line.strip():  # Skip empty or whitespace-only lines
                    process_command(line, search_results)
    except FileNotFoundError:
        print(f"Error: Input file '{input_file_path}' not found.")
        return

    # Write all search results to output.txt at once, no trailing newline
    if search_results:
        with open("output.txt", "w") as f:
            f.write('\n'.join(search_results))
    print("Processing complete.")

if __name__ == "__main__":
    main() 