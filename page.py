from typing import List, Optional, Any
from record import Record

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