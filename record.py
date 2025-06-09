from typing import List, Any

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