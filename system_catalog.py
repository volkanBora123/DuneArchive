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

# Example usage (for testing):
# result = create_type('create type house name:str origin:str leader:str militaryStrength:int wealth:int spiceProduction:int name')
# print(result) 