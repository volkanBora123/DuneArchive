import sys
import time
from system_catalog import create_type
from db_interface import create_record, search_record, delete_record

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
            status = create_record(type_name, values)
            if status == "FAILURE":
                from system_catalog import load_catalog
                catalog = load_catalog()
                type_entry = next((t for t in catalog['types'] if t['type_name'] == type_name), None)
                if not type_entry:
                    print(f"[DEBUG] create_record FAILURE: type '{type_name}' not found.")
                else:
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