# Dune Archive System – CMPE321 Project 4

## 🗂️ Overview

This project implements a file-based database management system inspired by the *Dune* universe. It supports type creation, record insertion, search, and deletion operations. Records are stored in a binary format using a page-based structure. The system was developed for the CMPE321 – Introduction to Database Systems course.

## 👨‍💻 Authors

- Volkan Bora Seki – 2021400156  
- Yağız Kaan Aydoğdu – 2021400225

## ▶️ How to Run

### 1. Requirements

- Python 3.7 or higher  
- No external libraries required

### 2. Directory Structure

```
.
├── archive.py
├── report.pdf
└── (generated files: *.db, system_catalog.json, log.csv, output.txt)
```

### 3. Running the Program

```bash
python3 archive.py input.txt
```

The program will read and process commands from `input.txt` line by line.

---

## 📝 Input Format

Each line in the input file represents a command. Supported commands:

- **Create Type**
  ```
  create type <type_name> <field_count> <pk_index> <field1_name> <field1_type> ...
  ```

- **Create Record**
  ```
  create record <type_name> <value1> <value2> ...
  ```

- **Search Record**
  ```
  search record <type_name> <primary_key_value>
  ```

- **Delete Record**
  ```
  delete record <type_name> <primary_key_value>
  ```

### Example

```text
create type house 6 1 name str origin str leader str military_strength int wealth int spice_production int
create record house Atreides Caladan Duke 8000 5000 150
search record house Atreides
delete record house Atreides
```

---

## 📦 Output and Generated Files

- `system_catalog.json` – Metadata for all defined types
- `<type_name>.db` – Binary data file for each type
- `log.csv` – Logs all operations with timestamp and status
- `output.txt` – Results of successful `search` operations


---

## ⚠️ Notes & Assumptions

- Only standard Python libraries are used.

---
