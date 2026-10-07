# Database Implementation

## Overview

This project uses SQLite to persist validated sensor data and engineered features. SQLite is a lightweight, file-based database that requires no server setup.

## Database Technology Choice

**Why SQLite**:
- ✓ No server installation needed
- ✓ Single file storage (easy to backup)
- ✓ Python built-in support (`sqlite3` module)
- ✓ Sufficient performance for this data volume
- ✓ Full SQL support
- ✓ ACID compliance (atomicity, consistency, isolation, durability)

**When to use SQLite**:
- Development and testing
- Single-user applications
- Data volumes < 1 GB
- Non-critical data (can restore from CSV)

**When NOT to use SQLite**:
- Multi-user concurrent access
- High-throughput inserts (>1000/sec)
- Complex queries on very large datasets
- Mission-critical production systems

## Database Location

```
data/processed/valid_data/sensor_data.db
```

**Configuration source**: `config/database.yaml`

```yaml
database:
  type: sqlite
  path: "/home/vvdn/Desktop/Projects/Python/Temperature-prediction-model-using-pyspark/data/processed/valid_data/sensor_data.db"
  table: "sensor_readings"
```

## Database Schema

### Table 1: sensor_readings

**Purpose**: Store validated sensor data

**Created by**: `run_training.py` (lines 143-152)

**Schema**:
```sql
CREATE TABLE IF NOT EXISTS sensor_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    UNIQUE(sensor_id, timestamp)
)
```

**Column Details**:

| Column | Type | Purpose | Constraints |
|--------|------|---------|-------------|
| `id` | INTEGER | Unique row identifier | PRIMARY KEY, AUTO-INCREMENT |
| `sensor_id` | TEXT | Manufacturing facility ID | None |
| `timestamp` | TEXT | When reading was taken | Part of UNIQUE key |
| `temperature` | REAL | Temperature in Celsius | Numeric, nullable |
| `humidity` | REAL | Humidity in percent | Numeric, nullable |

**Unique Constraint**: `UNIQUE(sensor_id, timestamp)`
- Prevents duplicate readings for same sensor at same time
- Composed of two columns (composite key)
- Uses INSERT OR IGNORE to silently skip duplicates

**Typical Data**:
```sql
SELECT * FROM sensor_readings LIMIT 3;

id | sensor_id      | timestamp              | temperature | humidity
1  | MANUFACTURING_01 | 2025-01-31T12:09:12Z | 368.73      | 50.39
2  | MANUFACTURING_01 | 2025-01-31T12:14:12Z | 397.54      | 51.87
3  | MANUFACTURING_01 | 2025-01-31T12:19:12Z | 386.60      | 58.31
```

### Table 2: sensor_features

**Purpose**: Store engineered features

**Created by**: `feature_store.py` (lines 10-23)

**Schema**:
```sql
CREATE TABLE IF NOT EXISTS sensor_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    timestamp TEXT,
    temperature REAL,
    humidity REAL,
    temperature_change REAL,
    rolling_avg_temperature REAL,
    UNIQUE(sensor_id, timestamp)
)
```

**Column Details**:

| Column | Type | Purpose |
|--------|------|---------|
| `id` | INTEGER | Unique identifier |
| `sensor_id` | TEXT | Facility identifier |
| `timestamp` | TEXT | Reading time |
| `temperature` | REAL | Raw temperature |
| `humidity` | REAL | Raw humidity |
| `temperature_change` | REAL | Change from previous reading |
| `rolling_avg_temperature` | REAL | 30-min rolling average |

**Unique Constraint**: `UNIQUE(sensor_id, timestamp)`
- Same as sensor_readings
- Allows only one feature set per (sensor, timestamp)

**Typical Data**:
```sql
SELECT * FROM sensor_features WHERE id=2;

id | sensor_id | timestamp | temperature | humidity | temp_change | rolling_avg
2  | MANUFACTURING_01 | 2025-01-31T12:14:12Z | 397.54 | 51.87 | 28.81 | 384.29
```

## Connection Management

### Opening Connection

```python
import sqlite3
import yaml

# Load configuration
with open("config/database.yaml", "r") as file:
    config = yaml.safe_load(file)

db_path = config["database"]["path"]

# Create connection
connection = sqlite3.connect(db_path)
cursor = connection.cursor()
```

**What happens**:
- If database file doesn't exist: SQLite creates it
- If database exists: Opens existing file
- Returns connection object for queries

### Executing Queries

**SELECT** (read data):
```python
cursor.execute("SELECT COUNT(*) FROM sensor_readings")
count = cursor.fetchone()[0]
print(f"Records in database: {count}")
```

**INSERT** (write data):
```python
cursor.execute("""
    INSERT INTO sensor_readings 
    (sensor_id, timestamp, temperature, humidity)
    VALUES (?, ?, ?, ?)
""", ("MANUFACTURING_01", "2025-01-31 12:09:12", 368.73, 50.39))
connection.commit()
```

**UPDATE** (modify data):
```python
cursor.execute("""
    UPDATE sensor_readings
    SET temperature = ?
    WHERE id = ?
""", (370.0, 1))
connection.commit()
```

**DELETE** (remove data):
```python
cursor.execute("DELETE FROM sensor_readings WHERE id = ?", (1,))
connection.commit()
```

### Closing Connection

```python
connection.close()
```

**Important**: Always close connections to avoid database lock issues

## Parameterized Queries

### Pattern Used in Project

```python
cursor.execute("""
    INSERT OR IGNORE INTO sensor_readings
    (sensor_id, timestamp, temperature, humidity)
    VALUES (?, ?, ?, ?)
""", (sensor_id, timestamp, temperature, humidity))
```

**Why `?` placeholders**:
- Prevents SQL injection attacks
- Automatically escapes special characters
- Values passed separately from SQL string

**Dangerous (NEVER do this)**:
```python
# VULNERABLE TO INJECTION!
query = f"INSERT INTO sensor_readings VALUES ({sensor_id}, {timestamp})"
cursor.execute(query)
```

## Duplicate Handling

### INSERT OR IGNORE

```python
cursor.execute("""
    INSERT OR IGNORE INTO sensor_readings
    (sensor_id, timestamp, temperature, humidity)
    VALUES (?, ?, ?, ?)
""", (...))
```

**How it works**:
1. Attempts to insert record
2. If UNIQUE constraint violated: Silently ignores insert
3. No error thrown, no warning given
4. Original record remains unchanged

**Example**:
```python
# First insert
cursor.execute("INSERT OR IGNORE INTO sensor_readings ...", 
               ("MANUFACTURING_01", "2025-01-31 12:09:12", 368.73, 50.39))
# Record inserted, id=1

# Duplicate insert (same sensor_id and timestamp)
cursor.execute("INSERT OR IGNORE INTO sensor_readings ...",
               ("MANUFACTURING_01", "2025-01-31 12:09:12", 369.00, 50.50))
# Silently ignored, record not updated
# Database still has original values (368.73, 50.39)

cursor.execute("SELECT COUNT(*) FROM sensor_readings")
# Result: 1 (not 2)
```

### Alternatives to INSERT OR IGNORE

**INSERT OR REPLACE**:
```python
cursor.execute("INSERT OR REPLACE INTO sensor_readings ...", (...))
```
- Updates existing record if conflict
- Useful for updating with new values

**INSERT OR UPDATE logic**:
```python
try:
    cursor.execute("INSERT INTO sensor_readings ...", (...))
except sqlite3.IntegrityError:
    cursor.execute("UPDATE sensor_readings SET temperature=? WHERE ...", (...))
```
- Explicit control over behavior
- More code but clear intent

## Database Operations in Project

### In run_training.py

**Step 1: Create connection** (lines 22-30):
```python
with open("config/database.yaml", "r") as file:
    config = yaml.safe_load(file)
db_path = config["database"]["path"]
connection = sqlite3.connect(db_path)
cursor = connection.cursor()
```

**Step 2: Create tables** (lines 143-152):
```python
cursor.execute("""CREATE TABLE IF NOT EXISTS sensor_readings (...)""")
```

**Step 3: Insert valid records** (lines 154-166):
```python
for row in valid_rows:
    cursor.execute("""INSERT OR IGNORE INTO sensor_readings ...""",
                   (row["sensor_id"], row["timestamp"], ...))
```

**Step 4: Commit transaction** (line 206):
```python
connection.commit()
```

**Step 5: Close connection** (line 207):
```python
connection.close()
```

### In feature_store.py

**Create feature table** (lines 6-23):
```python
def create_feature_table(connection):
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS sensor_features (...)""")
    connection.commit()
```

**Store features** (lines 26-53):
```python
def store_features(connection, feature_df):
    cursor = connection.cursor()
    for row in feature_df.collect():
        cursor.execute("""INSERT OR IGNORE INTO sensor_features ...""", (...))
    connection.commit()
```

**Query features** (lines 55-74):
```python
def check_features(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT ... FROM sensor_features LIMIT 10")
    for row in cursor.fetchall():
        print(row)
```

## Database Inspection

### Command Line

```bash
# Open database
sqlite3 data/processed/valid_data/sensor_data.db

# Show schema
.schema

# List tables
.tables

# Count records
SELECT COUNT(*) FROM sensor_readings;

# Show first 5 records
SELECT * FROM sensor_readings LIMIT 5;

# Exit
.quit
```

### Python Script

```python
import sqlite3

conn = sqlite3.connect("data/processed/valid_data/sensor_data.db")
cursor = conn.cursor()

# Count valid records
cursor.execute("SELECT COUNT(*) FROM sensor_readings")
print(f"Valid records: {cursor.fetchone()[0]}")

# Count features
cursor.execute("SELECT COUNT(*) FROM sensor_features")
print(f"Features stored: {cursor.fetchone()[0]}")

# Show temperature statistics
cursor.execute("""
    SELECT 
        COUNT(*),
        MIN(temperature),
        MAX(temperature),
        AVG(temperature)
    FROM sensor_readings
""")
count, min_temp, max_temp, avg_temp = cursor.fetchone()
print(f"Temperature: {count} records, min={min_temp:.1f}, max={max_temp:.1f}, avg={avg_temp:.1f}")

conn.close()
```

## Transactions and ACID

### Atomicity (All or Nothing)

```python
cursor.execute("INSERT INTO sensor_readings ...", (...))
cursor.execute("INSERT INTO sensor_readings ...", (...))
connection.commit()  # Both succeed or both fail
```

If error occurs before commit, neither record is inserted.

### Consistency

Database state remains valid after every transaction.

### Isolation

Concurrent queries don't interfere (single-user SQLite).

### Durability

Committed data persists even if power loss occurs.

## Performance Considerations

### Index Efficiency

**Most common queries**:
```sql
WHERE sensor_id = '...'
WHERE timestamp = '...'
WHERE sensor_id = '...' AND timestamp = '...'
```

**Current indexes**: None explicitly created (using UNIQUE constraint)

**Could improve with**:
```sql
CREATE INDEX idx_sensor_id ON sensor_readings(sensor_id);
CREATE INDEX idx_timestamp ON sensor_readings(timestamp);
```

### Batch Operations

**Slow**:
```python
for row in rows:
    cursor.execute("INSERT INTO sensor_readings ...", (...))
    connection.commit()  # Commits after every row!
```

**Fast**:
```python
for row in rows:
    cursor.execute("INSERT INTO sensor_readings ...", (...))
connection.commit()  # Commits after all rows
```

**Project uses**: Fast approach (single commit at end)

### Data Volume

Current project:
- **~1000 raw records**
- **~990 valid records**
- **~50 KB database file**
- **<1 second insert time**

Performance is excellent for this scale.

## Backup and Recovery

### Backup Database

```bash
cp data/processed/valid_data/sensor_data.db data/processed/valid_data/sensor_data.db.backup
```

### Restore from Backup

```bash
cp data/processed/valid_data/sensor_data.db.backup data/processed/valid_data/sensor_data.db
```

### Clear Database

```bash
rm data/processed/valid_data/sensor_data.db
# Next run will create fresh database
```

## Limitations and Future Improvements

**Current limitations**:
- No indexes (could slow queries on large data)
- No query logging
- No connection pooling
- No error handling for database operations
- No migrations system (schema changes require manual updates)

**Future improvements**:
- Use SQLAlchemy ORM (abstract database layer)
- Add indexes for common query patterns
- Implement connection pooling
- Add logging for database operations
- Use database migrations (Alembic)
- Add data validation at database level

## Next Steps

- To see how features flow to database: [Data Flow](./08-data-flow.md)
- To understand configuration: [Configuration Management](./10-configuration.md)
- To inspect the actual database: [Troubleshooting Guide](./15-troubleshooting.md)

---

**Key Takeaway**: SQLite provides simple, effective persistence for validated data and engineered features. The UNIQUE constraint prevents duplicates, and parameterized queries ensure security.
