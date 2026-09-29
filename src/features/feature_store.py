def create_feature_table(connection):

    cursor = connection.cursor()

    cursor.execute("""
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
    """)

    connection.commit()


def store_features(connection, feature_df):

    cursor = connection.cursor()

    rows = feature_df.collect()

    for row in rows:
        cursor.execute("""
            INSERT OR IGNORE INTO sensor_features
            (
                sensor_id,
                timestamp,
                temperature,
                humidity,
                temperature_change,
                rolling_avg_temperature
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row["sensor_id"],
            row["timestamp"].isoformat() if row["timestamp"] else None,
            row["temperature"],
            row["humidity"],
            row["temperature_change"],
            row["rolling_avg_temperature"]
        ))

    connection.commit()

def check_features(connection):

    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT
            sensor_id,
            timestamp,
            temperature,
            humidity,
            temperature_change,
            rolling_avg_temperature
        FROM sensor_features
        LIMIT 10
    """)

    rows = cursor.fetchall()

    for row in rows:
        print(row)