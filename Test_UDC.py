import json
import csv
import sys
import threading
import logging
from Distributor import Distributor
from UniversalDatabaseConnector import UniversalDatabaseConnector, DatabaseOperations
from DatabaseOperations import SQLMaker

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Create a sample configuration for the CSV
config_data = [
    {
        "service_type": "database",
        "service_name": "test_db",
        "version": "1.0",
        "settings": json.dumps({"driver": "sqlite3", "db_path": "test.db"})
    }
]

# Write the configuration to a CSV file
logger.debug("Writing configs.csv")
with open("configs.csv", "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=["service_type", "service_name", "version", "settings"])
    writer.writeheader()
    writer.writerows(config_data)
logger.debug("Wrote configs.csv")

# Initialize UniversalDatabaseConnector
logger.debug("Initializing UniversalDatabaseConnector")
connector = UniversalDatabaseConnector(db_path="configs.db")
logger.debug("Loading configurations")
if not connector.load_configs("configs.csv"):
    logger.error("Failed to load configurations")
    sys.exit(1)

# Initialize DatabaseOperations
logger.debug("Initializing DatabaseOperations")
db_ops = DatabaseOperations(connector=connector, service_name="test_db", version="1.0")

# Drop the table to ensure a clean state
logger.debug("Dropping table users")
success = db_ops.drop_table(
    table_name="users",
    if_exists=True
)
print(f"Drop table: {success}")
logger.debug("Drop table result: %s", success)

# Create a table with AUTOINCREMENT
logger.debug("Creating table users")
success = db_ops.create_table(
    table_name="users",
    columns={"id": "INTEGER PRIMARY KEY AUTOINCREMENT", "name": "TEXT"},
    if_not_exists=True
)
print(f"Create table: {success}")
logger.debug("Create table result: %s", success)

# Insert data without specifying id
logger.debug("Inserting data")
success = db_ops.insert(
    table_name="users",
    data={"name": "Alice"}
)
print(f"Insert: {success}")
logger.debug("Insert result: %s", success)

# Select data
logger.debug("Selecting data")
result = db_ops.select(
    table_name="users",
    columns=["id", "name"],
    where={"name": "Alice"},
    limit=1
)
print(f"Select: {result}")
logger.debug("Select result: %s", result)

# Update data
logger.debug("Updating data")
success = db_ops.update(
    table_name="users",
    data={"name": "Bob"},
    where={"name": "Alice"}
)
print(f"Update: {success}")
logger.debug("Update result: %s", success)

# Delete data
logger.debug("Deleting data")
success = db_ops.delete(
    table_name="users",
    where={"name": "Bob"}
)
print(f"Delete: {success}")
logger.debug("Delete result: %s", success)

# Close connection
logger.debug("Closing connection")
try:
    db_ops.close()
    logger.debug("Connection closed")
except Exception as e:
    logger.error("Error closing connection: %s", e)

# Log thread status
logger.debug("Active threads: %d, Thread list: %s", threading.active_count(), [t.name for t in threading.enumerate()])

# Confirm completion
print("Script completed successfully", flush=True)
logger.debug("Script completed successfully")
sys.exit(0)