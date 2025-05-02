from UniversalDatabaseConnector import UniversalDatabaseConnector

# Assuming the configurations are loaded from a CSV file as in the previous response
connector = UniversalDatabaseConnector()
connector.load_configs("databases.csv")  # Load your database configurations

# Create a DatabaseOperations instance for SQLite (or any other database)
db_ops = DatabaseOperations(connector, service_name="test_db_sqlite", version="1.0")
if db_ops.connect():
    # Create a table for gods
    columns = {
        "id": "INTEGER",
        "name": "TEXT",
        "color": "TEXT",
        "expression": "TEXT"
    }
    db_ops.create_table("gods", columns, primary_key="id")

    # Insert data for Twin & Twoon God
    db_ops.insert("gods", {"id": 1, "name": "Twin God", "color": "Red", "expression": "Angry"})
    db_ops.insert("gods", {"id": 2, "name": "Twoon God", "color": "Blue", "expression": "Menacing"})

    # Query the data
    results = db_ops.select("gods")
    print(results)

    # Close the connection
    db_ops.close()