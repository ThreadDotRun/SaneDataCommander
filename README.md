# Universal Database Connector

A Python library for managing database operations across multiple database systems (SQLite, MySQL, PostgreSQL, SQL Server) using a universal connector with connection pooling and dialect-agnostic SQL generation.

## Features
- **Universal Connectivity**: Supports SQLite, MySQL, PostgreSQL, and SQL Server through a single interface.
- **Connection Pooling**: Efficiently manages database connections with thread-safe pooling.
- **Dialect-Agnostic SQL**: Generates SQL queries compatible with different database systems using the `SQLMaker` class.
- **Configuration Management**: Loads and stores database configurations from CSV files into SQLite.
- **Thread-Safe Operations**: Ensures safe concurrent access with thread-local storage and locking.
- **Comprehensive Logging**: Detailed logs for debugging and monitoring operations.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/universal-database-connector.git
   cd universal-database-connector
   ```

2. Create a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

   Ensure you have the following dependencies in `requirements.txt`:
   ```
   pymysql>=1.0.2
   psycopg2>=2.9.5
   pyodbc>=4.0.32
   pymssql>=2.2.7
   ```

4. (Optional) Install database drivers:
   - For MySQL: Ensure a MySQL server is running.
   - For PostgreSQL: Ensure a PostgreSQL server is running.
   - For SQL Server: Install the ODBC Driver for SQL Server (e.g., version 17).

## Usage

The `main.py` script demonstrates how to use the library to perform CRUD operations on a SQLite database. Below is an example:

1. Create a configuration CSV file (`configs.csv`):
   ```csv
   service_type,service_name,version,settings
   database,test_db,1.0,"{""driver"": ""sqlite3"", ""db_path"": ""test.db""}"
   ```

2. Run the main script:
   ```bash
   python main.py
   ```

   This will:
   - Load configurations from `configs.csv`.
   - Create a `users` table.
   - Insert, select, update, and delete data.
   - Log all operations and close connections.

Example code snippet from `main.py`:
```python
from UniversalDatabaseConnector import UniversalDatabaseConnector, DatabaseOperations

# Initialize connector and operations
connector = UniversalDatabaseConnector(db_path="configs.db")
connector.load_configs("configs.csv")
db_ops = DatabaseOperations(connector=connector, service_name="test_db", version="1.0")

# Create a table
db_ops.create_table(
    table_name="users",
    columns={"id": "INTEGER PRIMARY KEY AUTOINCREMENT", "name": "TEXT"},
    if_not_exists=True
)

# Insert data
db_ops.insert(table_name="users", data={"name": "Alice"})

# Select data
result = db_ops.select(table_name="users", columns=["id", "name"], where={"name": "Alice"})
print(result)
```

## Dependencies
- Python >= 3.8
- pymysql >= 1.0.2 (for MySQL)
- psycopg2 >= 2.9.5 (for PostgreSQL)
- pyodbc >= 4.0.32 (for SQL Server)
- pymssql >= 2.2.7 (optional, for SQL Server)
- sqlite3 (included with Python)

## Project Structure
```
universal-database-connector/
├── main.py                   # Example script demonstrating usage
├── Distributor.py            # Manages configuration storage and retrieval
├── UniversalDatabaseConnector.py  # Connection pooling and universal connector
├── DatabaseOperations.py     # Database operations and SQL generation
├── configs.csv               # Sample configuration file
└── requirements.txt          # Project dependencies
```

## Contributing
Contributions are welcome! To contribute:
1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Make your changes and commit the code (`git commit -m 'Add feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Open a Pull Request.

Please ensure your code follows PEP 8 style guidelines and includes appropriate tests.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For questions or issues, please open an issue on GitHub or contact [your-email@example.com](mailto:your-email@example.com).