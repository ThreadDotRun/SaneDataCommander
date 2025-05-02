import logging
from typing import List, Dict, Any, Optional, Union
from typing import Tuple

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQLMaker:
    """A class to generate SQL statements for various database operations in a dialect-agnostic way.
    
    Args:
        dialect: The database dialect (e.g., 'mysql', 'postgresql', 'sqlite'). Defaults to 'generic'.
    """
    def __init__(self, dialect: str = "generic"):
        self.dialect = dialect.lower()
        logger.debug(f"Initialized SQLMaker with dialect: {self.dialect}")

    def create_table(self, 
                    table_name: str, 
                    columns: Dict[str, str], 
                    primary_key: Optional[Union[str, List[str]]] = None, 
                    if_not_exists: bool = True) -> str:
        if not table_name or not columns:
            raise ValueError("Table name and columns must not be empty")

        clauses = []
        if if_not_exists:
            clauses.append("CREATE TABLE IF NOT EXISTS")
        else:
            clauses.append("CREATE TABLE")

        clauses.append(table_name)

        col_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
        if primary_key:
            pk_cols = [primary_key] if isinstance(primary_key, str) else primary_key
            col_defs.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

        clauses.append(f"({', '.join(col_defs)})")

        sql = " ".join(clauses)
        logger.debug(f"Generated CREATE TABLE SQL: {sql}")
        return sql

    def drop_table(self, table_name: str, if_exists: bool = True) -> str:
        if not table_name:
            raise ValueError("Table name must not be empty")

        sql = f"DROP TABLE {'IF EXISTS ' if if_exists else ''}{table_name}"
        logger.debug(f"Generated DROP TABLE SQL: {sql}")
        return sql

    def create_index(self, 
                    index_name: str, 
                    table_name: str, 
                    columns: Union[str, List[str]], 
                    unique: bool = False) -> str:
        if not index_name or not table_name or not columns:
            raise ValueError("Index name, table name, and columns must not be empty")

        col_list = [columns] if isinstance(columns, str) else columns
        sql = f"CREATE {'UNIQUE ' if unique else ''}INDEX {index_name} ON {table_name} ({', '.join(col_list)})"
        logger.debug(f"Generated CREATE INDEX SQL: {sql}")
        return sql

    def insert(self, 
              table_name: str, 
              data: Dict[str, Any]) -> Tuple[str, Tuple[Any, ...]]:
        if not table_name or not data:
            raise ValueError("Table name and data must not be empty")

        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        values = tuple(data[col] for col in columns)
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        logger.debug(f"Generated INSERT SQL: {sql}")
        return sql, values

    def bulk_insert(self, 
                   table_name: str, 
                   data: List[Dict[str, Any]]) -> Tuple[str, List[Tuple[Any, ...]]]:
        if not table_name or not data:
            raise ValueError("Table name and data must not be empty")

        columns = list(data[0].keys())
        if not all(set(row.keys()) == set(columns) for row in data):
            raise ValueError("All data dictionaries must have the same columns")

        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        values = [tuple(row[col] for col in columns) for row in data]
        logger.debug(f"Generated BULK INSERT SQL: {sql}")
        return sql, values

    def select(self, 
              table_name: str, 
              columns: Union[str, List[str]] = "*", 
              where: Optional[Dict[str, Any]] = None, 
              order_by: Optional[Union[str, List[str]]] = None, 
              limit: Optional[int] = None) -> Tuple[str, List[Any]]:
        if not table_name:
            raise ValueError("Table name must not be empty")

        col_list = ["*"] if columns == "*" else ([columns] if isinstance(columns, str) else columns)
        sql_parts = [f"SELECT {', '.join(col_list)} FROM {table_name}"]
        params = []

        if where:
            conditions = [f"{col} = ?" for col in where.keys()]
            sql_parts.append("WHERE " + " AND ".join(conditions))
            params.extend(where.values())

        if order_by:
            order_cols = [order_by] if isinstance(order_by, str) else order_by
            sql_parts.append("ORDER BY " + ", ".join(order_cols))

        if limit is not None:
            if self.dialect in ["mysql", "postgresql", "sqlite"]:
                sql_parts.append(f"LIMIT {limit}")
            else:
                logger.warning(f"LIMIT clause not supported for dialect: {self.dialect}")

        sql = " ".join(sql_parts)
        logger.debug(f"Generated SELECT SQL: {sql}")
        return sql, params

    def update(self, 
              table_name: str, 
              data: Dict[str, Any], 
              where: Optional[Dict[str, Any]] = None) -> Tuple[str, List[Any]]:
        if not table_name or not data:
            raise ValueError("Table name and data must not be empty")

        set_clause = ", ".join([f"{col} = ?" for col in data.keys()])
        sql_parts = [f"UPDATE {table_name} SET {set_clause}"]
        params = list(data.values())

        if where:
            conditions = [f"{col} = ?" for col in where.keys()]
            sql_parts.append("WHERE " + " AND ".join(conditions))
            params.extend(where.values())

        sql = " ".join(sql_parts)
        logger.debug(f"Generated UPDATE SQL: {sql}")
        return sql, params

    def delete(self, 
              table_name: str, 
              where: Optional[Dict[str, Any]] = None) -> Tuple[str, List[Any]]:
        if not table_name:
            raise ValueError("Table name must not be empty")

        sql_parts = [f"DELETE FROM {table_name}"]
        params = []

        if where:
            conditions = [f"{col} = ?" for col in where.keys()]
            sql_parts.append("WHERE " + " AND ".join(conditions))
            params.extend(where.values())

        sql = " ".join(sql_parts)
        logger.debug(f"Generated DELETE SQL: {sql}")
        return sql, params

class DatabaseOperations:
    def __init__(self, connector, service_name: str, version: str = "1.0", dialect: str = "generic"):
        """Initialize with a UniversalDatabaseConnector instance and SQLMaker dialect.
        
        Args:
            connector: UniversalDatabaseConnector instance
            service_name: Name of the database service to connect to
            version: Version of the configuration to use
            dialect: Database dialect for SQLMaker (e.g., 'mysql', 'postgresql', 'sqlite')
        """
        self.connector = connector
        self.service_name = service_name
        self.version = version
        self.dialect = dialect
        self.connected = False
        self.sql_maker = SQLMaker(dialect=self.dialect)
        logger.debug(f"Initialized DatabaseOperations for {service_name}:{version} with dialect {dialect}")

    def connect(self) -> bool:
        """Establish connection using the UniversalDatabaseConnector."""
        self.connected = self.connector.connect(self.service_name, self.version)
        return self.connected

    def create_table(self, table_name: str, columns: Dict[str, str], 
                    primary_key: Optional[Union[str, List[str]]] = None,
                    if_not_exists: bool = True) -> bool:
        """Create a new table using SQLMaker.
        
        Args:
            table_name: Name of the table to create
            columns: Dictionary of column names to data types (e.g., {"id": "INTEGER", "name": "TEXT"})
            primary_key: Column name(s) for primary key
            if_not_exists: Whether to add IF NOT EXISTS clause
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False
        
        try:
            sql = self.sql_maker.create_table(table_name, columns, primary_key, if_not_exists)
            result = self.connector.execute_query(sql)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False

    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """Drop a table using SQLMaker.
        
        Args:
            table_name: Name of the table to drop
            if_exists: Whether to add IF EXISTS clause
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False
        
        try:
            sql = self.sql_maker.drop_table(table_name, if_exists)
            result = self.connector.execute_query(sql)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            return False

    def create_index(self, index_name: str, table_name: str, 
                    columns: Union[str, List[str]], unique: bool = False) -> bool:
        """Create an index on a table using SQLMaker.
        
        Args:
            index_name: Name of the index to create
            table_name: Table to create index on
            columns: Column name(s) to include in index
            unique: Whether to create a unique index
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False
        
        try:
            sql = self.sql_maker.create_index(index_name, table_name, columns, unique)
            result = self.connector.execute_query(sql)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e}")
            return False

    def insert(self, table_name: str, data: Dict[str, Any]) -> bool:
        """Insert a single row into a table using SQLMaker.
        
        Args:
            table_name: Name of the table to insert into
            data: Dictionary of column names to values
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.bulk_insert(table_name, [data])

    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """Insert multiple rows into a table using SQLMaker.
        
        Args:
            table_name: Name of the table to insert into
            data: List of dictionaries with column names to values
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False
        
        if not data:
            return True
        
        try:
            sql, values = self.sql_maker.bulk_insert(table_name, data)
            for value_tuple in values:
                result = self.connector.execute_query(sql, value_tuple)
                if result is None:
                    return False
            return True
        except Exception as e:
            logger.error(f"Failed to insert into {table_name}: {e}")
            return False

    def select(self, table_name: str, columns: Union[str, List[str]] = "*",
              where: Optional[Dict[str, Any]] = None,
              order_by: Optional[Union[str, List[str]]] = None,
              limit: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """Select rows from a table using SQLMaker.
        
        Args:
            table_name: Name of the table to select from
            columns: Column(s) to select (defaults to all)
            where: Dictionary of column names to values for WHERE clause
            order_by: Column(s) to order by
            limit: Maximum number of rows to return (not supported in all databases)
            
        Returns:
            List of dictionaries with results, or None on error
        """
        if not self.connected:
            if not self.connect():
                return None
        
        try:
            sql, params = self.sql_maker.select(table_name, columns, where, order_by, limit)
            result = self.connector.execute_query(sql, params)
            if result is None:
                return None
            col_list = ["*"] if columns == "*" else ([columns] if isinstance(columns, str) else columns)
            return [dict(zip(col_list, row)) for row in result]
        except Exception as e:
            logger.error(f"Failed to select from {table_name}: {e}")
            return None

    def update(self, table_name: str, data: Dict[str, Any],
              where: Optional[Dict[str, Any]] = None) -> bool:
        """Update rows in a table using SQLMaker.
        
        Args:
            table_name: Name of the table to update
            data: Dictionary of column names to new values
            where: Dictionary of column names to values for WHERE clause
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False
        
        try:
            sql, params = self.sql_maker.update(table_name, data, where)
            result = self.connector.execute_query(sql, params)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to update {table_name}: {e}")
            return False

    def delete(self, table_name: str, where: Optional[Dict[str, Any]] = None) -> bool:
        """Delete rows from a table using SQLMaker.
        
        Args:
            table_name: Name of the table to delete from
            where: Dictionary of column names to values for WHERE clause
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connected:
            if not self.connect():
                return False
        
        try:
            sql, params = self.sql_maker.delete(table_name, where)
            result = self.connector.execute_query(sql, params)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to delete from {table_name}: {e}")
            return False

    def close(self):
        """Close the connection."""
        self.connector.close()
        self.connected = False
        logger.debug(f"Closed connection for {self.service_name}:{self.version}")