import json
import sqlite3
import pymysql
import logging
import threading
from queue import Queue
from typing import List, Dict, Any, Optional, Union, Tuple
from DatabaseOperations import SQLMaker

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

from Distributor import Distributor

class DBConnectionPool:
    def __init__(self, max_connections=10):
        self.max_connections = max_connections
        self.pool = Queue(maxsize=max_connections)
        self.lock = threading.Lock()
        self.config = None
        self.driver = None

    def initialize_pool(self, config, driver):
        """Initialize the pool with connections based on config and driver."""
        self.config = config
        self.driver = driver
        try:
            for _ in range(self.max_connections):
                conn = self._create_connection()
                self.pool.put(conn)
                logger.debug("Initialized connection for pool")
        except Exception as e:
            logger.error("Failed to initialize connection pool: %s", e)
            raise

    def _create_connection(self):
        """Create a new connection based on driver."""
        settings = self.config["settings"]
        if self.driver == "sqlite3":
            conn = sqlite3.connect(settings["db_path"], check_same_thread=False, timeout=5)
            conn.execute("PRAGMA journal_mode=WAL")  # Enable Write-Ahead Logging
            return conn
        elif self.driver == "pymysql":
            return pymysql.connect(
                host=settings["host"],
                port=settings["port"],
                user=settings["user"],
                password=settings["password"],
                database=settings["database"]
            )
        else:
            raise ValueError(f"Unsupported driver: {self.driver}")

    def get_connection(self):
        """Get a connection from the pool, creating a new one if necessary."""
        logger.debug("Getting connection, pool size: %d", self.pool.qsize())
        with self.lock:
            if self.pool.empty() and self.pool.qsize() < self.max_connections:
                logger.debug("Creating new connection")
                conn = self._create_connection()
                return conn
        try:
            conn = self.pool.get(timeout=5)
            logger.debug("Retrieved connection from pool")
            return conn
        except Queue.Empty:
            logger.error("Connection pool empty after timeout")
            raise RuntimeError("No available connections")

    def release_connection(self, conn):
        """Return a connection to the pool."""
        logger.debug("Releasing connection, pool size: %d", self.pool.qsize())
        try:
            with self.lock:
                if self.pool.qsize() < self.max_connections:
                    self.pool.put(conn, timeout=10)  # Increased timeout
                    logger.debug("Connection returned to pool")
                else:
                    logger.warning("Connection pool full, not returning connection")
        except Queue.Full as e:
            logger.error("Failed to return connection to pool, queue full: %s", e)
            # Do not close the connection to preserve thread-local reference
        except Exception as e:
            logger.error("Unexpected error releasing connection: %s", e)
            # Do not close the connection to preserve thread-local reference

class UniversalDatabaseConnector:
    def __init__(self, db_path="my_configs.db"):
        """Initialize with a Distributor and thread-local storage."""
        self.distributor = Distributor(db_path=db_path)
        self.thread_local = threading.local()
        self.connection_pools = {}  # Dictionary to store connection pools by service_name:version
        self.lock = threading.Lock()  # Lock for pool initialization
        logger.debug("Initialized UniversalDatabaseConnector with db_path=%s", db_path)

    def load_configs(self, csv_path):
        """Load configurations from a CSV file (assumed thread-safe in Distributor)."""
        result = self.distributor.getConfigsFromDelimtedFile(csv_path)
        if result:
            self.distributor.storeConfigsInSQLite()
            logger.debug("Loaded and stored configs from %s", csv_path)
        return result

    def connect(self, service_name, version="1.0"):
        """Connect to a database using a connection pool."""
        try:
            pool_key = f"{service_name}:{version}"
            # Initialize connection pool if not already created
            with self.lock:
                if pool_key not in self.connection_pools:
                    config_json = self.distributor.GetConfigureation("database", service_name, version)
                    if not config_json:
                        logger.error("Configuration not found for %s, version %s", service_name, version)
                        return False

                    config = json.loads(config_json)
                    settings = config["settings"]
                    driver = settings.get("driver")

                    if driver not in ["sqlite3", "pymysql"]:
                        logger.error("Unsupported driver: %s", driver)
                        return False

                    # Optimize max_connections for SQLite
                    max_connections = 2 if driver == "sqlite3" else 10
                    pool = DBConnectionPool(max_connections=max_connections)
                    pool.initialize_pool(config, driver)
                    self.connection_pools[pool_key] = pool
                    logger.debug("Created connection pool for %s:%s with max_connections=%d", service_name, version, max_connections)

            # Get a connection for the current thread if not already assigned
            if not hasattr(self.thread_local, pool_key):
                pool = self.connection_pools[pool_key]
                conn = pool.get_connection()
                setattr(self.thread_local, pool_key, conn)
                logger.debug("Assigned connection to thread for %s:%s", service_name, version)
            return True
        except (sqlite3.Error, pymysql.Error, KeyError, json.JSONDecodeError, ValueError) as e:
            logger.error("Connection failed: %s", e)
            return False

    def execute_query(self, query: str, params: Optional[Union[Tuple, List]] = None) -> Optional[Any]:
        """Execute a query on the thread-local connection with optional parameters."""
        for pool_key in self.connection_pools:
            conn = getattr(self.thread_local, pool_key, None)
            if conn:
                try:
                    cursor = conn.cursor()
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
                    # Only fetch results for SELECT queries
                    if query.strip().upper().startswith("SELECT"):
                        results = cursor.fetchall()
                    else:
                        results = True  # Indicate success for non-SELECT queries
                    conn.commit()
                    logger.debug("Executed query: %s, params: %s, results: %s", query, params, results)
                    return results
                except (sqlite3.Error, pymysql.Error) as e:
                    logger.error("Query failed: %s", e)
                    conn.rollback()
                    return None
                finally:
                    # Return connection to pool but keep it in thread_local
                    self.connection_pools[pool_key].release_connection(conn)
            else:
                logger.error("No active connection for thread")
                return None
        logger.error("No active connection for thread")
        return None

    def close(self):
        """Close all connection pools and thread-local connections."""
        with self.lock:
            for pool_key, pool in self.connection_pools.items():
                while not pool.pool.empty():
                    conn = pool.pool.get()
                    conn.close()
                logger.debug("Closed connection pool for %s", pool_key)
            self.connection_pools.clear()

            # Clear thread-local connections
            for attr in list(self.thread_local.__dict__.keys()):
                delattr(self.thread_local, attr)
            logger.debug("Cleared all thread-local connections")

class DatabaseOperations:
    def __init__(self, connector, service_name: str, version: str = "1.0"):
        """
        Initialize with a UniversalDatabaseConnector instance.
        
        Args:
            connector: UniversalDatabaseConnector instance
            service_name: Name of the database service to connect to
            version: Version of the configuration to use
        """
        self.connector = connector
        self.service_name = service_name
        self.version = version
        self.connected = False
        # Determine dialect based on connector configuration
        config_json = self.connector.distributor.GetConfigureation("database", service_name, version)
        if config_json:
            config = json.loads(config_json)
            driver = config["settings"].get("driver", "sqlite3")
            self.dialect = "sqlite" if driver == "sqlite3" else "mysql" if driver == "pymysql" else "generic"
        else:
            self.dialect = "generic"
        self.sql_maker = SQLMaker(dialect=self.dialect)
        logger.debug(f"Initialized DatabaseOperations for {service_name}:{version} with dialect {self.dialect}")

    def connect(self) -> bool:
        """Establish connection using the UniversalDatabaseConnector."""
        self.connected = self.connector.connect(self.service_name, self.version)
        return self.connected

    def create_table(self, table_name: str, columns: Dict[str, str], 
                    primary_key: Optional[Union[str, List[str]]] = None,
                    if_not_exists: bool = True) -> bool:
        """
        Create a new table using SQLMaker.
        
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
        """
        Drop a table using SQLMaker.
        
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
        """
        Create an index on a table using SQLMaker.
        
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
        """
        Insert a single row into a table using SQLMaker.
        
        Args:
            table_name: Name of the table to insert into
            data: Dictionary of column names to values
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.bulk_insert(table_name, [data])

    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """
        Insert multiple rows into a table using SQLMaker.
        
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
            return True  # Nothing to insert
            
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
        """
        Select rows from a table using SQLMaker.
        
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
                
            # Convert to list of dictionaries
            col_list = ["*"] if columns == "*" else ([columns] if isinstance(columns, str) else columns)
            return [dict(zip(col_list, row)) for row in result]
        except Exception as e:
            logger.error(f"Failed to select from {table_name}: {e}")
            return None

    def update(self, table_name: str, data: Dict[str, Any],
              where: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update rows in a table using SQLMaker.
        
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
        """
        Delete rows from a table using SQLMaker.
        
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
        """Close all connection pools and thread-local connections."""
        logger.debug("Starting to close connection pools")
        with self.lock:
            for pool_key, pool in list(self.connection_pools.items()):
                logger.debug("Closing pool %s with %d connections", pool_key, pool.pool.qsize())
                while not pool.pool.empty():
                    try:
                        conn = pool.pool.get(timeout=1)
                        conn.close()
                        logger.debug("Closed connection in pool %s", pool_key)
                    except Queue.Empty:
                        logger.debug("Pool %s is empty", pool_key)
                        break
                    except Exception as e:
                        logger.error("Error closing connection in pool %s: %s", pool_key, e)
                logger.debug("Closed connection pool for %s", pool_key)
            self.connection_pools.clear()
            logger.debug("Cleared all connection pools")

            # Clear thread-local connections
            logger.debug("Clearing thread-local connections: %s", list(self.thread_local.__dict__.keys()))
            for attr in list(self.thread_local.__dict__.keys()):
                logger.debug("Removing thread-local attribute: %s", attr)
                delattr(self.thread_local, attr)
            logger.debug("Cleared all thread-local connections")
        logger.debug("Completed closing all resources")