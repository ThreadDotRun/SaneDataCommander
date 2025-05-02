import json
import sqlite3
import pymysql
import logging
import threading
from queue import Queue
from typing import List, Dict, Any, Optional, Union, Tuple

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

from Distributor import Distributor
from DatabaseOperations import DatabaseOperations, SQLMaker

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
            conn.execute("PRAGMA journal_mode=WAL")
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
        """Return a connection to the pool or log a warning if the pool is full."""
        logger.debug("Releasing connection, pool size: %d", self.pool.qsize())
        try:
            with self.lock:
                if self.pool.qsize() < self.max_connections:
                    self.pool.put(conn, timeout=10)
                    logger.debug("Connection returned to pool")
                else:
                    logger.warning("Connection pool full, retaining connection")
                    # Do not close the connection; let it be reused in the thread
        except Queue.Full as e:
            logger.error("Failed to return connection to pool, queue full: %s", e)
            # Do not close the connection
        except Exception as e:
            logger.error("Unexpected error releasing connection: %s", e)
            conn.close()

class UniversalDatabaseConnector:
    def __init__(self, db_path="my_configs.db"):
        """Initialize with a Distributor and thread-local storage."""
        self.distributor = Distributor(db_path=db_path)
        self.thread_local = threading.local()
        self.connection_pools = {}
        self.lock = threading.Lock()
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

                    max_connections = 5 if driver == "sqlite3" else 10
                    pool = DBConnectionPool(max_connections=max_connections)
                    pool.initialize_pool(config, driver)
                    self.connection_pools[pool_key] = pool
                    logger.debug("Created connection pool for %s:%s with max_connections=%d", service_name, version, max_connections)

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
                    if query.strip().upper().startswith("SELECT"):
                        results = cursor.fetchall()
                    else:
                        results = True
                    conn.commit()
                    logger.debug("Executed query: %s, params: %s, results: %s", query, params, results)
                    return results
                except (sqlite3.Error, pymysql.Error) as e:
                    logger.error("Query failed: %s", e)
                    conn.rollback()
                    return None
                finally:
                    cursor.close()  # Close the cursor, not the connection
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
            for attr in list(self.thread_local.__dict__.keys()):
                delattr(self.thread_local, attr)
            logger.debug("Cleared all thread-local connections")