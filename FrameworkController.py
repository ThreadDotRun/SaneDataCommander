import logging
import threading
import socket
from typing import Dict, List, Optional, Union, Any
from UniversalDatabaseConnector import UniversalDatabaseConnector, DatabaseOperations
from Crypto import Crypto
from NetworkSocketConnector import NetworkSocketConnector
from SecureDataTransmitter import SecureDataTransmitter
from GUIServer import GUIServer
from Distributor import Distributor  # Only for passing to components

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FrameworkController:
    """High-level controller for the Sane Data Commander framework, providing an interface for database, network, and GUI operations.

    This class orchestrates the framework's components using hardcoded service configurations for database, network, and GUI services,
    with separate configuration files for each. It does not directly use the Distributor class, instead passing service, name, and version
    to components for their internal configuration lookups.

    Attributes:
        db_connector (UniversalDatabaseConnector): Manages database connections.
        db_ops (DatabaseOperations): Handles database CRUD operations.
        crypto_server (Crypto): Manages encryption/decryption for network server.
        crypto_client (Crypto): Manages encryption/decryption for network client.
        socket_connector_server (NetworkSocketConnector): Establishes TCP socket connections for server.
        socket_connector_client (NetworkSocketConnector): Establishes TCP socket connections for client.
        transmitter_server (SecureDataTransmitter): Manages encrypted data transmission for server.
        transmitter_client (SecureDataTransmitter): Manages encrypted data transmission for client.
        gui_server (GUIServer): Manages the web-based GUI.
        server_thread (threading.Thread): Thread for running the GUI or network server.
    """

    def __init__(self):
        """Initialize the FrameworkController with hardcoded configuration paths for each component."""
        self.db_config_db_path = "configs.db"
        self.db_config_file = "configs.csv"
        self.network_config_db_path = "configs.db"
        self.network_config_file = "configs.csv"
        self.gui_config_db_path = "configs.db"
        self.gui_config_file = "configs.csv"
        self.gui_action_config_file = "gui_action_configs.txt"
        self.db_connector = None
        self.db_ops = None
        self.crypto_server = None
        self.crypto_client = None
        self.socket_connector_server = None
        self.socket_connector_client = None
        self.transmitter_server = None
        self.transmitter_client = None
        self.gui_server = None
        self.server_thread = None
        self.lock = threading.Lock()  # Thread-safe access to components
        logger.debug("Initialized FrameworkController with separate config paths")

    # --- Configuration Loading ---

    def load_configs(self) -> bool:
        """Load configurations for all components from their respective config files.

        Returns:
            bool: True if all configurations are loaded successfully, False otherwise.
        """
        try:
            # Load database configs
            print(f"self.db_config_db_path {self.db_config_db_path}")
            db_distributor = Distributor(db_path=self.db_config_db_path)
            if not db_distributor.getConfigsFromDelimtedFile(self.db_config_file):
                logger.error("Failed to load database configs from %s", self.db_config_file)
                return False
            if not db_distributor.storeConfigsInSQLite():
                logger.error("Failed to store database configs in %s", self.db_config_db_path)
                return False

            # Load network configs
            network_distributor = Distributor(db_path=self.network_config_db_path)
            if not network_distributor.getConfigsFromDelimtedFile(self.network_config_file):
                logger.error("Failed to load network configs from %s", self.network_config_file)
                return False
            if not network_distributor.storeConfigsInSQLite():
                logger.error("Failed to store network configs in %s", self.network_config_db_path)
                return False

            # Load GUI configs (GUI server loads gui_action_configs.txt internally)
            gui_distributor = Distributor(db_path=self.gui_config_db_path)
            if not gui_distributor.getConfigsFromDelimtedFile(self.gui_config_file):
                logger.error("Failed to load GUI configs from %s", self.gui_config_file)
                return False
            if not gui_distributor.storeConfigsInSQLite():
                logger.error("Failed to store GUI configs in %s", self.gui_config_db_path)
                return False

            logger.debug("Loaded all configs successfully")
            return True
        except Exception as e:
            logger.error("Failed to load configs: %s", e)
            return False

    # --- Initialization Methods ---

    def initialize_database(self) -> bool:
        """Initialize database components for the hardcoded database service.

        Uses service_type='database', service_name='test_db', version='1.0', with configs.csv and configs.db.

        Returns:
            bool: True if initialization and connection are successful, False otherwise.
        """
        with self.lock:
            try:
                self.db_connector = UniversalDatabaseConnector(db_path=self.db_config_db_path)
                self.db_ops = DatabaseOperations(
                    connector=self.db_connector,
                    service_name="test_db",
                    version="1.0",
                    dialect="sqlite"
                )
                success = self.db_ops.connect()
                if success:
                    logger.debug("Initialized database for test_db:1.0 with sqlite dialect")
                else:
                    logger.error("Failed to connect to database test_db:1.0")
                    self.db_connector = None
                    self.db_ops = None
                return success
            except Exception as e:
                logger.error("Failed to initialize database test_db:1.0: %s", e)
                self.db_connector = None
                self.db_ops = None
                return False

    def initialize_network(self) -> bool:
        """Initialize network components for the hardcoded network services (server and client).

        Uses service_type='network', service_name='server' and 'client', version='1.0', with network_configs.csv and network_configs.db.

        Returns:
            bool: True if initialization is successful for both server and client, False otherwise.
        """
        with self.lock:
            try:
                # Initialize server components
                self.crypto_server = Crypto(
                    distributor=Distributor(db_path=self.network_config_db_path),
                    service_name="server",
                    version="1.0"
                )
                self.socket_connector_server = NetworkSocketConnector(
                    distributor=Distributor(db_path=self.network_config_db_path),
                    service_name="server",
                    version="1.0"
                )
                self.transmitter_server = SecureDataTransmitter(
                    connector=self.socket_connector_server,
                    crypto=self.crypto_server
                )

                # Initialize client components
                self.crypto_client = Crypto(
                    distributor=Distributor(db_path=self.network_config_db_path),
                    service_name="client",
                    version="1.0"
                )
                self.socket_connector_client = NetworkSocketConnector(
                    distributor=Distributor(db_path=self.network_config_db_path),
                    service_name="client",
                    version="1.0"
                )
                self.transmitter_client = SecureDataTransmitter(
                    connector=self.socket_connector_client,
                    crypto=self.crypto_client
                )

                logger.debug("Initialized network components for server:1.0 and client:1.0")
                return True
            except Exception as e:
                logger.error("Failed to initialize network components: %s", e)
                self.crypto_server = None
                self.crypto_client = None
                self.socket_connector_server = None
                self.socket_connector_client = None
                self.transmitter_server = None
                self.transmitter_client = None
                return False

    def initialize_gui(self) -> bool:
        """Initialize GUI components for the hardcoded GUI service.

        Uses service_type='gui', service_name='web_interface', version='1.0', with test_gui_configs.csv, gui_action_configs.txt, and my_configs.db.

        Returns:
            bool: True if initialization is successful, False otherwise.
        """
        with self.lock:
            try:
                self.gui_server = GUIServer(
                    distributor=Distributor(db_path=self.gui_config_db_path),
                    service_name="web_interface",
                    version="1.0"
                )
                logger.debug("Initialized GUI for web_interface:1.0")
                return True
            except Exception as e:
                logger.error("Failed to initialize GUI web_interface:1.0: %s", e)
                self.gui_server = None
                return False

    # --- Database Operations ---

    def create_table(self, table_name: str, columns: Dict[str, str], primary_key: Optional[Union[str, List[str]]] = None, if_not_exists: bool = True) -> bool:
        """Create a table in the database.

        Args:
            table_name (str): Name of the table to create.
            columns (Dict[str, str]): Dictionary of column names to SQL data types.
            primary_key (Optional[Union[str, List[str]]]): Column(s) for the primary key.
            if_not_exists (bool): Add IF NOT EXISTS clause (default: True).

        Returns:
            bool: True if table creation is successful, False otherwise.
        """
        if not self.db_ops:
            logger.error("Database operations not initialized")
            return False
        try:
            success = self.db_ops.create_table(table_name, columns, primary_key, if_not_exists)
            logger.debug("Create table %s: %s", table_name, success)
            return success
        except Exception as e:
            logger.error("Failed to create table %s: %s", table_name, e)
            return False

    def insert_data(self, table_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> bool:
        """Insert single or multiple rows into a table.

        Args:
            table_name (str): Name of the table to insert into.
            data (Union[Dict[str, Any], List[Dict[str, Any]]]): Single dictionary or list of dictionaries with column names to values.

        Returns:
            bool: True if insertion is successful, False otherwise.
        """
        if not self.db_ops:
            logger.error("Database operations not initialized")
            return False
        try:
            if isinstance(data, dict):
                success = self.db_ops.insert(table_name, data)
            else:
                success = self.db_ops.bulk_insert(table_name, data)
            logger.debug("Insert into %s: %s", table_name, success)
            return success
        except Exception as e:
            logger.error("Failed to insert into %s: %s", table_name, e)
            return False

    def select_data(self, table_name: str, columns: Union[str, List[str]] = "*", where: Optional[Dict[str, Any]] = None, order_by: Optional[Union[str, List[str]]] = None, limit: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """Select rows from a table.

        Args:
            table_name (str): Name of the table to select from.
            columns (Union[str, List[str]]): Columns to select (default: "*").
            where (Optional[Dict[str, Any]]): Conditions for the WHERE clause.
            order_by (Optional[Union[str, List[str]]]): Columns to order by.
            limit (Optional[int]): Maximum rows to return.

        Returns:
            Optional[List[Dict[str, Any]]]: List of dictionaries with results, or None on error.
        """
        if not self.db_ops:
            logger.error("Database operations not initialized")
            return None
        try:
            result = self.db_ops.select(table_name, columns, where, order_by, limit)
            logger.debug("Select from %s: %s", table_name, result)
            return result
        except Exception as e:
            logger.error("Failed to select from %s: %s", table_name, e)
            return None

    def update_data(self, table_name: str, data: Dict[str, Any], where: Optional[Dict[str, Any]] = None) -> bool:
        """Update rows in a table.

        Args:
            table_name (str): Name of the table to update.
            data (Dict[str, Any]): Dictionary of column names to new values.
            where (Optional[Dict[str, Any]]): Conditions for the WHERE clause.

        Returns:
            bool: True if update is successful, False otherwise.
        """
        if not self.db_ops:
            logger.error("Database operations not initialized")
            return False
        try:
            success = self.db_ops.update(table_name, data, where)
            logger.debug("Update %s: %s", table_name, success)
            return success
        except Exception as e:
            logger.error("Failed to update %s: %s", table_name, e)
            return False

    def delete_data(self, table_name: str, where: Optional[Dict[str, Any]] = None) -> bool:
        """Delete rows from a table.

        Args:
            table_name (str): Name of the table to delete from.
            where (Optional[Dict[str, Any]]): Conditions for the WHERE clause.

        Returns:
            bool: True if deletion is successful, False otherwise.
        """
        if not self.db_ops:
            logger.error("Database operations not initialized")
            return False
        try:
            success = self.db_ops.delete(table_name, where)
            logger.debug("Delete from %s: %s", table_name, success)
            return success
        except Exception as e:
            logger.error("Failed to delete from %s: %s", table_name, e)
            return False

    # --- Network Operations ---

    def start_network_server(self) -> bool:
        """Start the network server in a separate thread for the hardcoded network service.

        Uses service_type='network', service_name='server', version='1.0'.

        Returns:
            bool: True if the server starts successfully, False otherwise.
        """
        if not all([self.transmitter_server, self.socket_connector_server]):
            logger.error("Network server components not initialized")
            return False
        try:
            with self.lock:
                if self.server_thread and self.server_thread.is_alive():
                    logger.warning("Network server already running")
                    return True
                socket = self.socket_connector_server.connect()
                self.server_thread = threading.Thread(
                    target=self.transmitter_server.start_server,
                    args=(socket,),
                    daemon=True
                )
                self.server_thread.start()
                logger.debug("Started network server for server:1.0")
                return True
        except Exception as e:
            logger.error("Failed to start network server: %s", e)
            return False

    def send_network_data(self, data: bytes) -> Optional[bytes]:
        """Send encrypted data to the server and receive the response using the client service.

        Uses service_type='network', service_name='client', version='1.0'.

        Args:
            data (bytes): Data to send.

        Returns:
            Optional[bytes]: Decrypted response from the server, or None on error.
        """
        if not all([self.transmitter_client, self.socket_connector_client]):
            logger.error("Network client components not initialized")
            return None
        try:
            socket = self.socket_connector_client.connect()
            response = self.transmitter_client.send_data(socket, data)
            socket.close()
            logger.debug("Sent %d bytes, received %d bytes", len(data), len(response) if response else 0)
            return response
        except Exception as e:
            logger.error("Failed to send network data: %s", e)
            return None

    # --- GUI Operations ---

    def start_gui_server(self) -> bool:
        """Start the GUI server in a separate process for the hardcoded GUI service.

        Uses service_type='gui', service_name='web_interface', version='1.0'.

        Returns:
            bool: True if the server starts successfully, False otherwise.
        """
        if not self.gui_server:
            logger.error("GUI server not initialized")
            return False
        try:
            with self.lock:
                import multiprocessing
                self.server_thread = multiprocessing.Process(
                    target=self.gui_server.start_server,
                    daemon=True
                )
                self.server_thread.start()
                logger.debug("Started GUI server process for web_interface:1.0")
                return True
        except Exception as e:
            logger.error("Failed to start GUI server: %s", e)
            return False
            # --- Cleanup ---

    def shutdown(self) -> None:
        """Shutdown all components and close connections."""
        with self.lock:
            try:
                if self.db_ops:
                    self.db_ops.close()
                    logger.debug("Closed database connections")
                if self.server_thread and self.server_thread.is_alive():
                    logger.warning("Server thread still running; requires manual termination")
                self.db_connector = None
                self.db_ops = None
                self.crypto_server = None
                self.crypto_client = None
                self.socket_connector_server = None
                self.socket_connector_client = None
                self.transmitter_server = None
                self.transmitter_client = None
                self.gui_server = None
                self.server_thread = None
                logger.debug("FrameworkController shutdown complete")
            except Exception as e:
                logger.error("Error during shutdown: %s", e)