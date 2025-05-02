import json
import socket
import logging
import time
from typing import Optional, Dict, List
from collections import defaultdict, deque
from Distributor import Distributor

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NetworkSecurity:
    """Helper class to detect and mitigate network flooding and vulnerabilities."""
    
    def __init__(self, config: dict):
        """Initialize security settings from configuration.
        
        Args:
            config: Dictionary with security settings (e.g., max_connections_per_ip, max_data_per_ip, timeout).
        """
        self.max_connections_per_ip = config.get("max_connections_per_ip", 10)  # Connections per minute
        self.max_data_per_ip = config.get("max_data_per_ip", 1024 * 1024)  # Bytes per minute (1 MB)
        self.timeout = config.get("timeout", 10)  # Seconds
        self.rate_window = config.get("rate_window", 60)  # Seconds
        self.connection_tracker: Dict[str, deque] = defaultdict(deque)  # Timestamps of connections per IP
        self.data_tracker: Dict[str, deque] = defaultdict(deque)  # (timestamp, bytes) per IP
        logger.debug("Initialized NetworkSecurity with max_connections=%d, max_data=%d, timeout=%d",
                     self.max_connections_per_ip, self.max_data_per_ip, self.timeout)

    def check_connection(self, client_addr: tuple) -> bool:
        """Check if a new connection from client_addr is allowed.
        
        Args:
            client_addr: Tuple of (host, port).
        
        Returns:
            bool: True if connection is allowed, False if blocked due to rate limiting.
        """
        client_ip = client_addr[0]
        current_time = time.time()
        
        # Remove old timestamps
        while self.connection_tracker[client_ip] and current_time - self.connection_tracker[client_ip][0] > self.rate_window:
            self.connection_tracker[client_ip].popleft()
        
        # Check connection limit
        if len(self.connection_tracker[client_ip]) >= self.max_connections_per_ip:
            logger.warning("Connection rate limit exceeded for %s: %d connections in %d seconds",
                           client_ip, len(self.connection_tracker[client_ip]), self.rate_window)
            return False
        
        # Record new connection
        self.connection_tracker[client_ip].append(current_time)
        logger.debug("Allowed connection from %s (%d/%d in %d seconds)",
                     client_ip, len(self.connection_tracker[client_ip]), self.max_connections_per_ip, self.rate_window)
        return True

    def check_data_rate(self, client_addr: tuple, data_size: int) -> bool:
        """Check if receiving data_size bytes from client_addr is allowed.
        
        Args:
            client_addr: Tuple of (host, port).
            data_size: Size of data in bytes.
        
        Returns:
            bool: True if data is allowed, False if blocked due to rate limiting.
        """
        client_ip = client_addr[0]
        current_time = time.time()
        
        # Remove old data entries
        while self.data_tracker[client_ip] and current_time - self.data_tracker[client_ip][0][0] > self.rate_window:
            self.data_tracker[client_ip].popleft()
        
        # Calculate total data in window
        total_data = sum(size for _, size in self.data_tracker[client_ip])
        if total_data + data_size > self.max_data_per_ip:
            logger.warning("Data rate limit exceeded for %s: %d bytes in %d seconds",
                           client_ip, total_data + data_size, self.rate_window)
            return False
        
        # Record new data
        self.data_tracker[client_ip].append((current_time, data_size))
        logger.debug("Allowed data from %s: %d bytes (%d/%d in %d seconds)",
                     client_ip, data_size, total_data + data_size, self.max_data_per_ip, self.rate_window)
        return True

    def set_socket_timeout(self, sock: socket.socket):
        """Set timeout on the socket to prevent slowloris attacks.
        
        Args:
            sock: Socket to configure.
        """
        sock.settimeout(self.timeout)
        logger.debug("Set socket timeout to %d seconds", self.timeout)

    def validate_data(self, data: bytes) -> bool:
        """Perform basic validation on received data.
        
        Args:
            data: Received data bytes.
        
        Returns:
            bool: True if data is valid, False if suspicious.
        """
        if not data:
            logger.warning("Empty data received")
            return False
        if len(data) > self.max_data_per_ip:
            logger.warning("Data size %d exceeds maximum allowed %d", len(data), self.max_data_per_ip)
            return False
        # Add more checks as needed (e.g., for specific protocols or formats)
        return True

class NetworkSocketConnector:
    """Establishes TCP socket connections with security checks."""
    
    def __init__(self, distributor: Distributor, service_name: str, version: str = "1.0"):
        """Initialize with a Distributor for socket configuration.
        
        Args:
            distributor: Distributor instance for configuration management.
            service_name: Name of the network service (e.g., 'server', 'client').
            version: Configuration version (default: '1.0').
        
        Raises:
            ValueError: If configuration is missing or invalid.
        """
        self.distributor = distributor
        self.service_name = service_name
        self.version = version
        self.config = self._load_socket_config()
        self.security = NetworkSecurity(self.config.get("security", {}))
        logger.debug("Initialized NetworkSocketConnector for %s:%s (role: %s)", service_name, version, self.config["role"])

    def _load_socket_config(self) -> dict:
        """Load socket configuration from Distributor.
        
        Returns:
            dict: Configuration with role, host, port, and security settings.
        
        Raises:
            ValueError: If configuration is missing or invalid.
        """
        config_json = self.distributor.GetConfigureation("network", self.service_name, self.version)
        if not config_json:
            logger.error("Socket configuration not found for %s:%s", self.service_name, self.version)
            raise ValueError("Socket configuration not found")
        
        try:
            config = json.loads(config_json)
            settings = config.get("settings", {})
            role = settings.get("role")
            if role not in ["client", "server"]:
                logger.error("Invalid role: %s", role)
                raise ValueError("Role must be 'client' or 'server'")
            host = settings.get("host")
            port = settings.get("port")
            if not isinstance(port, int) or port < 0 or port > 65535:
                logger.error("Invalid port: %s", port)
                raise ValueError("Port must be an integer between 0 and 65535")
            security = settings.get("security", {
                "max_connections_per_ip": 10,
                "max_data_per_ip": 1024 * 1024,  # 1 MB
                "timeout": 10,
                "rate_window": 60
            })
            return {"role": role, "host": host, "port": port, "security": security}
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Failed to parse socket configuration: %s", e)
            raise ValueError("Invalid socket configuration")

    def connect(self) -> socket.socket:
        """Create and return a connected TCP socket with security checks.
        
        Returns:
            socket.socket: Connected socket (server returns client socket after accept).
        
        Raises:
            socket.error: If connection fails.
            ValueError: If security checks fail.
        """
        if self.config["role"] == "server":
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.security.set_socket_timeout(server_socket)
            server_socket.bind((self.config["host"], self.config["port"]))
            server_socket.listen(5)
            logger.debug("Server listening on %s:%d", self.config["host"], self.config["port"])
            
            while True:
                try:
                    client_socket, addr = server_socket.accept()
                    if not self.security.check_connection(addr):
                        logger.warning("Rejected connection from %s due to rate limiting", addr)
                        client_socket.close()
                        continue
                    self.security.set_socket_timeout(client_socket)
                    logger.debug("Accepted connection from %s", addr)
                    server_socket.close()
                    return client_socket
                except socket.timeout:
                    logger.debug("Server socket timeout, continuing to listen")
                    continue
        else:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.security.set_socket_timeout(client_socket)
            client_socket.connect((self.config["host"], self.config["port"]))
            logger.debug("Connected to %s:%d", self.config["host"], self.config["port"])
            return client_socket

    def receive_stream(self, sock: socket.socket, client_addr: tuple, buffer_size: int = 1024) -> Optional[bytes]:
        """Receive data stream with security checks.
        
        Args:
            sock: Connected socket.
            client_addr: Tuple of (host, port) for the client.
            buffer_size: Maximum bytes to receive at once.
        
        Returns:
            bytes: Received data, or None if invalid or blocked.
        
        Raises:
            socket.timeout: If reception times out.
            socket.error: If socket fails.
        """
        try:
            data = sock.recv(buffer_size)
            if not self.security.check_data_rate(client_addr, len(data)):
                logger.warning("Data rate limit exceeded for %s, closing connection", client_addr)
                sock.close()
                return None
            if not self.security.validate_data(data):
                logger.warning("Invalid data received from %s, closing connection", client_addr)
                sock.close()
                return None
            logger.debug("Received %d bytes from %s", len(data), client_addr)
            return data
        except socket.timeout:
            logger.warning("Receive timeout for %s", client_addr)
            return None
        except socket.error as e:
            logger.error("Socket error receiving from %s: %s", client_addr, e)
            sock.close()
            return None

    def send_stream(self, sock: socket.socket, data: bytes, client_addr: tuple) -> bool:
        """Send data stream with security checks.
        
        Args:
            sock: Connected socket.
            data: Data to send.
            client_addr: Tuple of (host, port) for the client.
        
        Returns:
            bool: True if sent successfully, False otherwise.
        """
        if not self.security.validate_data(data):
            logger.warning("Invalid data to send to %s", client_addr)
            return False
        if not self.security.check_data_rate(client_addr, len(data)):
            logger.warning("Data rate limit exceeded for %s", client_addr)
            return False
        try:
            sock.send(data)
            logger.debug("Sent %d bytes to %s", len(data), client_addr)
            return True
        except socket.error as e:
            logger.error("Socket error sending to %s: %s", client_addr, e)
            sock.close()
            return False