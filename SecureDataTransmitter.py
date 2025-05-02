import socket
import logging
from Crypto import Crypto
from NetworkSocketConnector import NetworkSocketConnector

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SecureDataTransmitter:
    """Manages encrypted data transmission over TCP sockets."""
    
    def __init__(self, connector: NetworkSocketConnector, crypto: Crypto):
        """Initialize with a socket connector and crypto handler.
        
        Args:
            connector: NetworkSocketConnector instance for socket creation.
            crypto: Crypto instance for encryption/decryption.
        """
        self.connector = connector
        self.crypto = crypto
        logger.debug("Initialized SecureDataTransmitter")

    def send_data(self, socket: socket.socket, data: bytes) -> bytes:
        """Encrypt and send data over the socket, return decrypted response.
        
        Args:
            socket: Connected socket to send data over.
            data: Data to encrypt and send.
        
        Returns:
            bytes: Decrypted response from the peer (empty for servers).
        
        Raises:
            socket.error: If transmission fails.
        """
        encrypted_data = self.crypto.encrypt(data)
        socket.sendall(len(encrypted_data).to_bytes(4, byteorder='big') + encrypted_data)
        logger.debug("Sent %d encrypted bytes", len(encrypted_data))
        
        try:
            length_bytes = socket.recv(4)
            if not length_bytes:
                logger.debug("No response received")
                return b""
            length = int.from_bytes(length_bytes, byteorder='big')
            encrypted_response = socket.recv(length)
            response = self.crypto.decrypt(encrypted_response)
            logger.debug("Received %d decrypted bytes", len(response))
            return response
        except socket.error as e:
            logger.error("Failed to receive response: %s", e)
            raise

    def start_server(self, socket: socket.socket) -> None:
        """Run a server to receive and respond to encrypted client data.
        
        Args:
            socket: Server socket to accept connections.
        
        Raises:
            socket.error: If transmission fails.
            KeyboardInterrupt: To stop the server.
        """
        try:
            while True:
                client_socket = socket
                try:
                    length_bytes = client_socket.recv(4)
                    if not length_bytes:
                        logger.debug("Client disconnected")
                        break
                    length = int.from_bytes(length_bytes, byteorder='big')
                    encrypted_data = client_socket.recv(length)
                    data = self.crypto.decrypt(encrypted_data)
                    logger.debug("Received %d decrypted bytes", len(data))
                    
                    # Echo back the received data as a simple response
                    response = data
                    encrypted_response = self.crypto.encrypt(response)
                    client_socket.sendall(len(encrypted_response).to_bytes(4, byteorder='big') + encrypted_response)
                    logger.debug("Sent %d encrypted response bytes", len(encrypted_response))
                except socket.error as e:
                    logger.error("Server transmission error: %s", e)
                    break
                finally:
                    client_socket.close()
                    logger.debug("Closed client socket")
                    if socket != client_socket:
                        socket.close()
                        break
        except KeyboardInterrupt:
            logger.debug("Server stopped by user")
            socket.close()