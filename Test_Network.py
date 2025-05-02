import threading
import time
import logging
import base64
import json
from typing import Dict
from Distributor import Distributor
from Crypto import Crypto
from NetworkSocketConnector import NetworkSocketConnector
from SecureDataTransmitter import SecureDataTransmitter

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestNetwork:
    """Test class for Sane Data Commander network services components."""
    
    def __init__(self):
        """Initialize the test with a Distributor for configurations."""
        self.distributor = Distributor(db_path="network_configs.db")
        self.config_file = "network_configs.csv"
        self.test_configs = [
            {
                "crypto_type": "xor",
                "params": {"byte": 42},
                "description": "XOR encryption"
            },
            {
                "crypto_type": "cryptography:aes-cbc",
                "params": {
                    "key": base64.b64encode(b"testkey123456789012345678901234").decode(),
                    "iv": base64.b64encode(b"testiv1234567890").decode()
                },
                "description": "AES-CBC encryption (cryptography)"
            },
            {
                "crypto_type": "pycryptodome:aes-gcm",
                "params": {
                    "key": base64.b64encode(b"testkey123456789012345678901234").decode(),
                    "nonce": base64.b64encode(b"testnonce123456").decode()
                },
                "description": "AES-GCM encryption (pycryptodome)"
            }
        ]

    def create_config_file(self, crypto_type: str, params: Dict[str, str]) -> None:
        """Create a network_configs.csv file for testing.
        
        Args:
            crypto_type: The crypto plugin type (e.g., 'xor', 'cryptography:aes-cbc').
            params: Parameters for the crypto plugin (e.g., {'byte': 42}).
        """
        config_content = f"""service_type,service_name,version,settings
network,server,1.0,{{"role": "server", "host": "localhost", "port": 5000, "crypto": {{"type": "{crypto_type}", "params": {json.dumps(params)}}}}}
network,client,1.0,{{"role": "client", "host": "localhost", "port": 5000, "crypto": {{"type": "{crypto_type}", "params": {json.dumps(params)}}}}}
"""
        with open(self.config_file, 'w') as f:
            f.write(config_content)
        logger.debug("Created %s for %s", self.config_file, crypto_type)

    def run_server(self, service_name: str, version: str = "1.0") -> None:
        """Run the server in a separate thread.
        
        Args:
            service_name: Name of the server service (e.g., 'server').
            version: Configuration version (default: '1.0').
        """
        try:
            crypto = Crypto(self.distributor, service_name=service_name, version=version)
            connector = NetworkSocketConnector(self.distributor, service_name=service_name, version=version)
            transmitter = SecureDataTransmitter(connector, crypto)
            socket = connector.connect()
            transmitter.start_server(socket)
        except Exception as e:
            logger.error("Server failed: %s", e)

    def test_connectivity_and_data(self, crypto_type: str, params: Dict[str, str], description: str) -> bool:
        """Test connectivity and data transmission for a crypto plugin.
        
        Args:
            crypto_type: The crypto plugin type.
            params: Parameters for the crypto plugin.
            description: Description of the test case.
        
        Returns:
            bool: True if the test passes, False otherwise.
        """
        logger.info("Starting test: %s", description)
        self.distributor.getConfigsFromDelimtedFile(self.config_file)
        self.distributor.storeConfigsInSQLite()  # Optional, if the test expects configs to be stored in the database

        # Start server in a separate thread
        server_thread = threading.Thread(target=self.run_server, args=("server", "1.0"), daemon=True)
        server_thread.start()
        time.sleep(1)  # Give server time to start

        try:
            # Initialize client
            crypto = Crypto(self.distributor, service_name="client", version="1.0")
            connector = NetworkSocketConnector(self.distributor, service_name="client", version="1.0")
            transmitter = SecureDataTransmitter(connector, crypto)

            # Connect and send data
            socket = connector.connect()
            test_data = b"Hello, Server!"
            response = transmitter.send_data(socket, test_data)
            
            # Verify response
            if response == test_data:
                logger.info("Test passed: %s - Sent: %s, Received: %s", description, test_data, response)
                return True
            else:
                logger.error("Test failed: %s - Sent: %s, Received: %s", description, test_data, response)
                return False
        except Exception as e:
            logger.error("Test failed: %s - Error: %s", description, e)
            return False
        finally:
            socket.close()

    def run_tests(self) -> None:
        """Run all network tests."""
        logger.info("Running network tests...")
        results = []
        
        for config in self.test_configs:
            result = self.test_connectivity_and_data(
                crypto_type=config["crypto_type"],
                params=config["params"],
                description=config["description"]
            )
            results.append((config["description"], result))
        
        # Log summary
        logger.info("Test Summary:")
        for desc, result in results:
            status = "PASSED" if result else "FAILED"
            logger.info("%s: %s", desc, status)
        
        if all(result for _, result in results):
            logger.info("All tests passed!")
        else:
            logger.error("Some tests failed.")

if __name__ == "__main__":
    test_network = TestNetwork()
    test_network.run_tests()