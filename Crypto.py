import json
import logging
from typing import Optional
from abc import ABC, abstractmethod
from Distributor import Distributor

# Check library availability at runtime
try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    from Crypto.Cipher import AES
    PYCRYPTODOME_AVAILABLE = True
except ImportError:
    PYCRYPTODOME_AVAILABLE = False

import base64

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoPlugin(ABC):
    """Base class for encryption plugins."""
    
    @abstractmethod
    def __init__(self, params: dict):
        """Initialize the plugin with configuration parameters."""
        pass
    
    @abstractmethod
    def encrypt(self, data: bytes) -> bytes:
        """Encrypt the data."""
        pass
    
    @abstractmethod
    def decrypt(self, data: bytes) -> bytes:
        """Decrypt the data."""
        pass

class XORPlugin(CryptoPlugin):
    """Plugin for XOR-based encryption."""
    
    def __init__(self, params: dict):
        self.xor_byte = params.get("byte")
        if not isinstance(self.xor_byte, int) or self.xor_byte < 0 or self.xor_byte > 255:
            logger.error("Invalid XOR byte: %s", self.xor_byte)
            raise ValueError("XOR byte must be an integer between 0 and 255")
        logger.debug("Initialized XORPlugin with byte %d", self.xor_byte)
    
    def encrypt(self, data: bytes) -> bytes:
        encrypted = bytes(b ^ self.xor_byte for b in data)
        logger.debug("Encrypted %d bytes with XOR", len(data))
        return encrypted
    
    def decrypt(self, data: bytes) -> bytes:
        decrypted = bytes(b ^ self.xor_byte for b in data)
        logger.debug("Decrypted %d bytes with XOR", len(data))
        return decrypted

class AES_CBC_CryptographyPlugin(CryptoPlugin):
    """Plugin for AES-CBC encryption using cryptography library."""
    
    def __init__(self, params: dict):
        if not CRYPTOGRAPHY_AVAILABLE:
            logger.error("cryptography library not available")
            raise ValueError("cryptography library is required for AES-CBC")
        try:
            self.key = base64.b64decode(params.get("key"))
            self.iv = base64.b64decode(params.get("iv"))
            if len(self.key) not in [16, 24, 32] or len(self.iv) != 16:
                logger.error("Invalid AES key or IV length: key=%d, iv=%d", len(self.key), len(self.iv))
                raise ValueError("Invalid AES key or IV length")
            logger.debug("Initialized AES_CBC_CryptographyPlugin")
        except (base64.binascii.Error, TypeError) as e:
            logger.error("Invalid base64 key or IV: %s", e)
            raise ValueError("Key and IV must be valid base64 strings")

    def encrypt(self, data: bytes) -> bytes:
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.iv), backend=default_backend())
        encryptor = cipher.encryptor()
        # Pad data to AES block size (16 bytes)
        padding_length = 16 - (len(data) % 16)
        padded_data = data + bytes([padding_length] * padding_length)
        encrypted = encryptor.update(padded_data) + encryptor.finalize()
        logger.debug("Encrypted %d bytes with AES-CBC", len(data))
        return encrypted
    
    def decrypt(self, data: bytes) -> bytes:
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_data = decryptor.update(data) + decryptor.finalize()
        padding_length = padded_data[-1]
        decrypted = padded_data[:-padding_length]
        logger.debug("Decrypted %d bytes with AES-CBC", len(decrypted))
        return decrypted

class AES_GCM_PycryptodomePlugin(CryptoPlugin):
    """Plugin for AES-GCM encryption using pycryptodome library."""
    
    def __init__(self, params: dict):
        if not PYCRYPTODOME_AVAILABLE:
            logger.error("pycryptodome library not available")
            raise ValueError("pycryptodome library is required for AES-GCM")
        try:
            self.key = base64.b64decode(params.get("key"))
            self.nonce = base64.b64decode(params.get("nonce"))
            if len(self.key) not in [16, 24, 32] or len(self.nonce) != 12:
                logger.error("Invalid AES key or nonce length: key=%d, nonce=%d", len(self.key), len(self.nonce))
                raise ValueError("Invalid AES key or nonce length")
            logger.debug("Initialized AES_GCM_PycryptodomePlugin")
        except (base64.binascii.Error, TypeError) as e:
            logger.error("Invalid base64 key or nonce: %s", e)
            raise ValueError("Key and nonce must be valid base64 strings")

    def encrypt(self, data: bytes) -> bytes:
        cipher = AES.new(self.key, AES.MODE_GCM, nonce=self.nonce)
        ciphertext, tag = cipher.encrypt_and_digest(data)
        encrypted = ciphertext + tag
        logger.debug("Encrypted %d bytes with AES-GCM", len(data))
        return encrypted
    
    def decrypt(self, data: bytes) -> bytes:
        tag = data[-16:]
        ciphertext = data[:-16]
        cipher = AES.new(self.key, AES.MODE_GCM, nonce=self.nonce)
        decrypted = cipher.decrypt_and_verify(ciphertext, tag)
        logger.debug("Decrypted %d bytes with AES-GCM", len(decrypted))
        return decrypted

class Crypto:
    """Handles message cryptography using a plugin-based architecture."""
    
    def __init__(self, distributor: Distributor, service_name: str, version: str = "1.0"):
        """Initialize with a Distributor for crypto configuration.
        
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
        self.plugin = self._load_crypto_plugin()
        logger.debug("Initialized Crypto for %s:%s with plugin %s", service_name, version, self.plugin.__class__.__name__)

    def _load_crypto_plugin(self) -> CryptoPlugin:
        """Load the appropriate crypto plugin based on configuration.
        
        Returns:
            CryptoPlugin: Initialized plugin instance.
        
        Raises:
            ValueError: If configuration is missing or invalid.
        """
        config_json = self.distributor.GetConfigureation("network", self.service_name, self.version)
        if not config_json:
            logger.error("Crypto configuration not found for %s:%s", self.service_name, self.version)
            raise ValueError("Crypto configuration not found")
        
        try:
            config = json.loads(config_json)
            crypto_settings = config.get("settings", {}).get("crypto", {})
            crypto_type = crypto_settings.get("type")
            params = crypto_settings.get("params", {})
            
            plugin_map = {"xor": XORPlugin}
            if CRYPTOGRAPHY_AVAILABLE:
                plugin_map["cryptography:aes-cbc"] = AES_CBC_CryptographyPlugin
            if PYCRYPTODOME_AVAILABLE:
                plugin_map["pycryptodome:aes-gcm"] = AES_GCM_PycryptodomePlugin
            
            plugin_class = plugin_map.get(crypto_type)
            if not plugin_class:
                logger.error("Unsupported crypto type: %s", crypto_type)
                raise ValueError(f"Unsupported crypto type: {crypto_type}")
            
            return plugin_class(params)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Failed to parse crypto configuration: %s", e)
            raise ValueError("Invalid crypto configuration")

    def encrypt(self, data: bytes) -> bytes:
        """Encrypt data using the loaded plugin.
        
        Args:
            data: Data to encrypt.
        
        Returns:
            bytes: Encrypted data.
        """
        return self.plugin.encrypt(data)

    def decrypt(self, data: bytes) -> bytes:
        """Decrypt data using the loaded plugin.
        
        Args:
            data: Data to decrypt.
        
        Returns:
            bytes: Decrypted data.
        """
        return self.plugin.decrypt(data)