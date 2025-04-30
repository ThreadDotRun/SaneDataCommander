import sqlite3
import csv
import json
import logging
from datetime import datetime

# Configure logging for Distributor
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Distributor:
    _configs = {}

    def __init__(self, db_path="configs.db"):
        self.db_path = db_path
        self._init_db()
        logger.debug("Initialized Distributor with db_path=%s", db_path)

    def _init_db(self):
        """Set up the SQLite database and configurations table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS configurations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        service_type TEXT NOT NULL,
                        service_name TEXT NOT NULL,
                        version TEXT NOT NULL,
                        settings TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(service_type, service_name, version)
                    )
                """)
                conn.commit()
                logger.debug("Created configurations table in %s", self.db_path)
        except sqlite3.Error as e:
            logger.error("Failed to initialize database: %s", e)
            raise

    def getConfigsFromDelimtedFile(self, file_path):
        """Load configurations from a CSV file."""
        try:
            with open(file_path, 'r', newline='') as file:
                reader = csv.DictReader(file)
                expected_columns = {'service_type', 'service_name', 'version', 'settings'}
                if not expected_columns.issubset(reader.fieldnames):
                    logger.error("CSV missing required columns: %s", reader.fieldnames)
                    return False
                for row in reader:
                    config = {
                        'service_type': row['service_type'],
                        'service_name': row['service_name'],
                        'version': row['version'],
                        'settings': json.loads(row['settings'])
                    }
                    key = (row['service_type'], row['service_name'], row['version'])
                    self._configs[key] = config
                    logger.debug("Loaded config: %s", config)
                return True
        except (FileNotFoundError, json.JSONDecodeError, csv.Error) as e:
            logger.error("Error reading CSV file %s: %s", file_path, e)
            return False

    def storeConfigsInSQLite(self):
        """Store in-memory configurations in SQLite."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for config in self._configs.values():
                    settings_json = json.dumps(config['settings'])
                    cursor.execute("""
                        INSERT OR REPLACE INTO configurations 
                        (service_type, service_name, version, settings, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (config['service_type'], config['service_name'], 
                          config['version'], settings_json, datetime.utcnow()))
                conn.commit()
                logger.debug("Stored %d configs in SQLite", len(self._configs))
                return True
        except (sqlite3.Error, TypeError) as e:
            logger.error("Error storing configs in SQLite: %s", e)
            return False

    def GetConfigureation(self, service, name, version):
        """Retrieve a configuration as a JSON string."""
        key = (service, name, version)
        config = self._configs.get(key)
        if config:
            config_json = json.dumps(config)
            logger.debug("Retrieved config from memory: %s", config_json)
            return config_json
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT service_type, service_name, version, settings 
                    FROM configurations 
                    WHERE service_type = ? AND service_name = ? AND version = ?
                """, (service, name, version))
                result = cursor.fetchone()
                if result:
                    config = {
                        'service_type': result[0],
                        'service_name': result[1],
                        'version': result[2],
                        'settings': json.loads(result[3])
                    }
                    self._configs[key] = config
                    config_json = json.dumps(config)
                    logger.debug("Retrieved config from database: %s", config_json)
                    return config_json
                logger.debug("Config not found: %s, %s, %s", service, name, version)
                return None
        except sqlite3.Error as e:
            logger.error("Error retrieving config: %s", e)
            return None

    def addConfiguration(self, config):
        """Add a new configuration to memory and database."""
        required_keys = {'service_type', 'service_name', 'version', 'settings'}
        if not all(k in config for k in required_keys):
            logger.error("Config missing required fields: %s", config)
            return False
        try:
            key = (config['service_type'], config['service_name'], config['version'])
            self._configs[key] = config
            settings_json = json.dumps(config['settings'])
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO configurations 
                    (service_type, service_name, version, settings, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (config['service_type'], config['service_name'], 
                      config['version'], settings_json, datetime.utcnow()))
                conn.commit()
                logger.debug("Added config: %s", config)
                return True
        except (sqlite3.Error, TypeError) as e:
            logger.error("Error adding config: %s", e)
            return False