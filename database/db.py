import pyodbc
import json
import logging
import os


os.makedirs("logs", exist_ok=True)


with open(r"database/config.json", "r") as config_file:
    config = json.load(config_file)

log_file = config["logging"].get("log_file", "logs/default.log")
logging.basicConfig(
    filename=config["logging"]["log_file"],
    level=getattr(logging, config["logging"]["log_level"]),
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class SqlDatabaseConnection:
    def __init__(self, use_fetching_db=False):
        if use_fetching_db:
            # Use the fetching_db config for queries
            db_config = config["database"]["fetching_db"]
            self.connection = None
            self.connection_string = (
                f"DRIVER={db_config['driver']};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};"
                f"PWD={db_config['password']};"
                f"Connection Timeout={db_config['timeout']};"
            )
        else:
            # Use the storing_db config 
            db_config = config["database"]["storing_db"]
            self.connection = None
            
            # Check if using Windows Authentication or SQL Server Authentication
            if db_config.get('Trusted_Connection', 'No').lower() == 'yes':
                # Windows Authentication
                self.connection_string = (
                    f"DRIVER={db_config['driver']};"
                    f"SERVER={db_config['server']};"
                    f"DATABASE={db_config['database']};"
                    f"Trusted_Connection=yes;"
                    f"Connection Timeout={db_config['timeout']};"
                )
            else:
                # SQL Server Authentication
                self.connection_string = (
                    f"DRIVER={db_config['driver']};"
                    f"SERVER={db_config['server']};"
                    f"DATABASE={db_config['database']};"
                    f"UID={db_config['username']};"
                    f"PWD={db_config['password']};"
                    f"Connection Timeout={db_config['timeout']};"
                )

    def connect(self):
        try:
            print(f"Attempting to connect with: {self.connection_string}")  # Debug line
            self.connection = pyodbc.connect(self.connection_string)
            print("Database connection established successfully.")
            logging.info("Database connection established successfully.")
            return True
        except pyodbc.Error as e:
            print(f"Error in connection: {e}")
            self.connection = None
            logging.error(f"Connection error: {e}")
            return False

    def get_connection(self):
        if self.connection is None:
            if not self.connect():
                return None
        
        try:
            # Test connection
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return self.connection
        except pyodbc.Error as e:
            print(f"Connection test failed, reconnecting: {e}")
            if self.connect():
                return self.connection
            return None

class SqlDatabaseConnectionLegacy:
    def __init__(self):
        db_config = config["database"]["storing_db"]
        self.connection = None
        
        # Check authentication method
        if db_config.get('Trusted_Connection', 'No').lower() == 'yes':
            # Windows Authentication
            self.connection_string = (
                f"DRIVER={db_config['driver']};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"Trusted_Connection=yes;"
                f"Connection Timeout={db_config['timeout']};"
            )
        else:
            # SQL Server Authentication
            self.connection_string = (
                f"DRIVER={db_config['driver']};"
                f"SERVER={db_config['server']};"
                f"DATABASE={db_config['database']};"
                f"UID={db_config['username']};"
                f"PWD={db_config['password']};"
                f"Connection Timeout={db_config['timeout']};"
            )

    def connect(self):
        try:
            print(f"Attempting to connect with: {self.connection_string}")  
            self.connection = pyodbc.connect(self.connection_string)
            print("Connection to storing DB established successfully.")
            logging.info("Connection to storing DB established successfully.")
        except pyodbc.Error as e:
            print("Error in connection:", e)
            self.connection = None
            logging.error("Connection error: %s", e)

    def get_connection(self):
        if self.connection is None:
            self.connect()
        return self.connection

