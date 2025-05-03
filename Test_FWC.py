from FrameworkController import FrameworkController
import threading
import time

# Create a sample configuration CSV
config_content = """service_type,service_name,version,settings
database,test_db,1.0,{"driver": "sqlite3", "db_path": "test.db"}
network,server,1.0,{"role": "server", "host": "localhost", "port": 5000, "crypto": {"type": "xor", "params": {"byte": 42}}}
network,client,1.0,{"role": "client", "host": "localhost", "port": 5000, "crypto": {"type": "xor", "params": {"byte": 42}}}
gui,web_interface,1.0,{"host": "localhost", "port": 8000, "template": "default_template.html", "actions": {"button1": "click", "textbox1": "uppercase"}}
"""

with open("configs.csv", "w", encoding="utf-8", newline="") as f:
    f.write(config_content)
print("Created configs.csv")

# Initialize the controller
controller = FrameworkController(config_db_path="configs.db", config_file="configs.csv")
if not controller.initialize():
    print("Initialization failed")
    exit(1)

# Database operations
controller.create_table("users", {"id": "INTEGER", "name": "TEXT"}, primary_key="id")
controller.insert_data("users", {"name": "Alice"})
results = controller.select_data("users", columns=["id", "name"], where={"name": "Alice"})
print("Selected data:", results)
controller.update_data("users", {"name": "Bob"}, where={"name": "Alice"})
controller.delete_data("users", where={"name": "Bob"})

# Start network server in a separate thread
network_thread = threading.Thread(target=controller.start_network_server, daemon=True)
network_thread.start()
time.sleep(1)  # Allow server to start

# Send network data
response = controller.send_network_data(b"Hello, Server!")
print("Network response:", response.decode() if response else "No response")

# Start GUI server
controller.start_gui_server()

# Note: GUI server runs indefinitely; use Ctrl+C to stop
# Cleanup
controller.shutdown()