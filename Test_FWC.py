from FrameworkController import FrameworkController

def main():
    # Initialize the controller
    controller = FrameworkController()

    # Load configurations
    if not controller.load_configs():
        print("Failed to load configurations")
        return

    # Initialize database
    if not controller.initialize_database():
        print("Failed to initialize database")
        return

    # Create a table
    columns = {"id": "INTEGER PRIMARY KEY AUTOINCREMENT", "name": "TEXT"}
    if controller.create_table("users", columns, primary_key="id"):
        print("Created table 'users'")

    # Insert data
    if controller.insert_data("users", {"name": "Alice"}):
        print("Inserted data: Alice")

    # Select data
    result = controller.select_data("users", columns=["id", "name"], where={"name": "Alice"})
    if result:
        print(f"Selected data: {result}")

    # Initialize network
    if not controller.initialize_network():
        print("Failed to initialize network")
        return

    # Start network server
    if controller.start_network_server():
        print("Started network server")

    # Send network data
    response = controller.send_network_data(b"Hello, Server!")
    if response:
        print(f"Network response: {response.decode()}")

    # Initialize GUI
    if not controller.initialize_gui():
        print("Failed to initialize GUI")
        return

    # Start GUI server
    if controller.start_gui_server():
        print("Started GUI server at http://localhost:8000")

    # Keep the main thread alive to allow server threads to run
    try:
        while True:
            pass
    except KeyboardInterrupt:
        controller.shutdown()
        print("Shutdown complete")

if __name__ == "__main__":
    main()