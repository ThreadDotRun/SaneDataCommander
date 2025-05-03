from Distributor import Distributor
from GUIServer import GUIServer
import multiprocessing

def run_gui_server():
    distributor = Distributor("configs.db")
    gui_server = GUIServer(distributor, "web_interface", "1.0")
    print("Starting GUI server at http://localhost:8000")
    gui_server.start_server()

if __name__ == "__main__":
    process = multiprocessing.Process(target=run_gui_server, daemon=True)
    process.start()
    process.join()  # Optionally join to keep the main process alive