from Distributor import Distributor
from GUIServer import GUIServer

if __name__ == "__main__":
    distributor = Distributor("configs.db")
    gui_server = GUIServer(distributor, "web_interface", "1.0")
    gui_server.start_server()