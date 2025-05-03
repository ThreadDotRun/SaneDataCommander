import os
import re
import json
import time
import threading
import logging
import csv
from flask import Flask, request, jsonify
from gunicorn.app.base import BaseApplication
from Distributor import Distributor
from html.parser import HTMLParser
from typing import Dict, List, Optional, Callable

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GUIServer:
    def __init__(self, distributor: Distributor, service_name: str, version: str):
        """Initialize the GUI server with Distributor for configuration.

        Args:
            distributor: Distributor instance for configuration management.
            service_name: Name of the GUI service (e.g., 'web_interface').
            version: Configuration version (e.g., '1.0').
        """
        self.distributor = distributor
        self.service_name = service_name
        self.version = version
        self.app = Flask(__name__)
        self.template_processor = TemplateProcessor("./assets/html_templates")
        self.action_processor = ActionProcessor(distributor, service_name, version)
        self.rate_limits: Dict[str, List[float]] = {}  # client_ip: [timestamps]
        self.rate_limit_lock = threading.Lock()  # Thread-safe lock for rate_limits
        self.max_requests_per_second = 100
        self.template_cache: Dict[str, str] = {}  # Cache for templates and CSS
        self.cache_lock = threading.Lock()  # Thread-safe lock for cache

        # Load configurations from configs.csv
        if not self.distributor.getConfigsFromDelimtedFile("configs.csv"):
            logger.error("Failed to load configurations from configs.csv")
            raise FileNotFoundError("Failed to load configurations from configs.csv")
        if not self.distributor.storeConfigsInSQLite():
            logger.error("Failed to store configurations in SQLite")
            raise ValueError("Failed to store configurations in SQLite")

        self.config = self._load_config()
        self._reload_templates()  # Reload all templates on startup
        self._load_routes()
        logger.debug("Initialized GUIServer for %s:%s", service_name, version)

    def _load_config(self) -> Dict:
        """Load GUI configuration from Distributor."""
        config_json = self.distributor.GetConfigureation("gui", self.service_name, self.version)
        if not config_json:
            logger.error("No configuration found for gui:%s:%s", self.service_name, self.version)
            raise ValueError(f"No configuration found for gui:{self.service_name}:{self.version}")
        config = json.loads(config_json)["settings"]
        logger.debug("Loaded GUI config: %s", config)
        return config

    def _reload_templates(self):
        """Reload all templates and CSS files into memory on server startup."""
        with self.cache_lock:
            self.template_cache.clear()  # Clear existing cache
            template_dir = "./assets/html_templates"
            if not os.path.exists(template_dir):
                logger.error("Template directory %s not found", template_dir)
                raise FileNotFoundError(f"Template directory {template_dir} not found")

            for filename in os.listdir(template_dir):
                if filename.endswith(('.html', '.css')):
                    file_path = os.path.join(template_dir, filename)
                    if os.path.isfile(file_path):
                        with open(file_path, "r") as f:
                            self.template_cache[filename] = f.read()
                            logger.debug("Cached template: %s", filename)

    def _load_routes(self):
        """Define Flask routes for the GUI endpoint."""
        @self.app.route("/gui", methods=["POST"])
        def handle_gui_request():
            return self.handle_request()

        @self.app.route("/client.js")
        def serve_client_js():
            return self._generate_client_js(), 200, {"Content-Type": "application/javascript"}

        @self.app.route("/<path:filename>")
        def serve_static(filename):
            """Serve CSS or other static files from cache."""
            with self.cache_lock:
                if filename in self.template_cache:
                    content_type = "text/css" if filename.endswith(".css") else "text/plain"
                    return self.template_cache[filename], 200, {"Content-Type": content_type}
                logger.warning("Static file not found: %s", filename)
                return jsonify({"error": "File not found"}), 404

        @self.app.route("/")
        def serve_template():
            template_name = self.config.get("template", "default_template.html")
            variables = {"app_name": "SaneDataCommander"}  # Example variable
            functions = {}  # Add server-side functions as needed
            html_content = self.template_processor.process_template(template_name, variables, functions, self.template_cache, self.cache_lock)
            return html_content, 200, {"Content-Type": "text/html"}

    def _rate_limit(self, client_ip: str) -> bool:
        """Enforce rate limiting: max 100 requests/second per client."""
        current_time = time.time()
        with self.rate_limit_lock:
            if client_ip not in self.rate_limits:
                self.rate_limits[client_ip] = []

            # Remove timestamps older than 1 second
            self.rate_limits[client_ip] = [t for t in self.rate_limits[client_ip] if current_time - t < 1]

            # Check if within limit
            if len(self.rate_limits[client_ip]) >= self.max_requests_per_second:
                logger.warning("Rate limit exceeded for client %s", client_ip)
                return False

            self.rate_limits[client_ip].append(current_time)
            return True

    def handle_request(self):
        """Handle POST requests to /gui endpoint."""
        client_ip = request.remote_addr
        if not self._rate_limit(client_ip):
            return jsonify({"error": "Rate limit exceeded"}), 429

        data = request.get_json()
        if not data or "type" not in data or "name" not in data:
            logger.warning("Invalid request received from %s", client_ip)
            return jsonify({"error": "Invalid request"}), 400

        request_type = data["type"]
        name = data["name"]
        input_data = data.get("data")
        logger.debug("Processing request: type=%s, name=%s, data=%s", request_type, name, input_data)

        if request_type in ["variable", "function"]:
            variables = {"app_name": "SaneDataCommander"}  # Example variable
            functions = {}  # Add server-side functions as needed
            result = self.template_processor.process_tag(name, variables, functions)
            return jsonify(["html", result])
        elif request_type == "action":
            result = self.action_processor.process_action(name, input_data)
            return jsonify(result)
        else:
            logger.warning("Invalid request type: %s", request_type)
            return jsonify({"error": "Invalid request type"}), 400

    def _generate_client_js(self) -> str:
        """Generate client-side JavaScript for handling button clicks and text inputs."""
        return """
        async function handleButtonClick(id) {
            try {
                const response = await fetch('/gui', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: 'action', name: id })
                });
                const data = await response.json();
                document.getElementById('result').innerText = data[1];
            } catch (error) {
                console.error('Error:', error);
            }
        }

        async function handleTextInput(id) {
            try {
                const value = document.getElementById(id).value;
                const response = await fetch('/gui', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: 'action', name: id, data: value })
                });
                const data = await response.json();
                document.getElementById('result').innerText = data[1];
            } catch (error) {
                console.error('Error:', error);
            }
        }
        """

    def start_server(self):
        """Start the Gunicorn server."""
        class StandaloneApplication(BaseApplication):
            def __init__(self, app, options=None):
                self.options = options or {}
                self.application = app
                super().__init__()

            def load_config(self):
                for key, value in self.options.items():
                    self.cfg.set(key.lower(), value)

            def load(self):
                return self.application

        options = {
            "bind": f"{self.config.get('host', 'localhost')}:{self.config.get('port', 8000)}",
            "workers": 4,
            "timeout": 30
        }
        logger.info("Starting Gunicorn server on %s", options["bind"])
        StandaloneApplication(self.app, options).run()

class TemplateProcessor:
    def __init__(self, template_dir: str):
        """Initialize with the directory containing HTML templates and CSS."""
        self.template_dir = template_dir
        self.tag_pattern = re.compile(r"\{% ([a-zA-Z_][a-zA-Z0-9_]*) %\}")
        logger.debug("Initialized TemplateProcessor with template_dir=%s", template_dir)

    def load_template(self, template_name: str, cache: Dict[str, str], cache_lock: threading.Lock) -> str:
        """Load a template from cache or disk in a thread-safe manner."""
        with cache_lock:  # Use the passed cache_lock parameter
            if template_name in cache:
                return cache[template_name]

            template_path = os.path.join(self.template_dir, template_name)
            if not os.path.exists(template_path):
                logger.error("Template %s not found", template_name)
                raise FileNotFoundError(f"Template {template_name} not found")

            with open(template_path, "r") as f:
                cache[template_name] = f.read()
                logger.debug("Loaded template %s into cache", template_name)
            return cache[template_name]

    def process_template(self, template_name: str, variables: Dict[str, str], functions: Dict[str, Callable], cache: Dict[str, str], cache_lock: threading.Lock) -> str:
        """Process the template by replacing {% tags %} with variable/function values."""
        template_content = self.load_template(template_name, cache, cache_lock)
        return self._replace_tags(template_content, variables, functions)

    def process_tag(self, tag: str, variables: Dict[str, str], functions: Dict[str, Callable]) -> str:
        """Process a single tag and return its resolved value."""
        if tag in variables:
            return variables[tag]
        elif tag in functions:
            return str(functions[tag]())
        else:
            logger.warning("Tag %s not found in variables or functions", tag)
            return ""

    def _replace_tags(self, content: str, variables: Dict[str, str], functions: Dict[str, Callable]) -> str:
        """Replace all {% tags %} in the content with resolved values."""
        def replace_match(match):
            tag = match.group(1)
            return self.process_tag(tag, variables, functions)

        return self.tag_pattern.sub(replace_match, content)

    def validate_template(self, template_content: str) -> bool:
        """Validate that the template is well-formed HTML."""
        class SimpleHTMLParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.errors = []

            def error(self, message):
                self.errors.append(message)

        parser = SimpleHTMLParser()
        try:
            parser.feed(template_content)
            valid = len(parser.errors) == 0
            if not valid:
                logger.warning("Template validation failed: %s", parser.errors)
            return valid
        except Exception as e:
            logger.error("Template validation error: %s", e)
            return False

class ActionProcessor:
    def __init__(self, distributor: Distributor, service_name: str, version: str):
        """Initialize with Distributor to load action configurations."""
        self.distributor = distributor
        self.service_name = service_name
        self.version = version
        self.actions = {}
        self.ui_action_map = {}  # Maps UI IDs (e.g., textbox1) to action IDs (e.g., uppercase)
        self.action_functions = {
            "upper": lambda data: data.upper() if data else "",
            "reverse": lambda data: data[::-1] if data else "",
        }
        self.load_actions()
        logger.debug("Initialized ActionProcessor for %s:%s", service_name, version)

    def load_actions(self):
        """Load action configurations from gui_action_configs.txt and configs.csv."""
        # Load actions from gui_action_configs.txt
        action_file = "gui_action_configs.txt"
        try:
            with open(action_file, 'r', newline='') as file:
                reader = csv.DictReader(file)
                expected_columns = {'action_id', 'type', 'logic'}
                if not expected_columns.issubset(reader.fieldnames):
                    logger.error("Action config file %s missing required columns: %s", action_file, reader.fieldnames)
                    return

                for row in reader:
                    action_id = row['action_id']
                    action_type = row['type']
                    try:
                        logic = json.loads(row['logic'])
                    except json.JSONDecodeError as e:
                        logger.error("Invalid JSON in logic for action %s: %s", action_id, e)
                        continue

                    self.actions[action_id] = {
                        'type': action_type,
                        'logic': logic
                    }
                    logger.debug("Loaded action from file: %s, type=%s, logic=%s", action_id, action_type, logic)
        except FileNotFoundError:
            logger.warning("Action config file %s not found, relying on configs.csv", action_file)
        except csv.Error as e:
            logger.error("Error reading action config file %s: %s", action_file, e)

        # Load UI-to-action mappings from configs.csv
        config_json = self.distributor.GetConfigureation("gui", self.service_name, self.version)
        if config_json:
            try:
                config = json.loads(config_json)
                config_actions = config.get("settings", {}).get("actions", {})
                for ui_id, action_id in config_actions.items():
                    self.ui_action_map[ui_id] = action_id
                    logger.debug("Mapped UI ID %s to action %s", ui_id, action_id)
            except json.JSONDecodeError as e:
                logger.error("Failed to parse config for UI action mappings: %s", e)

    def process_action(self, action_id: str, input_data: Optional[str] = None) -> List[str]:
        """Process an action based on its ID and optional input data."""
        # Map UI ID to action ID using ui_action_map
        actual_action_id = self.ui_action_map.get(action_id, action_id)
        action = self.actions.get(actual_action_id)
        if not action:
            logger.warning("Unknown action: %s (mapped from %s)", actual_action_id, action_id)
            return ["string", "Unknown action"]

        action_type = action['type']
        logic = action['logic']
        logger.debug("Processing action: %s (mapped from %s), type=%s, input=%s", actual_action_id, action_id, action_type, input_data)

        if action_type == "transform" and input_data:
            func_name = logic.get("function")
            if func_name in self.action_functions:
                try:
                    result = self.action_functions[func_name](input_data)
                    return ["string", result]
                except Exception as e:
                    logger.error("Error executing transform action %s: %s", actual_action_id, e)
                    return ["string", "Action execution failed"]
            else:
                logger.warning("Unknown transform function: %s", func_name)
                return ["string", "Unsupported transform function"]
        elif action_type == "event":
            response_template = logic.get("response", "")
            try:
                result = response_template.format(action_id=action_id)  # Use original action_id for response
                return ["string", result]
            except KeyError as e:
                logger.error("Invalid response template for action %s: %s", actual_action_id, e)
                return ["string", "Invalid action configuration"]
        else:
            logger.warning("Unsupported action type: %s", action_type)
            return ["string", "Unsupported action"]

