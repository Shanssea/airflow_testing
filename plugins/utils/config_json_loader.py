import os
import json
from pathlib import Path

class ConfigJSONLoader:
    """Helper class to get config variables from JSON file
    """
    def __init__(self, dag_path: str):
        self.env = os.getenv("ENVIRONMENT", "dev")
        base_dir = os.getenv("AIRFLOW_CONFIG_DIR")
        if not base_dir:
            base_dir = Path(dag_path).resolve().parent / "config"
        self.config_path = Path(base_dir) / f"config_{self.env}.json"
        self.config = self._load_config()
        
        self.dag = self.config.get("dag", {})
        self.postgres = self.config.get("postgres", {})
        self.email = self.config.get("email", {})
        self.sql = self.config.get("sql", {})

    def _load_config(self) -> dict:
        import json
        with open(self.config_path, 'r') as f:
            return json.load(f)
        
    