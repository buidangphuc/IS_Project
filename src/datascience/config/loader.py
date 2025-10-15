import yaml
from datascience.constants.paths import resolve_paths

def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)

import os

def load_all(config_path=None, params_path=None):
    cfg_path = config_path or os.getenv("APP_CONFIG", "config/config.yaml")
    params_path = params_path or os.getenv("APP_PARAMS", "config/params.yaml")
    cfg = load_yaml(cfg_path)
    params = load_yaml(params_path)
    paths = resolve_paths(cfg)
    return cfg, params, paths
