from pathlib import Path
from typing import Dict, Any

def project_root() -> Path:
    # <project/> root from src/datascience/constants/paths.py
    return Path(__file__).resolve().parents[3]

def resolve_paths(cfg: Dict[str, Any]) -> Dict[str, str]:
    root = project_root()
    return {
        "ARTIFACTS": str(root / cfg["artifacts_dir"]),
        "RAW": str(root / cfg["raw_dir"]),
        "LAKE": str(root / cfg["lake_dir"]),
        "VAL": str(root / cfg["validation_dir"]),
        "TRAINER": str(root / cfg["trainer_dir"]),
        "EVAL": str(root / cfg["eval_dir"]),
    }
