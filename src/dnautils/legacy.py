from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Optional

_LEGACY_MODULE: Optional[ModuleType] = None


def _default_legacy_path() -> Path:
    # dna-utils/src/dnautils/legacy.py -> repo root
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / "scripts" / "SQLtest" / "Common_Functions.py"


def load_legacy_module() -> ModuleType:
    global _LEGACY_MODULE

    if _LEGACY_MODULE is not None:
        return _LEGACY_MODULE

    module_path = Path(
        os.getenv("DNAUTILS_LEGACY_PATH")
        or os.getenv("COMMON_FUNCTIONS_PATH")
        or str(_default_legacy_path())
    )
    if not module_path.exists():
        raise FileNotFoundError(
            f"Could not find legacy module at {module_path}. "
            "Set DNAUTILS_LEGACY_PATH (or COMMON_FUNCTIONS_PATH) to the correct file location."
        )

    spec = importlib.util.spec_from_file_location("legacy_common_functions", str(module_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module spec from {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _LEGACY_MODULE = module
    return module


def call(name: str, *args, **kwargs):
    module = load_legacy_module()
    fn = getattr(module, name)
    return fn(*args, **kwargs)
