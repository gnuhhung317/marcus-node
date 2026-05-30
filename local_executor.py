from __future__ import annotations

# pyright: reportMissingImports=false

import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parent / "src"
_PACKAGE_ROOT = _SRC_ROOT / "local_executor"

sys.path.insert(0, str(_SRC_ROOT))

# Let this wrapper behave like a package module so imports such as
# `local_executor.local_store` keep working when the file is imported first.
__path__ = [str(_PACKAGE_ROOT)]

from local_executor.cli import main


if __name__ == "__main__":
    main()
