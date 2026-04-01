from __future__ import annotations

# pyright: reportMissingImports=false

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from local_executor.cli import main


if __name__ == "__main__":
    main()
