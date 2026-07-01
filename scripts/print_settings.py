"""Print the current platform settings with secrets masked."""

from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from apex_lakehouse.config import load_settings


def main() -> None:
    print(json.dumps(load_settings().public_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
