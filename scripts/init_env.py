"""Create .env from .env.example, replacing every `change-me` with a random secret.

Usage: python3 scripts/init_env.py [--force]
"""

from __future__ import annotations

import base64
import os
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / ".env.example"
TARGET = ROOT / ".env"
PLACEHOLDER = "change-me"


def generate(key: str) -> str:
    if key == "AIRFLOW_FERNET_KEY":
        # Fernet requires a url-safe base64-encoded 32-byte key.
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    return secrets.token_urlsafe(24)


def main(argv: list[str]) -> int:
    if TARGET.exists() and "--force" not in argv:
        print(".env already exists; use --force to regenerate (this changes all passwords).")
        return 0

    lines = []
    for line in EXAMPLE.read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and not line.lstrip().startswith("#") and value.strip() == PLACEHOLDER:
            line = f"{key}={generate(key.strip())}"
        lines.append(line)

    TARGET.write_text("\n".join(lines) + "\n")
    TARGET.chmod(0o600)
    print(f"wrote {TARGET.name} with generated secrets")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
