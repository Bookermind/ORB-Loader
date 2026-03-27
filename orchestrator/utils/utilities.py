import hashlib
from pathlib import Path

def generate_hash(file: Path | str) -> bytes:
    """
    Generates a sha256 hash for a given file (passed as input)
    """
    sha256 = hashlib.sha256()
    with open(file, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.digest()