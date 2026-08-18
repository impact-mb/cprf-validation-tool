"""Generate a secure password hash for CPRF Validation Tool v2026.01.

Run locally:
    python generate_password_hash.py

The script never saves or prints the original password. It prints only the
salted PBKDF2-SHA256 hash that should be copied into Streamlit Secrets as
APP_PASSWORD_HASH.
"""

from __future__ import annotations

import getpass
import hashlib
import secrets

HASH_SCHEME = "pbkdf2_sha256"
DEFAULT_ITERATIONS = 600_000
SALT_BYTES = 16


def make_password_hash(password: str, iterations: int = DEFAULT_ITERATIONS) -> str:
    """Return a salted PBKDF2-SHA256 password hash string."""
    if not password:
        raise ValueError("Password cannot be empty.")
    if iterations < 100_000:
        raise ValueError("PBKDF2 iterations must be at least 100,000.")

    salt = secrets.token_bytes(SALT_BYTES)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return f"{HASH_SCHEME}${iterations}${salt.hex()}${digest.hex()}"


def main() -> None:
    print("CPRF Validation Tool v2026.01 - Password Hash Generator")
    print("Magic Bus Impact Team")
    print("\nYour password will be hidden while typing and will NOT be saved.\n")

    password = getpass.getpass("Enter new app password: ")
    confirm_password = getpass.getpass("Confirm password: ")

    if password != confirm_password:
        raise SystemExit("ERROR: Passwords do not match. Nothing was generated.")

    if len(password) < 10:
        raise SystemExit("ERROR: Please use a password with at least 10 characters.")

    password_hash = make_password_hash(password)

    print("\nPassword hash generated successfully.\n")
    print("Copy ONLY the following line into Streamlit Secrets:\n")
    print(f'APP_PASSWORD_HASH = "{password_hash}"')
    print("\nDo not add the original password to GitHub or Streamlit Secrets.")


if __name__ == "__main__":
    main()
