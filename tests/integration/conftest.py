import os
import subprocess
from pathlib import Path

import pytest

os.environ["DB_NAME"] = "payments_test"

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    db_name = os.getenv("DB_NAME", "payments_test")

    if db_name != "payments_test":
        raise RuntimeError(
            "Integration tests must use payments_test"
        )

    # Reset only the dedicated integration-test database.
    subprocess.run(
        [
            "docker", "exec", "-i",
            "postgres", "psql",
            "-v", "ON_ERROR_STOP=1",
            "-U", "admin",
            "-d", db_name,
            "-c", "DROP SCHEMA public CASCADE; CREATE SCHEMA public;",
        ],
        check=True,
    )

    for sql_file in [
        "db/schema.sql",
        "db/ledger_schema.sql",
        "db/ledger_functions.sql",
        "db/ledger_permissions.sql",
    ]:
        with open(PROJECT_ROOT / sql_file) as f:
            subprocess.run(
                [
                    "docker", "exec", "-i",
                    "postgres", "psql", 
                    "-v", "ON_ERROR_STOP=1",
                    "-U", "admin",
                    "-d", db_name,
                ],
                stdin=f,
                check=True,
            )


@pytest.fixture(scope="session", autouse=True)
def verify_test_database(setup_test_database):
    from db.connection import get_connection

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]

        if db_name != "payments_test":
            raise RuntimeError(
                f"Unsafe test database connection: {db_name}"
            )
    finally:
        conn.close()