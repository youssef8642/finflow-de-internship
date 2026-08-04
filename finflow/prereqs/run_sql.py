from pathlib import Path
import sys

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from finflow.config.logger import get_logger


logger = get_logger(__name__)

db_path = PROJECT_ROOT / "data" / "finflow.duckdb"
sql_path = Path(__file__).resolve().parent / "sql_warmup.sql"
prereqs_db_path = Path(__file__).resolve().parent / "data" / "finflow.duckdb"

if not db_path.exists() and prereqs_db_path.exists():
    db_path = prereqs_db_path

logger.info("Opening DuckDB database at %s", db_path)
con = duckdb.connect(str(db_path))

logger.info("Loading SQL warmup script from %s", sql_path)
with open(sql_path, "r", encoding="utf-8") as file:
    sql = file.read()

queries = sql.split(";")
query_number = 0

for i, query in enumerate(queries, 1):
    query = query.strip()
    query_lines = [line for line in query.splitlines() if not line.strip().startswith("--")]
    query = "\n".join(query_lines).strip()

    if not query:
        continue

    query_number += 1

    print(f"\n===== QUERY {query_number} =====")
    logger.debug("Executing warmup query %s", query_number)

    result = con.execute(query).fetchall()

    if not result:
        print("(no rows returned)")
    else:
        for row in result:
            print(row)

logger.info("Completed %s warmup queries", query_number)
con.close()