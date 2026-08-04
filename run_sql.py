import duckdb

con = duckdb.connect("data/finflow.duckdb")

with open("prereqs/sql_warmup.sql", "r") as file:
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

    result = con.execute(query).fetchall()

    if not result:
        print("(no rows returned)")
    else:
        for row in result:
            print(row)

con.close()