import duckdb

con = duckdb.connect(
    "database/crypto.duckdb"
)

result = con.execute(
    "SELECT COUNT(*) FROM trades"
).fetchall()

print(result)