import duckdb

con = duckdb.connect(
    "database/crypto.duckdb"
)

df = con.execute("""
SELECT *
FROM trades
LIMIT 70
""").df()

print(df)