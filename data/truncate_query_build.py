from snowman.query import truncate
from your.database.schema import User

query, params = truncate.table.if_.exists(User).build()

expected = "TRUNCATE TABLE IF EXISTS DATABASE.SCHEMA.USER"

assert query == expected
assert params == ()
