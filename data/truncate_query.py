from snowman.query import truncate
from your.database.schema import User
from your.snowflake import snowflake_conn

with snowflake_conn.cursor() as cursor:
    truncate.table.if_.exists(User).execute(cursor)
