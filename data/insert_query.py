from snowman.query import insert
from your.database.schema import User
from your.snowflake import snowflake_conn

with snowflake_conn.cursor() as cursor:
    insert.into(User).values(
        User(
            id=1,
            name="John Doe",
        ),
    ).execute(cursor)
