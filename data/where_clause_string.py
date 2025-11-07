import datetime

from snowman.query import update
from your.database.schema import User
from your.snowflake import snowflake_conn

with snowflake_conn.cursor() as cursor:
    update(
        User,
    ).set(
        {"name": "Jane Doe"},
    ).where(
        """
        NAME IN (%s)
        AND (
            AGE IS NOT NULL
            AND AGE >= %s
            AND AGE <= %s
        )
        AND CREATED_AT >= %s
        """,
        (["Jane", "Doe"], 18, 100, datetime.datetime(2001, 1, 1)),
    ).execute(cursor)
