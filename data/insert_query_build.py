import datetime

from snowman.query import insert
from snowman.query.minify import minify
from your.database.schema import User

now = datetime.datetime.now()

query, params = (
    insert.into(
        User,
    ).values(
        User(
            id=1,
            name="John Doe",
            created_at=now,
        ),
    )
).build()

expected = minify(
    """
    INSERT INTO
        DATABASE.SCHEMA.USER
    (
        ID,
        NAME,
        AGE,
        CREATED_AT
    )
    VALUES (
        %s,
        %s,
        %s,
        %s
    )
    """,
)

assert query == expected
assert params == (1, "John Doe", None, now)
