import datetime

from snowman.query import update
from snowman.query.minify import minify
from your.database.schema import User

query, params = (
    update(
        User,
    )
    .set(
        {"name": "Jane Doe"},
    )
    .where(
        lambda c: (c.self.name.in_(["Jane", "Doe"]))
        .and_(c.self.age > 18)
        .and_(c.self.created_at >= datetime.datetime(2001, 1, 1)),
    )
).build()

expected = minify(
    """
    UPDATE
        DATABASE.SCHEMA.USER
    SET
        NAME = %s
    WHERE
        NAME IN (%s)
        AND AGE > %s
        AND CREATED_AT >= %s
    """,
)

assert query == expected
assert params == ("Jane Doe", ["Jane", "Doe"], 18, datetime.datetime(2001, 1, 1))
