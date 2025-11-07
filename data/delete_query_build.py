from snowman.query import delete
from snowman.query.minify import minify
from your.database.schema import User

query, params = (
    delete.from_(
        User,
    ).where(
        lambda c: c.self.id == 1,
    )
).build()

expected = minify(
    """
    DELETE FROM
        DATABASE.SCHEMA.USER
    WHERE
        ID = %s
    """,
)

assert query == expected
assert params == (1,)
