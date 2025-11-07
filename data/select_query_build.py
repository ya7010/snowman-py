from snowman.query import select
from snowman.query.minify import minify
from your.database.schema import User

query, params = (
    select()
    .from_(User)
    .where(
        lambda c: (c.self.id == 1)
        .and_(c.self.name == "John Doe")
        .and_(c.self.age >= 18),
    )
    .order.by(
        lambda c: [
            c.self.id.desc,
            c.self.name.asc.nulls.last,
        ],
    )
    .limit(1)
    .offset(0)
).build()

expected = minify(
    """
    SELECT
        *
    FROM
        DATABASE.SCHEMA.USER
    WHERE
        ID = %s
        AND NAME = %s
        AND AGE >= %s
    ORDER BY
        ID DESC,
        NAME ASC NULLS LAST
    LIMIT %s
    OFFSET %s
    """,
)

assert query == expected
assert params == (1, "John Doe", 18, 1, 0)
