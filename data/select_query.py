from snowman.query import select
from your.database.schema import User
from your.snowflake import snowflake_conn

with snowflake_conn.cursor() as cursor:
    user: User | None = (
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
        .execute(cursor)
        .fetchone()
    )
