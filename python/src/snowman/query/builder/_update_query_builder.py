import itertools
from typing import Any, Callable, Generic, Sequence, Type, cast, overload

from pydantic import BaseModel
from typing_extensions import override

from snowman._cursor import Cursor
from snowman._features import UpdateTag
from snowman.query.builder.condition.condition import Condition
from snowman.query.builder.context.where_context import WhereContext
from snowman.relation import full_table_name
from snowman.relation.table import (
    GenericColumnAccessor,
    GenericInsertColumnTypedDict,
    GenericOrderItemAccessor,
    GenericTable,
    GenericUpdateColumnTypedDict,
    Table,
)

from ._builder import QueryBuilder, QueryWithParams, execute_with_tag


class UpdateStatement(
    Generic[
        GenericTable,
        GenericColumnAccessor,
        GenericOrderItemAccessor,
        GenericInsertColumnTypedDict,
        GenericUpdateColumnTypedDict,
    ]
):
    def __init__(
        self,
        table: Type[
            Table[
                GenericTable,
                GenericColumnAccessor,
                GenericOrderItemAccessor,
                GenericInsertColumnTypedDict,
                GenericUpdateColumnTypedDict,
            ]
        ],
        /,
        *,
        _columns_type: Type[GenericUpdateColumnTypedDict] | None = None,
    ) -> None:
        self._table = table

    def set(
        self,
        fields: Table[
            GenericTable,
            GenericColumnAccessor,
            GenericOrderItemAccessor,
            GenericInsertColumnTypedDict,
            GenericUpdateColumnTypedDict,
        ]
        | GenericUpdateColumnTypedDict,
    ) -> "UpdateSetQueryBuilder[GenericTable, GenericColumnAccessor, GenericOrderItemAccessor, GenericInsertColumnTypedDict, GenericUpdateColumnTypedDict]":
        return UpdateSetQueryBuilder(self._table, fields)


class UpdateSetQueryBuilder(
    Generic[
        GenericTable,
        GenericColumnAccessor,
        GenericOrderItemAccessor,
        GenericInsertColumnTypedDict,
        GenericUpdateColumnTypedDict,
    ]
):
    def __init__(
        self,
        table: type[
            Table[
                GenericTable,
                GenericColumnAccessor,
                GenericOrderItemAccessor,
                GenericInsertColumnTypedDict,
                GenericUpdateColumnTypedDict,
            ]
        ],
        columns: Table[
            GenericTable,
            GenericColumnAccessor,
            GenericOrderItemAccessor,
            GenericInsertColumnTypedDict,
            GenericUpdateColumnTypedDict,
        ]
        | GenericUpdateColumnTypedDict,
    ):
        self._table = table
        self._columns = columns

    @overload
    def where(
        self,
        condition: Callable[[WhereContext[GenericColumnAccessor]], Condition],
        /,
    ) -> "UpdateSetWhereQueryBuidler[GenericTable, GenericColumnAccessor, GenericOrderItemAccessor, GenericInsertColumnTypedDict, GenericUpdateColumnTypedDict]": ...

    @overload
    def where(
        self,
        condition: Condition,
        /,
    ) -> "UpdateSetWhereQueryBuidler[GenericTable, GenericColumnAccessor, GenericOrderItemAccessor, GenericInsertColumnTypedDict, GenericUpdateColumnTypedDict]": ...

    @overload
    def where(
        self,
        condition: str,
        params: Sequence[Any] | None = None,
        /,
    ) -> "UpdateSetWhereQueryBuidler[GenericTable, GenericColumnAccessor, GenericOrderItemAccessor, GenericInsertColumnTypedDict, GenericUpdateColumnTypedDict]": ...

    def where(
        self,
        condition: Callable[[WhereContext[GenericColumnAccessor]], Condition]
        | Condition
        | str,
        params: Sequence[Any] | None = None,
        /,
    ) -> "UpdateSetWhereQueryBuidler[GenericTable, GenericColumnAccessor, GenericOrderItemAccessor, GenericInsertColumnTypedDict, GenericUpdateColumnTypedDict]":
        """
        Specify the condition of the where clause.

        Query parameters only support positional placeholders, so specify them with `%s`.

        e.g)
            - `.where(lambda c: (c(User).id == 1).and_(c(User).name == "Alice"))`
            - `.where((column(User).id == 1).and_(column(User).name == "Alice"))`
            - `.where("id = %s AND name = %s", [1, "Alice"])`
        """
        if callable(condition):
            condition, params = condition(WhereContext(self._table)).to_sql()

        elif isinstance(condition, Condition):
            condition, params = condition.to_sql()

        return UpdateSetWhereQueryBuidler(
            self._table,
            self._columns,
            where_condition=condition,
            where_params=params or (),
        )


class UpdateSetWhereQueryBuidler(
    QueryBuilder[None],
    Generic[
        GenericTable,
        GenericColumnAccessor,
        GenericOrderItemAccessor,
        GenericInsertColumnTypedDict,
        GenericUpdateColumnTypedDict,
    ],
):
    def __init__(
        self,
        table: type[
            Table[
                GenericTable,
                GenericColumnAccessor,
                GenericOrderItemAccessor,
                GenericInsertColumnTypedDict,
                GenericUpdateColumnTypedDict,
            ]
        ],
        columns: Table[
            GenericTable,
            GenericColumnAccessor,
            GenericOrderItemAccessor,
            GenericInsertColumnTypedDict,
            GenericUpdateColumnTypedDict,
        ]
        | GenericUpdateColumnTypedDict,
        *,
        where_condition: str,
        where_params: Sequence[Any],
    ):
        self._table = table
        self._columns = cast(
            dict,
            columns.model_dump(exclude_unset=True, by_alias=True)
            if isinstance(columns, BaseModel)
            else {
                (
                    cast(str, table.model_fields[k].alias)
                    if table.model_fields[k].alias is not None
                    else k
                ): v
                for k, v in columns.items()
            },
        )
        self._where_condition = where_condition
        self._where_params = where_params

    @override
    def build(self) -> QueryWithParams:
        values = ", ".join([f"{key} = %s" for key in self._columns.keys()])

        query = f"UPDATE {full_table_name(self._table)} SET {values} WHERE {self._where_condition}".strip()

        return QueryWithParams(
            query,
            tuple(itertools.chain(self._columns.values(), self._where_params)),
        )

    @override
    def execute(self, cursor: Cursor) -> None:
        query, params = self.build()

        execute_with_tag(
            UpdateTag[self._table],  # type: ignore
            cursor,
            query,
            params,
        )
