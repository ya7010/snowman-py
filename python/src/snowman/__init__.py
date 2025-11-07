"""
# ⛄ [Snowman](https://github.com/ya7010/snowman-py) ⛄

Python model and query builder for [Snowflake](https://www.snowflake.com/).
"""

import importlib
import importlib.metadata

from . import datatype as datatype
from . import pydantic as pydantic
from . import query as query
from .query.column import Column as Column
from .query.order_item import ColumnOrderItem as ColumnOrderItem
from .relation.table import Table as Table
from .relation.table import table as table
from .relation.view import View as View
from .relation.view import view as view

__version__ = importlib.metadata.version("snowman-py")
