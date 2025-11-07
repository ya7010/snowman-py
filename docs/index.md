---
hide:
    - toc
    - navigation
---

<div style="text-align: center">
    <img alt="logo" src="https://raw.githubusercontent.com/ya7010/snowman-py/main/docs/images/logo.svg" width="300" />
    <h1><strong>Snowman:</strong> Pydantic Model Generator and Query Builder for
    <a href="https://www.snowflake.com/">Snowflake</a></h1>
</div>

--8<-- "README.md:badges"

Snowman is a Python library that provides a way to extract Snowflake table information in a type-safe.

Snowman provides two main features:

* Automatically generate Pydantic models from Snowflake information schemas
* Query builder to generate SQL queries in a type-safe

???+ note "Generated Pydantic Model"
    #### Source: Snowflake
    ```sql
    --8<-- "docs/data/your/database/schema.sql"
    ```

    #### Output: Python Code
    ```python
    --8<-- "docs/data/your/database/schema.py"
    ```

???+ note "Query Builder"
    === "SELECT"
        🚧 **This is a draft.** 🚧 

        It does not support complex queries that include only specific fields or table joins.

        !!! example
            ```python
            --8<-- "docs/data/select_query.py"
            ```

    === "INSERT"
        !!! example
            ```python
            --8<-- "docs/data/insert_query.py"
            ```

    === "UPDATE"
        !!! example
            ```python
            --8<-- "docs/data/update_query.py"
            ```

    === "DELETE"
        !!! example
            ```python
            --8<-- "docs/data/delete_query.py"
            ```

    === "TRUNCATE"
        !!! example
            ```python
            --8<-- "docs/data/truncate_query.py"
            ```
