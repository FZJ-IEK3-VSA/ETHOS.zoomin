import os
from typing import Any, Optional, Callable, Iterable
import csv
from io import StringIO
from functools import wraps
from psycopg2 import pool
import numpy as np
import pandas as pd
import dask.dataframe as dd
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_pwd = os.environ.get("DB_PASSWORD")
db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")

# Initialize the connection pool
db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    user=db_user,
    password=db_pwd,
    host=db_host,
    port=db_port,
    database=db_name,
)


def with_db_connection() -> Any:
    """Wrap a set up-tear down Postgres connection while providing a cursor object to make queries with."""

    def wrap(func_call: Callable) -> Any:
        @wraps(func_call)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                # Get connection from the pool
                connection = db_pool.getconn()
                if connection is None:
                    raise Exception("Failed to get DB connection from the pool")

                with connection:
                    with connection.cursor() as cursor:
                        return_val = func_call(cursor, *args, **kwargs)

                # Return the connection to the pool
                db_pool.putconn(connection)

                # return value
                return return_val

            except Exception as error:
                # Return the connection to the pool in case of error
                if connection is not None:
                    db_pool.putconn(connection, close=True)

                # Log more details about the error
                print(
                    f"Attempting to connect to the database for function {func_call.__name__} with args {args} and kwargs {kwargs}"
                )
                raise error

        return wrapper

    return wrap


def get_db_uri() -> str:
    """Return db uri."""
    return f"postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"


def get_db_engine() -> Any:
    """Set up database connection engine."""
    db_uri = get_db_uri()
    engine = create_engine(db_uri, pool_pre_ping=True)

    return engine


@with_db_connection()
def get_values(cursor, sql_cmd):
    cursor.execute(sql_cmd)
    result = cursor.fetchall()

    result_list = [res[0] for res in result]

    if len(result_list) == 1:
        return result_list[0]
    else:
        return result_list


@with_db_connection()
def execute_sql_cmd(cursor: Any, sql_cmd) -> Any:
    """Execute any sql_cmd passed"""

    cursor.execute(sql_cmd)


@with_db_connection()
def get_col_values(
    cursor: Any, table: str, col: str, cols_criteria: Optional[dict] = None
) -> Any:
    """Return all `col` values or a subset corresponding to other column values in a table."""
    sql_cmd = f"SELECT {col} FROM {table}"

    if cols_criteria is not None:
        where_clause = " AND ".join(
            [
                f"{key}='{val}'"
                if isinstance(val, str)
                else f"{key}=NULL"
                if val == None
                else f"{key}={val}"
                for (key, val) in cols_criteria.items()
            ]
        )

        sql_cmd = f"{sql_cmd} WHERE {where_clause}"

    cursor.execute(sql_cmd)

    result = cursor.fetchall()

    if result == []:
        raise ValueError(f"The value/values do not exist in the DB")

    # return a list of values if there is more than 1 unique
    # value, else just the unique value
    result_list = [res[0] for res in result]
    if len(np.unique(result_list)) == 1:
        return result_list[0]

    return result_list


def get_primary_key(table: str, cols_criteria: dict) -> Any:
    """Return primary key/keys corresponding to other column values in a table."""
    col_vals = get_col_values(table, "id", cols_criteria)

    if not isinstance(col_vals, int):
        raise ValueError("many primary keys returned.")
    return col_vals


@with_db_connection()
def get_table(cursor: Any, sql_cmd: str) -> pd.DataFrame:
    """Return a table as dataframe based on the sql_cmd."""
    engine = get_db_engine()
    engine_conn = engine.connect()

    sql_iterator = pd.read_sql_query(sql=sql_cmd, con=engine_conn, chunksize=5)

    chunks = []
    for chunk in sql_iterator:
        chunks.append(chunk)

    # Concatenate all processed chunks into a single DataFrame
    table_df = pd.concat(chunks, ignore_index=True)

    return table_df


@with_db_connection()
def get_regions(cursor: Any, resolution: str) -> pd.DataFrame:
    """Return dataframe of region codes and their primary keys corresponding to the specified resolution from the DB."""
    # Construct sql command
    sql_cmd = f"SELECT id, region_code FROM regions WHERE resolution='{resolution}'"

    # get table
    regions_df = get_table(sql_cmd=sql_cmd)

    return regions_df


@with_db_connection()
def get_proxy_data(cursor: Any, var_name: str, spatial_resolution) -> pd.DataFrame:
    """Return dataframe from processed_data table at specified resolution."""  # TODO: update docstring

    try:
        if var_name.startswith("cproj_"):
            sql_cmd = f"""SELECT d.region_id, r.region_code, d.value, d.year, d.confidence_level_id 
                            FROM processed_data d
                            JOIN regions r ON d.region_id = r.id
                            WHERE d.var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}') AND 
                                d.year=2020 AND 
                                d.climate_experiment='RCP4.5' AND 
                                d.region_id IN (SELECT id FROM regions WHERE resolution = '{spatial_resolution}');"""
        else:
            sql_cmd = f"""SELECT d.region_id, r.region_code, d.value, d.year, d.confidence_level_id 
                        FROM processed_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE d.var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}') AND 
                             d.region_id IN (SELECT id FROM regions WHERE resolution = '{spatial_resolution}');"""

        data_df = get_table(sql_cmd)

    except:
        raise ValueError(f"{var_name} not found. Check your proxy equation")

    return data_df


def _psql_insert_copy(table: Any, conn: Any, keys: list, data_iter: Iterable) -> None:
    """Execute SQL statement inserting data.

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
        Database table
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        Database connection
    keys : list of str
        Column names
    data_iter : Iterable
        Iterable that iterates the values to be inserted

    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(keys)
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)


def add_to_processed_data(db_ready_df: pd.DataFrame) -> None:
    """Add the data to processed_data table."""
    if len(db_ready_df) > 10000:
        db_uri = get_db_uri()

        ddf = dd.from_pandas(db_ready_df, npartitions=10)

        ddf.to_sql(
            name="processed_data",
            uri=db_uri,
            index=False,
            if_exists="append",
            parallel=True,
        )

    # for smaller datasets make a normal entry
    else:
        engine = get_db_engine()
        db_ready_df.to_sql(
            "processed_data",
            engine,
            index=False,
            if_exists="append",
            method=_psql_insert_copy,
        )
