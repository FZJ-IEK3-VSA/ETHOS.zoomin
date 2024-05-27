import os
import time
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
def get_processed_lau_data(cursor: Any, var_name: str) -> pd.DataFrame:
    """Return dataframe from processed_data table at LAU level."""  # TODO: update docstring

    try:
        _fk_var_id = get_primary_key("var_details", {"var_name": var_name})

        if var_name.startswith("cproj_"):
            climate_experiment_id = get_primary_key(
                "climate_experiments", {"climate_experiment": "RCP2.6"}
            )
            sql_cmd = f"SELECT region_id, value, year, quality_rating_id FROM processed_data \
                        WHERE var_detail_id={_fk_var_id} AND \
                        year=2020 AND \
                        climate_experiment_id={climate_experiment_id}"
        else:
            sql_cmd = f"SELECT region_id, value, year, quality_rating_id FROM processed_data \
                        WHERE var_detail_id={_fk_var_id}"

        data_df = get_table(sql_cmd)

    except:
        raise ValueError(f"{var_name} not found. Check your proxy equation")

    regions_df = get_regions("LAU")

    final_df = pd.merge(
        data_df, regions_df, left_on="region_id", right_on="id", how="inner"
    )
    final_df.drop(columns=["id"], inplace=True)

    return final_df


@with_db_connection()
def get_data_for_calculations(
    cursor: Any,
    var_name: str,
    calc_var_type: str,
    pathway_name: Optional[int] = None,
) -> pd.DataFrame:
    """Return dataframe from processedd_data table."""

    var_detail_id = get_primary_key("var_details", {"var_name": var_name})

    if calc_var_type == "soi":
        sql_cmd = f"SELECT region_id, pathway_id, year, value, quality_rating_id FROM processed_data \
                WHERE var_detail_id={var_detail_id}"

        data_df = get_table(sql_cmd)

        if "eucalc_" not in var_name:  # -> year and pathway_id are ignored
            data_df.drop(columns=["pathway_id", "year"], inplace=True)
        else:
            # for eucalc value, only the first year is considered #TODO: for eucalc value, you need any one pathway and year = 2020. For this year, the value should be the same no matter which pathway.
            # dont drop year. Use it the same way as you do for collected_vars to assign quality rating
            first_year = data_df["year"].values.min()
            data_df = data_df[data_df["year"] == first_year]

            data_df.drop(columns=["year"], inplace=True)

    elif calc_var_type == "eucalc_var":
        # NOTE: helps to deploy for different countries individually
        if pathway_name is None:
            sql_cmd = f"SELECT region_id, pathway_id, year, value, quality_rating_id FROM processed_data \
                    WHERE var_detail_id={var_detail_id}"
        else:
            pathway_id = get_primary_key(
                "pathways", {"pathway_file_name": pathway_name}
            )
            sql_cmd = f"SELECT region_id, pathway_id, year, value, quality_rating_id FROM processed_data \
                    WHERE var_detail_id={var_detail_id} AND pathway_id={pathway_id}"

        data_df = get_table(sql_cmd)

    else:
        sql_cmd = f"SELECT region_id, value, quality_rating_id FROM processed_data \
                WHERE var_detail_id={var_detail_id}"
        data_df = get_table(sql_cmd)

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
