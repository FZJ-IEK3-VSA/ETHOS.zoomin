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

db_country = os.environ.get("DB_COUNTRY")
db_version = os.environ.get("DB_VERSION")

db_name = f"{db_country.lower()}_v{db_version}"
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
    """
    Return Database URI.

    :returns: db_uri
    :rtype: str
    """
    db_uri = f"postgresql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
    return db_uri


ENGINE = create_engine(
    get_db_uri(),
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
    future=True,
)


@with_db_connection()
def get_values(cursor, sql_cmd):
    """
    Execute the passed SQL command and return the result.

    :param sql_cmd: SQL command
    :type sql_cmd: str

    :returns: db_output
    :rtype: str/float/int/list
    """
    cursor.execute(sql_cmd)
    result = cursor.fetchall()

    db_output = [res[0] for res in result]

    if len(db_output) == 1:
        return db_output[0]
    else:
        return db_output


@with_db_connection()
def execute_sql_cmd(cursor: Any, sql_cmd: str):
    """
    Execute the passed SQL command

    :param sql_cmd: SQL command
    :type sql_cmd: str
    """
    cursor.execute(sql_cmd)


@with_db_connection()
def get_col_values(
    cursor: Any, table: str, col: str, cols_criteria: Optional[dict] = None
) -> Any:
    """
    Return all unique values in a tables' column, corresponding to values in other column(s).

    :param table: Name of the table from which to fetch the values
    :type table: str

    :param col: Name of the column from which to fetch the values
    :type col: str

    **Default arguments:**

    :param cols_criteria: If it is required to filter on values in other column(s),
    then the criteria must be passed here

    * Ex.: {"var_detail_id": 1186, "region_id": 89},

        |br| * the default value is None

    :type cols_criteria: dict

    :returns: out_result
    :rtype: str/float/int/list
    """
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
    out_result = [res[0] for res in result]
    if len(np.unique(out_result)) == 1:
        return out_result[0]

    return out_result


def get_primary_key(table: str, cols_criteria: dict) -> Any:
    """
    Return primary key corresponding to other column values in a table.

    :param table: Name of the table from which to fetch the values
    :type table: str

    :param cols_criteria: Values in other columns to filter on

    * Ex.: {"var_name": "population", "region_id": 89}

    :type cols_criteria: dict

    :returns: col_val
    :rtype: int
    """
    col_val = get_col_values(table, "id", cols_criteria)

    if not isinstance(col_val, int):
        raise ValueError("many primary keys returned.")
    return col_val


def get_table(sql_cmd: str) -> pd.DataFrame:
    """
    Return a table as dataframe based on the passed SQL command.

    :param sql_cmd: SQL command
    :type sql_cmd: str

    :returns: table_df
    :rtype: pd.DataFrame
    """
    with ENGINE.begin() as conn:
        sql_iterator = pd.read_sql_query(sql=sql_cmd, con=conn, chunksize=5)

        chunks = []
        for chunk in sql_iterator:
            chunks.append(chunk)

        # Concatenate all processed chunks into a single DataFrame
        table_df = pd.concat(chunks, ignore_index=True)

        return table_df


def get_regions(resolution: str) -> pd.DataFrame:
    """
    Return region codes and their primary keys corresponding to a specified
    sptatial resolution from the database.

    :param resolution: Desired spatial resolution
    :type resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :returns: regions_df
    :rtype: pd.DataFrame
    """
    # Construct sql command
    sql_cmd = f"SELECT id, region_code FROM regions WHERE resolution='{resolution}'"

    # get table
    regions_df = get_table(sql_cmd=sql_cmd)

    return regions_df


def get_proxy_data(var_name: str, spatial_resolution: str) -> pd.DataFrame:
    """
    Return data that is to be used as a spatial proxy during disaggregation,
    at a specified spatial resolution.

    :param var_name: The name of the proxy, strictly as per the "variables_with_details_and_tags.xlsx"
    :type var_name: str

    :param spatial_resolution: Desired spatial resolution
    :type spatial_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :returns: data_df
    :rtype: pd.DataFrame
    """
    try:
        # if the proxy is climate data, then values correponding to RCP4.5 and for year 2025 are returned
        if var_name.startswith("cproj_"):
            sql_cmd = f"""SELECT d.region_id, r.region_code, d.value, d.year, d.confidence_level_id 
                            FROM processed_data d
                            JOIN regions r ON d.region_id = r.id
                            WHERE d.var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}') AND 
                                d.year=2025 AND 
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
    """
    Insert a large chunk of data into the `processed_data` table, in the database.

    :param db_ready_df: data table with all the column names exactly as in the `processed_data` table
    :type db_ready_df: pd.DataFrame
    """
    # for big datasets make chunks and insert each chunk
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
        with ENGINE.begin() as conn:
            db_ready_df.to_sql(
                "processed_data",
                conn,
                index=False,
                if_exists="append",
                method=_psql_insert_copy,
            )
