from zoomin.db_access import get_col_values, with_db_connection


@with_db_connection()
def execute_post_disagg_calc(cursor, sql_cmd):
    cursor.execute(sql_cmd)


def perform_post_disagg_calculation(var_name: str):

    sql_cmd = get_col_values(
        "var_details", "calculation_sql_cmd", {"var_name": var_name}
    )
    execute_post_disagg_calc(sql_cmd)
