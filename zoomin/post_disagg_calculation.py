from zoomin.db_access import get_col_values, execute_sql_cmd


def perform_post_disagg_calculation(var_name: str):
    """For a specified variable, fetch the post-disaggregation calculation, stored
    as SQL command in the database, and execute it.

    :param var_name: The name of the variable, for which post-disaggregation calculation is required.
    :type var_name: str
    """
    sql_cmd = get_col_values(
        "var_details", "calculation_sql_cmd", {"var_name": var_name}
    )
    execute_sql_cmd(sql_cmd)
