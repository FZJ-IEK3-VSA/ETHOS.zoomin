import pytest
import numpy as np
from zoomin.db_access import with_db_connection, get_table


@with_db_connection()
def get_values(cursor, sql_cmd):
    cursor.execute(sql_cmd)
    result = cursor.fetchall()
    return result[0][0]


def test_categorical():
    output_values = get_values(
        "SELECT value FROM processed_data WHERE var_detail_id IN (SELECT id FROM var_details WHERE var_unit='(categorical)')"
    )
    allowed_values = {1, 2, 0, -1, -5, -10}
    assert set(output_values).issubset(allowed_values)
