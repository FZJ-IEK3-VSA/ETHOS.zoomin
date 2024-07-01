import pytest
from zoomin.db_access import get_table


def test_random_forest_based_disagg():
    sql_cmd = f"""SELECT value FROM processed_data 
                    WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'gross_value_added_nace_sector_c') 
                    AND region_id IN (SELECT id FROM regions WHERE resolution='NUTS0');"""

    nuts0_df = get_table(sql_cmd)

    sql_cmd = f"""SELECT value FROM processed_data 
                    WHERE var_detail_id in (SELECT id FROM var_details WHERE var_name = 'gross_value_added_nace_sector_c') 
                    AND region_id IN (SELECT id FROM regions WHERE resolution='LAU');"""

    lau_df = get_table(sql_cmd)

    assert nuts0_df.value.item() == lau_df.value.sum()
