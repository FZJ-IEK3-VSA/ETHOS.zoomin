"""Creates a copy of staged eucalc data in processed_data table"""
from zoomin.db_access import with_db_connection
from zoomin import db_access

# NUTS0 -------------------
# only copying, no aggregation to higher levels
@with_db_connection()
def copy_eucalc_data_into_processed_data(cursor):

    sql_cmd = f"""INSERT INTO processed_data (
                    region_id,
                    var_detail_id,
                    confidence_level_id,
                    proxy_detail_id,
                    pathway,
                    year,
                    value
                )
                SELECT 
                    region_id,
                    var_detail_id,
                    confidence_level_id,
                    proxy_detail_id,
                    pathway,
                    year,
                    value
                FROM 
                    staged_eucalc_data;"""

    cursor.execute(sql_cmd)


copy_eucalc_data_into_processed_data()
