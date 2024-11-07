"""Creates a copy of staged collected data in processed_data table and aggregates the data to higher spatial levels"""
from zoomin.db_access import with_db_connection
from zoomin import db_access

# NUTS0 -------------------
# only copying, no aggregation to higher levels
@with_db_connection()
def copy_collected_data_into_processed_data(cursor):

    sql_cmd = f"""INSERT INTO processed_data (
                    region_id,
                    var_detail_id,
                    confidence_level_id,
                    proxy_detail_id,
                    year,
                    value
                )
                SELECT 
                    region_id,
                    var_detail_id,
                    confidence_level_id,
                    proxy_detail_id,
                    year,
                    value
                FROM 
                    staged_collected_data;"""

    cursor.execute(sql_cmd)


copy_collected_data_into_processed_data()

# ---------------------------------
# number of chars to consider based on a resolution
char_dict = {"NUTS3": 5, "NUTS2": 4, "NUTS1": 3, "NUTS0": 2}


def aggregate_collected_var(cursor, var_detail_id, agg_mode, spatial_resolution):

    if spatial_resolution == "NUTS2":
        agg_spatial_levels = ["NUTS1", "NUTS0"]

    elif spatial_resolution == "NUTS3":
        agg_spatial_levels = ["NUTS2", "NUTS1", "NUTS0"]

    elif spatial_resolution == "LAU":
        agg_spatial_levels = ["NUTS3", "NUTS2", "NUTS1", "NUTS0"]

    for agg_spatial_level in agg_spatial_levels:
        n_char_to_keep = char_dict[agg_spatial_level]

        # NOTE:for variables with mixed years, the oldest year is considered during aggregation
        sql_cmd = f"""
        INSERT INTO processed_data (region_id, var_detail_id, confidence_level_id, proxy_detail_id, year, value)
        SELECT 
            r_agg.id,
            scd.var_detail_id,
            MIN(scd.confidence_level_id),
            scd.proxy_detail_id,
            MIN(scd.year), 
            ROUND(CAST({agg_mode}(scd.value) AS numeric), 5)
        FROM 
            staged_collected_data AS scd
        JOIN 
            regions AS r_org ON scd.region_id = r_org.id
        JOIN 
            regions AS r_agg ON LEFT(r_org.region_code, {n_char_to_keep}) = r_agg.region_code
        WHERE 
            scd.var_detail_id = {var_detail_id}
        GROUP BY 
            r_agg.id,
            scd.var_detail_id,
            scd.proxy_detail_id;
        """

        cursor.execute(sql_cmd)


@with_db_connection()
def aggregate_collected_data(cursor):
    for spatial_resolution in ["NUTS2", "NUTS3", "LAU"]:
        original_resolution_id = db_access.get_primary_key(
            "original_resolutions", {"original_resolution": spatial_resolution}
        )

        sql_cmd = f"""SELECT id, var_aggregation_method FROM var_details 
                    WHERE 
                        (var_name NOT LIKE 'eucalc_%%' AND 
                        var_name NOT LIKE 'cimp_%%' AND 
                        var_name NOT LIKE 'cproj_%%')
                        AND original_resolution_id={original_resolution_id}
                        AND post_disagg_calculation_eq IS NULL;"""

        var_details = db_access.get_table(sql_cmd)

        for idx, row in var_details.iterrows():
            var_detail_id = row["id"]
            var_aggregation_method = row["var_aggregation_method"]

            aggregate_collected_var(
                cursor, var_detail_id, var_aggregation_method, spatial_resolution
            )


aggregate_collected_data()
