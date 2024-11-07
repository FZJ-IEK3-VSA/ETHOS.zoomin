"""Creates a copy of staged climate data in processed_data table and aggregates the data to higher spatial levels"""
from zoomin.db_access import with_db_connection


@with_db_connection()
def copy_climate_data_into_processed_data(cursor):
    sql_cmd = """INSERT INTO processed_data (
                    region_id,
                    var_detail_id,
                    confidence_level_id,
                    proxy_detail_id,
                    climate_experiment,
                    year,
                    value
                )
                SELECT 
                    region_id,
                    var_detail_id,
                    confidence_level_id,
                    proxy_detail_id,
                    climate_experiment,
                    year,
                    value
                FROM 
                    staged_climate_data;"""
    cursor.execute(sql_cmd)


copy_climate_data_into_processed_data()

# number of chars to consider based on a resolution
char_dict = {"NUTS3": 5, "NUTS2": 4, "NUTS1": 3, "NUTS0": 2}


def aggregate_climate_data(cursor, data_type, agg_spatial_level):

    if data_type == "climate_projection":
        var_start = "cproj"
        agg_mode = "AVG"
    else:
        var_start = "cimp"
        agg_mode = "MAX"

    n_char_to_keep = char_dict[agg_spatial_level]

    sql_cmd = f"""
    INSERT INTO processed_data (region_id, var_detail_id, confidence_level_id, proxy_detail_id, climate_experiment, year, value)
    SELECT 
        r_agg.id,
        scd.var_detail_id,
        MIN(scd.confidence_level_id),
        scd.proxy_detail_id,
        scd.climate_experiment,
        scd.year,
        ROUND(CAST({agg_mode}(scd.value) AS numeric), 5)
    FROM 
        staged_climate_data AS scd
    JOIN 
        regions AS r_nuts3 ON scd.region_id = r_nuts3.id
    JOIN 
        regions AS r_agg ON LEFT(r_nuts3.region_code, {n_char_to_keep}) = r_agg.region_code
    WHERE 
        scd.var_detail_id IN (SELECT id FROM var_details WHERE var_name LIKE '{var_start}_%')
    GROUP BY 
        r_agg.id,
        scd.var_detail_id,
        scd.proxy_detail_id,
        scd.climate_experiment,
        scd.year;
    """

    cursor.execute(sql_cmd)


@with_db_connection()
def aggregate_all_levels(cursor):
    for data_type in ["climate_projection", "climate_impact"]:
        for agg_spatial_level in ["NUTS0", "NUTS1", "NUTS2"]:
            aggregate_climate_data(cursor, data_type, agg_spatial_level)


aggregate_all_levels()
