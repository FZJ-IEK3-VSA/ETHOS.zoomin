from zoomin.db_access import with_db_connection
from zoomin import db_access
from zoomin import disaggregation_manager as disagg_manager

var_name = "eucalc_ind_material_production_chemicals"

eucalc_years = ["2020", "2025", "2030", "2035", "2040", "2045", "2050"]
pathways = ["national", "with_behavioural_changes"]


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
                    staged_eucalc_data
                WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}');"""

    cursor.execute(sql_cmd)


copy_eucalc_data_into_processed_data()

# disaggregate
for spatial_level in ["NUTS2", "NUTS3", "LAU"]:
    for pathway in pathways:
        for year in eucalc_years:
            bad_proxy = disagg_manager.disaggregate_eucalc_var(
                var_name, pathway, year, "LAU"
            )
