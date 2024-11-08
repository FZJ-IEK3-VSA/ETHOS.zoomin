"""Main functions to process climate vars, collected vars and EUCalc vars of all spatial levels"""
from zoomin.db_access import (
    get_primary_key,
    get_table,
    add_to_processed_data,
)
from zoomin import disaggregation as disagg

############## Climate data ##################
def disaggregate_climate_var(climate_var_detail) -> None:
    """Disaggregate to the specified spatial resolution and add to the database."""  # TODO: docstring
    # get data
    if "cproj_" in climate_var_detail:
        [var_name, data_year] = climate_var_detail.split("-")

        data_year = int(data_year)

        sql_cmd = f"""SELECT r.region_code, d.climate_experiment, d.var_detail_id, d.value, d.confidence_level_id, d.year, d.proxy_detail_id
                        FROM staged_climate_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE d.var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}') 
                            AND d.year = {data_year}"""
        var_data = get_table(sql_cmd)

    else:
        var_name = climate_var_detail

        sql_cmd = f"""SELECT r.region_code, d.climate_experiment, d.var_detail_id, d.value, d.confidence_level_id, d.year, d.proxy_detail_id
                        FROM staged_climate_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE d.var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}')"""
        var_data = get_table(sql_cmd)

    proxy_confidence_level = 3  # because all climate data is given this rating

    source_resolution = "NUTS3"  # because all climate data is at NUTS3
    target_resolution = "LAU"  # because only LAU is possible

    # Disaggregate
    # NOTE: all climate data is disaggregated the same way - same value all regions
    disagg.distribute_data_equally(
        var_data,
        source_resolution,
        target_resolution,
        proxy_confidence_level,
    )


############## Collected data ##################
def disaggregate_collected_var(var_name, source_resolution, target_resolution) -> None:
    """Disaggregate to the specified spatial resolution and add to the database."""  # TODO: docstring

    # get data
    sql_cmd = f"""SELECT r.region_code, d.var_detail_id, d.value, d.confidence_level_id, d.year, d.proxy_detail_id,
                    FROM staged_collected_data d
                    JOIN regions r ON d.region_id = r.id
                    WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}');"""
    var_data = get_table(sql_cmd)

    var_unit = f"SELECT var_unit FROM var_details WHERE var_name = '{var_name}';"

    # Disaggregate
    proxy_detail_id = var_data["proxy_detail_id"][0].item()

    proxy_details_row = get_table(
        f"""SELECT disaggregation_proxy, proxy_confidence_level, disaggregation_binary_criteria
                FROM proxy_details WHERE id={proxy_detail_id}"""
    )

    disagg_proxy = proxy_details_row["disaggregation_proxy"][0]
    proxy_confidence_level = proxy_details_row["proxy_confidence_level"][0]

    if isinstance(disagg_proxy, str):
        if disagg_proxy == "no proxy, same value all regions":

            disagg.distribute_data_equally(
                var_data, source_resolution, target_resolution, proxy_confidence_level
            )

        else:
            disagg_binary_criteria = proxy_details_row[
                "disaggregation_binary_criteria"
            ][0]

            bad_proxy = disagg.perform_proxy_based_disaggregation(
                var_data,
                source_resolution,
                target_resolution,
                disagg_proxy,
                disagg_binary_criteria,
                proxy_confidence_level,
                var_unit,
            )
            return bad_proxy

    else:
        raise ValueError(
            "One of proxy_equation or same_value_all_regions should be provided"
        )


############## EUCalc data ##################
def disaggregate_eucalc_var(var_name, pathway, year, target_resolution) -> None:
    """Disaggregate to the specified spatial resolution and add to the database."""  # TODO: docstring

    # get data
    sql_cmd = f"""SELECT r.region_code, d.var_detail_id, d.pathway, d.value, d.confidence_level_id, d.year, d.proxy_detail_id
                    FROM staged_eucalc_data d
                    JOIN regions r ON d.region_id = r.id
                    WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}') AND 
                            d.pathway = '{pathway}' AND 
                            d.year = {year}"""

    var_data = get_table(sql_cmd)

    var_unit = f"SELECT var_unit FROM var_details WHERE var_name = '{var_name}';"

    # Disaggregate
    proxy_detail_id = var_data["proxy_detail_id"][0].item()

    proxy_details_row = get_table(
        f"""SELECT disaggregation_proxy, proxy_confidence_level, disaggregation_binary_criteria
                FROM proxy_details WHERE id={proxy_detail_id}"""
    )

    disagg_proxy = proxy_details_row["disaggregation_proxy"][0]
    proxy_confidence_level = proxy_details_row["proxy_confidence_level"][0]

    ## disaggregate
    if isinstance(disagg_proxy, str):
        if disagg_proxy == "no proxy, same value all regions":
            disagg.distribute_data_equally(
                var_data,
                "NUTS0",
                target_resolution,
                proxy_confidence_level,
            )

        else:
            disagg_binary_criteria = proxy_details_row[
                "disaggregation_binary_criteria"
            ][0]

            bad_proxy = disagg.perform_proxy_based_disaggregation(
                var_data,
                "NUTS0",
                target_resolution,
                disagg_proxy,
                disagg_binary_criteria,
                proxy_confidence_level,
                var_unit,
            )
            return bad_proxy

    else:
        raise ValueError(
            "One of proxy_equation or same_value_all_regions should be provided"
        )
