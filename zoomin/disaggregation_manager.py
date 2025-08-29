"""Main functions to process climate vars, collected vars and EUCalc vars of all spatial levels"""
from zoomin.db_access import get_table, get_values
from zoomin import disaggregation as disagg

############## Climate data ##################
def disaggregate_climate_var(climate_var_detail) -> None:
    """
    Spatially disaggregate the passed climate data, for a particular year and dump
    it into `processed_data` table in the database.

    :param climate_var_detail: The climate variable appended with a year.
        e.g.: "cproj_annual_mean_minimum_temperature-2030"
    :type climate_var_detail: str

    :param proxy_data: Data containing values in each target region
    :type proxy_data: pd.DataFrame

    :param proxy_confidence_level: The confidence in the spatial proxy used.
    :type proxy_confidence_level: int

    :returns: disagg_data
    :rtype: pd.DataFrame
    """
    # get data
    if ("cproj_" in climate_var_detail) or ("cimp_ts" in climate_var_detail):
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
    disagg.allocate_same_value_to_all_child_regions(
        var_data,
        source_resolution,
        target_resolution,
        proxy_confidence_level,
    )


############## Collected data ##################
def disaggregate_collected_var(var_name, source_resolution, target_resolution) -> None:
    """
    Spatially disaggregate the passed collected variable data and dump
    it into `processed_data` table in the database.

    :param var_name: Name of the collected data variable.
    :type var_name: str

    :param source_resolution: The spatial resolution of the collected dataset.
    :type source_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3"}

    :param target_resolution: The spatial resolution to which the collected dataset is to be disaggregated.
    :type target_resolution: str, one of {"NUTS1", "NUTS2", "NUTS3", "LAU"}
    """
    # get data
    sql_cmd = f"""SELECT r.region_code, d.var_detail_id, d.value, d.confidence_level_id, d.year, d.proxy_detail_id
                    FROM staged_collected_data d
                    JOIN regions r ON d.region_id = r.id
                    WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}');"""
    var_data = get_table(sql_cmd)

    var_unit = get_values(
        f"SELECT var_unit FROM var_details WHERE var_name = '{var_name}';"
    )

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

            disagg.allocate_same_value_to_all_child_regions(
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
    """
    Spatially disaggregate the passed EUCalc variable data and dump it
    into `processed_data` table in the database.

    :param var_name: Name of the EUCalc data variable.
    :type var_name: str

    :param pathway: EUCalc pathway.
    :type pathway: str, one of {"national", "with_behavioural_changes"}

    :param year: EUCalc variable year.
    :type year: int

    :param target_resolution: The spatial resolution to which the EUCalc dataset is to be disaggregated.
    :type target_resolution: str, one of {"NUTS1", "NUTS2", "NUTS3", "LAU"}
    """
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
            disagg.allocate_same_value_to_all_child_regions(
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
