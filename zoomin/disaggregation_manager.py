"""Main functions to process climate vars, collected vars and EUCalc vars of all spatial levels"""
import pandas as pd

from zoomin.db_access import (
    get_regions,
    get_primary_key,
    get_table,
    get_col_values,
    add_to_processed_data,
)
from zoomin import disaggregation as disagg


############## Climate data ##################


def process_climate_var(climate_var_detail) -> None:
    """Take staged climate data, disaggregate to LAU regions and add to the database."""  # TODO: docstring

    if "cproj_" in climate_var_detail:
        [var_name, data_year] = climate_var_detail.split("-")

        data_year = int(data_year)

        var_detail_id = get_primary_key("var_details", {"var_name": var_name})

        sql_cmd = f"""SELECT r.region_code, d.region_id, d.climate_experiment_id, d.var_detail_id, d.value, d.quality_rating_id, d.year
                        FROM staged_climate_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE d.var_detail_id = {var_detail_id} AND d.year = {data_year}
        """
        var_data = get_table(sql_cmd)

    else:
        var_name = climate_var_detail
        var_detail_id = get_primary_key("var_details", {"var_name": var_name})

        sql_cmd = f"""SELECT r.region_code, d.region_id, d.climate_experiment_id, d.var_detail_id, d.value, d.quality_rating_id, d.year
                        FROM staged_climate_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE d.var_detail_id = {var_detail_id}
        """
        var_data = get_table(sql_cmd)

    disaggregation_quality_rating = get_col_values(
        "var_details", "disaggregation_quality_rating", {"var_name": var_name}
    )

    source_resolution = "NUTS3"  # because all climate data is at NUTS3

    # STEP 1: Add a copy of staged data
    staged_db_df = var_data.drop(columns="region_code")
    add_to_processed_data(staged_db_df)

    # STEP 2: Aggregate staged data to higher spatial levels
    data_to_agg = var_data[
        [
            "region_code",
            "climate_experiment_id",
            "var_detail_id",
            "value",
            "quality_rating_id",
            "year",
        ]
    ].copy()

    disagg.aggregate_data(data_to_agg, var_name, source_resolution, "staged_data")

    # STEP 3: Disaggregate
    data_to_disagg = var_data[
        [
            "region_code",
            "climate_experiment_id",
            "var_detail_id",
            "value",
            "quality_rating_id",
            "year",
        ]
    ].copy()

    # NOTE: all climate data is disaggregated the same way - same value all regions
    disagg.distribute_data_equally(
        data_to_disagg, var_name, source_resolution, disaggregation_quality_rating
    )


############## Collected data ##################


def process_collected_var(var_name) -> None:
    """Take processed and saved data, disaggregate to LAU regions and add to the database."""  # TODO: docstring

    var_detail_id = get_primary_key("var_details", {"var_name": var_name})

    sql_cmd = f"""SELECT r.region_code, d.region_id, d.var_detail_id, d.value, d.quality_rating_id, d.year
                    FROM staged_collected_data d
                    JOIN regions r ON d.region_id = r.id
                    WHERE var_detail_id = {var_detail_id}
    """
    var_data = get_table(sql_cmd)

    original_resolution_id = get_col_values(
        "var_details", "original_resolution_id", {"var_name": var_name}
    )
    source_resolution = get_col_values(
        "original_resolutions", "original_resolution", {"id": original_resolution_id}
    )

    # STEP 1: Add a copy of staged data
    staged_db_df = var_data.drop(columns="region_code")
    add_to_processed_data(staged_db_df)

    # STEP 2: Aggregate staged data to higher spatial levels
    data_to_agg = var_data[
        ["region_code", "var_detail_id", "value", "quality_rating_id", "year"]
    ].copy()

    disagg.aggregate_data(data_to_agg, var_name, source_resolution, "staged_data")

    ##STEP 3: Disaggregate (if source_resolution != LAU)
    if source_resolution != "LAU":
        processing_detail_id = get_col_values(
            "var_details", "processing_detail_id", {"var_name": var_name}
        )

        disagg_proxy = get_col_values(
            "processing_details", "disaggregation_proxy", {"id": processing_detail_id}
        )

        disaggregation_quality_rating = get_col_values(
            "var_details", "disaggregation_quality_rating", {"var_name": var_name}
        )

        if isinstance(disagg_proxy, str):
            if disagg_proxy == "no proxy, same value all regions":
                data_to_disagg = var_data[
                    [
                        "region_code",
                        "var_detail_id",
                        "value",
                        "quality_rating_id",
                        "year",
                    ]
                ].copy()

                disagg.distribute_data_equally(
                    data_to_disagg,
                    var_name,
                    source_resolution,
                    disaggregation_quality_rating,
                )

            elif "Disaggregation using random forest model." in disagg_proxy:
                data_to_disagg = var_data[
                    ["region_code", "var_detail_id", "quality_rating_id", "year"]
                ].copy()

                disagg.perform_random_forest_based_disaggregation(
                    var_name,
                    data_to_disagg,
                    disagg_proxy,
                    processing_detail_id,
                    source_resolution,
                    disaggregation_quality_rating,
                )

            else:
                data_to_disagg = var_data[
                    [
                        "region_code",
                        "var_detail_id",
                        "value",
                        "quality_rating_id",
                        "year",
                    ]
                ].copy()

                disagg_binary_criteria = get_col_values(
                    "processing_details",
                    "disaggregation_binary_criteria",
                    {"id": processing_detail_id},
                )

                bad_proxy = disagg.perform_proxy_based_disaggregation(
                    data_to_disagg,
                    var_name,
                    source_resolution,
                    disagg_proxy,
                    disagg_binary_criteria,
                    disaggregation_quality_rating,
                )
                return bad_proxy

        else:
            raise ValueError(
                "Either the data should be at LAU resolution. or one of proxy_equation or same_value_all_regions should be provided"
            )


############## EUCalc data ##################


def process_eucalc_var(var_name, pathway_name, year) -> None:
    """Take processed and saved data, disaggregate to LAU regions and add to the database."""  # TODO: docstring

    country = pathway_name[:2].upper()

    var_detail_id = get_primary_key("var_details", {"var_name": var_name})
    pathway_id = get_primary_key("pathways", {"pathway_file_name": pathway_name})

    sql_cmd = f"""SELECT region_id, var_detail_id, pathway_id, value, quality_rating_id, year
                    FROM staged_eucalc_data 
                    WHERE var_detail_id = {var_detail_id} AND pathway_id = {pathway_id} AND year = {year}
    """
    var_data = get_table(sql_cmd)

    # STEP 1: Add a copy of staged data
    staged_db_df = var_data.copy(deep=True)
    add_to_processed_data(staged_db_df)

    source_resolution = "NUTS0"

    ##STEP 2: Disaggregate
    ##prep data
    _regions_df = get_regions(source_resolution)
    prepped_var_data = pd.merge(
        _regions_df,
        var_data,
        left_on="id",
        right_on="region_id",
        how="right",
    )

    data_to_disagg = prepped_var_data[
        [
            "region_code",
            "var_detail_id",
            "pathway_id",
            "value",
            "quality_rating_id",
            "year",
        ]
    ].copy()

    ## get processing details
    processing_detail_id = get_col_values(
        "var_details", "processing_detail_id", {"var_name": var_name}
    )

    disagg_binary_criteria = get_col_values(
        "processing_details",
        "disaggregation_binary_criteria",
        {"id": processing_detail_id},
    )
    disagg_proxy = get_col_values(
        "processing_details", "disaggregation_proxy", {"id": processing_detail_id}
    )

    disaggregation_quality_rating = get_col_values(
        "var_details", "disaggregation_quality_rating", {"var_name": var_name}
    )

    ## disaggregate
    if isinstance(disagg_proxy, str):
        if disagg_proxy == "no proxy, same value all regions":
            disagg.distribute_data_equally(
                data_to_disagg,
                var_name,
                source_resolution,
                disaggregation_quality_rating,
            )

        else:
            bad_proxy = disagg.perform_proxy_based_disaggregation(
                data_to_disagg,
                var_name,
                source_resolution,
                disagg_proxy,
                disagg_binary_criteria,
                disaggregation_quality_rating,
                country,
            )
            return bad_proxy

    else:
        raise ValueError(
            "One of proxy_equation or same_value_all_regions should be provided"
        )
