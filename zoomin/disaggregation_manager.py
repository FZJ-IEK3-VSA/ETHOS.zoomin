"""Main functions to process climate vars, collected vars and EUCalc vars of all spatial levels"""
import pickle

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

        sql_cmd = f"""SELECT r.region_code, d.region_id, d.climate_experiment_id, d.var_detail_id, d.value, d.quality_rating_id, d.year, d.proxy_detail_id
                        FROM staged_climate_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE d.var_detail_id = {var_detail_id} AND d.year = {data_year}
        """
        var_data = get_table(sql_cmd)

    else:
        var_name = climate_var_detail
        var_detail_id = get_primary_key("var_details", {"var_name": var_name})

        sql_cmd = f"""SELECT r.region_code, d.region_id, d.climate_experiment_id, d.var_detail_id, d.value, d.quality_rating_id, d.year, d.proxy_detail_id
                        FROM staged_climate_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE d.var_detail_id = {var_detail_id}
        """
        var_data = get_table(sql_cmd)

    disaggregation_quality_rating = 3  # because all climate data is given this rating

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
            "proxy_detail_id",
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
            "proxy_detail_id",
        ]
    ].copy()

    # NOTE: all climate data is disaggregated the same way - same value all regions
    disagg.distribute_data_equally(
        data_to_disagg, var_name, source_resolution, disaggregation_quality_rating
    )


############## Collected data ##################


def process_collected_var(var_name) -> None:
    """Take processed and saved data, disaggregate to LAU regions and add to the database."""  # TODO: docstring

    var_details_row = get_table(
        f"SELECT id, original_resolution_id from var_details where var_name='{var_name}'"
    )
    var_detail_id = var_details_row["id"][0]
    original_resolution_id = var_details_row["original_resolution_id"][0]

    source_resolution = get_col_values(
        "original_resolutions", "original_resolution", {"id": original_resolution_id}
    )

    sql_cmd = f"""SELECT r.region_code, d.region_id, d.var_detail_id, d.value, d.quality_rating_id, d.year, d.proxy_detail_id
                    FROM staged_collected_data d
                    JOIN regions r ON d.region_id = r.id
                    WHERE var_detail_id = {var_detail_id}
    """
    var_data = get_table(sql_cmd)

    # STEP 1: Add a copy of staged data
    staged_db_df = var_data.drop(columns="region_code")
    add_to_processed_data(staged_db_df)

    # STEP 2: Aggregate staged data to higher spatial levels
    data_to_agg = var_data[
        [
            "region_code",
            "var_detail_id",
            "value",
            "quality_rating_id",
            "year",
            "proxy_detail_id",
        ]
    ].copy()

    disagg.aggregate_data(data_to_agg, var_name, source_resolution, "staged_data")

    ##STEP 3: Disaggregate (if source_resolution != LAU)
    if source_resolution != "LAU":
        proxy_detail_id = data_to_agg["proxy_detail_id"][0].item()

        proxy_details_row = get_table(
            f"""SELECT disaggregation_proxy, 
                                      disaggregation_quality_rating,
                                      disaggregation_binary_criteria,
                                      random_forest_model
                                      from proxy_details where id={proxy_detail_id}"""
        )

        disagg_proxy = proxy_details_row["disaggregation_proxy"][0]
        disaggregation_quality_rating = proxy_details_row[
            "disaggregation_quality_rating"
        ][0]

        if isinstance(disagg_proxy, str):
            if disagg_proxy == "no proxy, same value all regions":
                data_to_disagg = var_data[
                    [
                        "region_code",
                        "var_detail_id",
                        "value",
                        "quality_rating_id",
                        "year",
                        "proxy_detail_id",
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
                    [
                        "region_code",
                        "var_detail_id",
                        "value",
                        "quality_rating_id",
                        "year",
                        "proxy_detail_id",
                    ]
                ].copy()

                rf_model_data = proxy_details_row["random_forest_model"][0]

                rf_model = pickle.loads(rf_model_data)

                disagg.perform_random_forest_based_disaggregation(
                    var_name,
                    data_to_disagg,
                    rf_model,
                    disagg_proxy,
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
                        "proxy_detail_id",
                    ]
                ].copy()

                disagg_binary_criteria = proxy_details_row[
                    "disaggregation_binary_criteria"
                ][0]

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


def process_eucalc_var(var_name, pathway_description, year) -> None:
    """Take processed and saved data, disaggregate to LAU regions and add to the database."""  # TODO: docstring

    var_detail_id = get_primary_key("var_details", {"var_name": var_name})
    pathway_id = get_primary_key(
        "pathways", {"pathway_description": pathway_description}
    )

    sql_cmd = f"""SELECT region_id, var_detail_id, pathway_id, value, quality_rating_id, year, proxy_detail_id
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
            "proxy_detail_id",
        ]
    ].copy()

    ## get proxy details it is making 3 requests to DB #TODO: change it to just 1
    proxy_detail_id = data_to_disagg["proxy_detail_id"][0].item()

    disagg_binary_criteria = get_col_values(
        "proxy_details",
        "disaggregation_binary_criteria",
        {"id": proxy_detail_id},
    )
    disagg_proxy = get_col_values(
        "proxy_details", "disaggregation_proxy", {"id": proxy_detail_id}
    )

    disaggregation_quality_rating = get_col_values(
        "proxy_details", "disaggregation_quality_rating", {"id": proxy_detail_id}
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
            )
            return bad_proxy

    else:
        raise ValueError(
            "One of proxy_equation or same_value_all_regions should be provided"
        )
