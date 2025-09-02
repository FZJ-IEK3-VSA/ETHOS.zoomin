"""Functions to help disaggregate values to LAU and populate DB with data."""
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv, find_dotenv

from zoomin.db_access import (
    get_regions,
    add_to_processed_data,
)
from zoomin import disaggregation_utils as disagg_utils

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

db_country = os.environ.get("DB_COUNTRY")
db_version = os.environ.get("DB_VERSION")

db_name = f"{db_country.lower()}_v{db_version}"


def allocate_same_value_to_all_child_regions(
    var_data,
    source_resolution,
    target_resolution,
    proxy_confidence_level,
):
    """
    For the variables with "no proxy, same value all regions", as proxy value,
    same value as the parent region is allocated to the child regions. Finally,
    the disaggregated data is dumped into the `processed_data` table, in the database.

    :param var_data: The source data table to be allocated to a target spatial level
    :type var_data: pd.DataFrame

    :param source_resolution: The spatial resolution of source data/`var_data`
    :type source_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :param target_resolution: The spatial resolution to which to allocate the data
    :type target_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :param proxy_confidence_level: Defines the confidence in the accuracy of the target values.
    :type proxy_confidence_level: int
    """
    # STEP1: Disaggregate
    regions_df = get_regions(target_resolution)

    regions_df = disagg_utils.match_source_target_resolutions(
        source_resolution, regions_df
    )

    final_df = pd.merge(
        regions_df,
        var_data,
        left_on="match_region_code",
        right_on="region_code",
        how="right",
    )
    ## Calculate final confidence_level_id by taking the minimum between confidence_level_id of values
    ## and the proxy_confidence_level
    final_df.drop(
        columns=[
            "region_code_x",
            "region_code_y",
            "match_region_code",
        ],
        inplace=True,
    )
    final_df.rename(columns={"id": "region_id"}, inplace=True)

    final_df["confidence_level_id"] = np.minimum(
        final_df["confidence_level_id"], proxy_confidence_level
    )

    add_to_processed_data(final_df)


def perform_proxy_based_disaggregation(
    var_data,
    source_resolution,
    target_resolution,
    disagg_proxy,
    disagg_binary_criteria,
    proxy_confidence_level,
    var_unit,
):
    """
    The variables with spatial proxies are spatially disaggregated here. Finally,
    the disaggregated data is dumped into the `processed_data` table, in the database.

    :param var_data: The source data table to be allocated to a target spatial level
    :type var_data: pd.DataFrame

    :param source_resolution: The spatial resolution of source data/`var_data`
    :type source_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :param target_resolution: The spatial resolution to which to allocate the data
    :type target_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :param disagg_proxy: The spatial proxy. Could be a single variable or an equation.
    :type disagg_proxy: str

    :param disagg_binary_criteria: The binary criteria to be considered. E.g. population>500.
        In this case, the values are only disaggregated to all the target regions, whose
        population is greater than 500
    :type disagg_binary_criteria: str

    :param proxy_confidence_level: Defines the confidence in the accuracy of the target values.
    :type proxy_confidence_level: int

    :param var_unit: The unit of measure of the `var_data`.
    :type var_unit: str
    """
    # STEP1: Disaggregate
    proxy_data = disagg_utils.solve_proxy_equation(disagg_proxy, target_resolution)

    if len(proxy_data) == 0:
        raise ValueError("Proxy data not found in the database.")

    proxy_data = disagg_utils.match_source_target_resolutions(
        source_resolution, proxy_data
    )

    if isinstance(disagg_binary_criteria, str):
        proxy_data = disagg_utils.apply_binary_disaggregation_criteria(
            proxy_data, disagg_binary_criteria, target_resolution
        )

    final_df, is_bad_proxy_list = disagg_utils.disaggregate_data(
        var_data, proxy_data, proxy_confidence_level
    )

    final_df.drop(columns=["region_code", "match_region_code"], inplace=True)

    # round to whole number if var_unit is number like population values
    if var_unit == "number":
        final_df["value"] = final_df["value"].astype(int)

    add_to_processed_data(final_df)

    if any(is_bad_proxy_list):
        return "bad_proxy"
