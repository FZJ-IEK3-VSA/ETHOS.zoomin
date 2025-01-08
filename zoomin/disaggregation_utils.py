import re
import warnings
import numpy as np
import pandas as pd
from zoomin.db_access import get_proxy_data

# number of chars to consider based on a resolution
char_dict = {"NUTS3": 5, "NUTS2": 4, "NUTS1": 3, "NUTS0": 2}


def solve_proxy_equation(equation: str, target_resolution):
    # TODO: doctrings

    # read in all the proxy data and normalise value column before performing arithmetic operations
    operators = r"[\+\-\*\%\(\)\/\.\n]"

    # Splitting the string using the defined pattern
    split_result = re.split(operators, equation)

    # Filtering out empty strings and digits
    var_list = [
        part.strip()
        for part in split_result
        if part.strip() and not part.strip().isdigit()
    ]

    result = None

    for var_name in var_list:
        proxy_data = get_proxy_data(var_name, target_resolution)

        # If there is no variance in data, we cannot normailize it. So everything is just set to 0
        if len(proxy_data["value"].unique()) == 1:
            proxy_data["value"] = 0
        else:
            proxy_data["value"] = (
                proxy_data["value"] / proxy_data["value"].max()
            )  # normalizing this way to retain true 0s in the normalized data

        proxy_data.rename(columns={"value": var_name}, inplace=True)

        if result is None:
            result = proxy_data
        else:
            result = pd.merge(result, proxy_data, on=["region_code", "region_id"])

            # merged confidence_level_id and year
            # NOTE: depends on the poorest quality rating and most old data. Hence min
            result["confidence_level_id"] = result[
                ["confidence_level_id_x", "confidence_level_id_y"]
            ].min(axis=1)
            result["year"] = result[["year_x", "year_y"]].min(axis=1)

            result.drop(
                columns=[
                    "confidence_level_id_x",
                    "confidence_level_id_y",
                    "year_x",
                    "year_y",
                ],
                inplace=True,
            )

    result = result.eval(f"value = {equation}")
    result["value"] = result["value"].replace([np.inf, np.nan], 0)

    result = result[
        ["region_code", "region_id", "confidence_level_id", "year", "value"]
    ].copy()

    return result


def match_source_target_resolutions(
    source_resolution: str, proxy_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Add a 'match_region_code' column to `proxy_data`. This column should contain
    regions from `source_resolution` that correspond to the target regions of `proxy_data`.

    :param source_resolution: The resolution of the source value.
    :type source_resolution: str

    :param proxy_data: Data containing values in each target region
    :type proxy_data: pd.DataFrame

    :returns: proxy_data
    :rtype: pd.DataFrame
    """
    n_char = char_dict[source_resolution]
    proxy_data["match_region_code"] = proxy_data["region_code"].str[:n_char]

    return proxy_data


def apply_binary_disaggregation_criteria(
    proxy_data, binary_disaggregation_criteria, target_resolution
):
    # TODO: docstring
    """set the values in the value column of proxy_data to 0 for those region_ids where the corresponding value in the result dataframe is less than the threshold."""
    out_proxy_data = proxy_data.copy()

    [equation, threshold] = binary_disaggregation_criteria.split(
        ">="
    )  # NOTE: only greater than or equal to is implemented

    result = solve_proxy_equation(equation, target_resolution)
    result = result[["region_id", "region_code", "value"]].copy()

    # Merge the dataframes on 'region_id'
    merged = proxy_data.merge(result, on=["region_id", "region_code"], how="left")

    # Update the 'value' column in proxy_data based on the threshold
    merged.loc[merged["value_y"] < float(threshold), "value_x"] = 0

    # Drop the extra columns and rename the columns to original names
    out_proxy_data = merged.drop(columns=["value_y"]).rename(
        columns={"value_x": "value"}
    )

    return out_proxy_data


def disaggregate_value(
    target_value, proxy_data
):  # TODO: add a feature that disaggregates int value resulting in int values again
    # TODO: docstring
    disagg_data = proxy_data.copy(deep=True)

    total = disagg_data["value"].values.sum()

    # INFO: If proxy data is 0 in all regions, then the target value cannot
    # be distributed to target regions. In this case, the provided
    # proxy is ignored and the target value is equally distributed
    # to all target regions TODO: raise a warning. Make sure no proxy leads to this situation because
    # this means that it is a bad_proxy
    if total == 0:
        disagg_data = disagg_data.drop(["value"], axis=1)
        disagg_data["value"] = target_value / len(proxy_data)
        is_bad_proxy = True

    elif target_value == 0:
        disagg_data = disagg_data.drop(["value"], axis=1)
        disagg_data["value"] = 0

        is_bad_proxy = False

    else:
        # disaggregte
        disagg_data["share"] = disagg_data["value"] / total
        disagg_data["disagg_value"] = disagg_data["share"] * target_value

        # clean up columns
        disagg_data = disagg_data.drop(columns=["value", "share"]).rename(
            columns={"disagg_value": "value"}
        )

        is_bad_proxy = False

    return disagg_data, is_bad_proxy


def disaggregate_data(target_data, proxy_data, proxy_confidence_level):
    """#TODO: update docstring
    Spatially disaggregate the passed `target_data` to a target resolution.
    Use `proxy_data` to obtain shares in each target region.

    :param target_data: The data to be disaggregated
    :type target_data: int/float

    :param proxy_data: Data containing values in each target region
    :type proxy_data: pd.DataFrame

    :returns: disagg_data, is_bad_proxy #TODO: check how to write doc string with two return values
    """
    # disaggregate value in each source region to the corresponding target regions
    disagg_df_list = []

    for key_row in target_data.iterrows():
        row = key_row[1]
        _proxy_data = proxy_data[proxy_data["match_region_code"] == row["region_code"]]

        is_bad_proxy_list = []

        disagg_df, is_bad_proxy = disaggregate_value(row["value"], _proxy_data)
        is_bad_proxy_list.append(is_bad_proxy)

        ## Calculate confidence_level_id by taking the minimum of
        ## confidence_level_id of proxy values, confidence_level_id of target value, and proxy_confidence_level
        _confidence_level = min(proxy_confidence_level, row["confidence_level_id"])

        disagg_df["confidence_level_id"] = np.minimum(
            disagg_df[
                "confidence_level_id"
            ],  # NOTE: disagg_df had proxy data's quality rating and year at this point
            _confidence_level,
        )

        # NOTE: same year as the target value to all
        target_data_year = row["year"]
        disagg_df["year"] = target_data_year

        # add var_detail_id
        disagg_df["var_detail_id"] = row["var_detail_id"]

        # add proxy_detail_id
        disagg_df["proxy_detail_id"] = row["proxy_detail_id"]

        # add pathway
        if "pathway" in row.keys():
            disagg_df["pathway"] = row["pathway"]

        disagg_df_list.append(disagg_df)

    final_disagg_df = pd.concat(disagg_df_list)

    return final_disagg_df, is_bad_proxy_list
