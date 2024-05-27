from typing import Optional
import warnings
import numpy as np
import pandas as pd
from zoomin.db_access import get_processed_lau_data

# number of chars to consider based on a resolution
char_dict = {"NUTS3": 5, "NUTS2": 4, "NUTS1": 3, "NUTS0": 2}


def solve_dfs(df_1, df_2, operator):
    # TODO: docstring
    result = pd.merge(
        df_1,
        df_2,
        on=["region_id", "region_code"],
        how="inner",
    )

    # normalise value_x and value_y columns before performing arithmetic operations on them
    result["value_x"] = (result["value_x"] - result["value_x"].min()) / (
        result["value_x"].max() - result["value_x"].min()
    )
    
    result["value_y"] = (result["value_y"] - result["value_y"].min()) / (
        result["value_y"].max() - result["value_y"].min()
    )

    result["value_x"].replace([np.inf, np.nan], 0, inplace=True)
    result["value_y"].replace([np.inf, np.nan], 0, inplace=True)


    if operator == "+":
        result["value"] = result["value_x"] + result["value_y"]

    elif operator == "/":
        result["value"] = result["value_x"] / result["value_y"]

        # NOTE: there are some 0s in the data that lead to NAs/infinity in the calculation due to divide by 0 problem
        # for now these are set to 0
        if np.isinf(result["value"].values).any() or result["value"].isna().any():
            warnings.warn("INFs/NAs present in calculated data. These are set to 0")
            result["value"].replace([np.inf, np.nan], 0, inplace=True)

    elif operator == "*":
        result["value"] = result[["value_x", "value_y"]].multiply(axis=1)

    else:
        raise ValueError("Unknown operation")

    # merged quality_rating_id and year (required for finaly quality rating evaluation
    # NOTE: depends on the poorest quality rating and most old data. Hence min
    result["quality_rating_id"] = result[
        ["quality_rating_id_x", "quality_rating_id_y"]
    ].min(axis=1)
    result["year"] = result[["year_x", "year_y"]].min(axis=1)

    result.drop(
        columns=[
            "value_x",
            "value_y",
            "quality_rating_id_x",
            "quality_rating_id_y",
            "year_x",
            "year_y",
        ],
        inplace=True,
    )

    return result


def add_vars(equation: str):
    """#TODO: doctrings
    The equation allows for construction of complex proxies.
     * Ex.: "population/statistical area" #TODO: more examples with |s
    """
    proxy_vars_ids = equation.split("+")

    for i, var_id in enumerate(proxy_vars_ids):
        var_data = get_processed_lau_data(var_id)

        if i == 0:
            result = var_data

        else:
            result = solve_dfs(result, var_data, "+")

    return result


def divide_vars(equation: str):
    # TODO: docstrings
    [var_id_1, var_id_2] = equation.split("/")

    var_1_df = get_processed_lau_data(var_id_1)
    var_2_df = get_processed_lau_data(var_id_2)

    result = solve_dfs(var_1_df, var_2_df, "/")

    return result


def multiply_vars(equation: str):
    # TODO: docstring
    [var_id_1, var_id_2] = equation.split("*")

    if var_id_1.isdigit():
        result = get_processed_lau_data(var_id_2)
        result["value"] = result["value"] * float(var_id_1)

    else:
        var_1_df = get_processed_lau_data(var_id_1)
        var_2_df = get_processed_lau_data(var_id_2)

        result = solve_dfs(var_1_df, var_2_df, "*")

    return result


def solve_proxy_equation(equation: str):
    # TODO: doctrings
    """
    Currently covers the following cases:
    1. single proxy: var_1
    2. several proxies added without weighting: var_1 + var_2 + var_3 ...
    3. 1 proxy divided by the other: var_1/var_2
    4. several proxies added with weighting: 2*var_1 |+ 3*var_2 | .....
    5. sum of proxies (or divide or multiply two proxies), divided by a proxy: var_1 + var_2 ... |/ var_n
    """

    def _calculate(_eq):
        if "/" in _eq:
            result = divide_vars(_eq)

        elif "+" in _eq:
            result = add_vars(_eq)

        elif "*" in _eq:
            result = multiply_vars(_eq)

        else:
            result = get_processed_lau_data(_eq)

        return result

    eq_parts = equation.split("|")

    for i, eq_part in enumerate(eq_parts):
        if i == 0:
            result = _calculate(eq_part)
        else:
            operator = eq_part[0]
            _eq_part = eq_part[1:]

            _result = _calculate(_eq_part)

            result = solve_dfs(result, _result, operator)

    # normalize value column
    result["value"] = (result["value"] - result["value"].min()) / (
        result["value"].max() - result["value"].min()
    )
    result["value"].replace([np.inf, np.nan], 0, inplace=True)

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
    proxy_data, binary_disaggregation_criteria
):
    # TODO: docstring
    """set the values in the value column of proxy_data to 0 for those region_ids where the corresponding value in the result dataframe is less than the threshold."""
    out_proxy_data = proxy_data.copy()

    [equation, threshold] = binary_disaggregation_criteria.split(
        ">="
    )  # NOTE: only greater than or equal to is implemented

    result = solve_proxy_equation(equation)
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


def disaggregate_data(target_data, proxy_data, disaggregation_quality_rating):
    """#TODO: update docstring
    Spatially disaggregate the passed `target_value` to a target resolution.
    Use `proxy_data` to obtain shares in each target region.

    :param target_value: The value to be disaggregated
    :type target_value: int/float

    :param proxy_data: Data containing values in each target region
    :type proxy_data: pd.DataFrame

    :returns: disagg_data, is_bad_proxy #TODO: check how to write doc string with two return values

        * Contains aggregated time series as values
        * Coordinates correspond to new regions

        (In the above example, '01_reg_02_reg', '03_reg_04_reg' form new coordinates)
    :rtype: xr.DataArray
    """
    # disaggregate value in each source region to the corresponding target regions
    disagg_df_list = []

    for key_row in target_data.iterrows():
        row = key_row[1]
        _proxy_data = proxy_data[proxy_data["match_region_code"] == row["region_code"]]

        is_bad_proxy_list = []

        disagg_df, is_bad_proxy = disaggregate_value(row["value"], _proxy_data)
        is_bad_proxy_list.append(is_bad_proxy)

        ## Calculate quality_rating_id by taking the minimum of
        ## quality_rating_id of proxy values, quality_rating_id of target value, and disaggregation_quality_rating
        _quality_rating = min(disaggregation_quality_rating, row["quality_rating_id"])

        disagg_df["quality_rating_id"] = np.minimum(
            disagg_df[
                "quality_rating_id"
            ],  # NOTE: disagg_df had proxy data's quality rating and year at this point
            _quality_rating,
        )

        # NOTE: same year as the target value to all
        target_data_year = row["year"]
        disagg_df["year"] = target_data_year

        # add var_detail_id
        disagg_df["var_detail_id"] = row["var_detail_id"]

        # add proxy_detail_id
        disagg_df["proxy_detail_id"] = row["proxy_detail_id"]

        # add pathway_id
        if "pathway_id" in row.keys():
            disagg_df["pathway_id"] = row["pathway_id"]

        disagg_df_list.append(disagg_df)

    final_disagg_df = pd.concat(disagg_df_list)

    return final_disagg_df, is_bad_proxy_list
