import re
import warnings
import numpy as np
import pandas as pd
from zoomin.db_access import get_proxy_data

# number of chars to consider based on a resolution
char_dict = {"NUTS3": 5, "NUTS2": 4, "NUTS1": 3, "NUTS0": 2}


def is_float(s):
    """
    check if the passed value is float.

    :param s: The value.
    :type s: Any

    :returns: A bool value
    :rtype: bool
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def solve_proxy_equation(equation: str, target_resolution: str):
    """
    If the proxy is an equation, e.g. population+gross_value_added, then the
    datasets are queried and the values are calculated per region to return
    final spatial proxy.

    :param equation: The proxy equation.
    :type equation: str

    :param target_resolution: The resolution at which the proxy data is to be calculated.
    :type target_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :returns: result
    :rtype: pd.DataFrame
    """
    # read in all the proxy data and normalise value column before performing arithmetic operations
    operators = r"[\+\-\*\%\(\)\/\n]"

    # Splitting the string using the defined pattern
    split_result = re.split(operators, equation)

    # Filtering out empty strings and digits
    var_list = [
        part.strip()
        for part in split_result
        if part.strip() and not part.strip().isdigit() and not is_float(part.strip())
    ]

    result = None

    for var_name in var_list:
        proxy_data = get_proxy_data(var_name, target_resolution)

        # If there is no variance in data, we cannot normailize it. So everything is just set to 0
        if (len(proxy_data) > 1) & (len(proxy_data["value"].unique()) == 1):
            proxy_data["value"] = 0
        else:
            proxy_data["value"] = (
                proxy_data["value"] / proxy_data["value"].max()
            )  # normalizing this way to retain true 0s in the normalized data

            proxy_data["value"] = proxy_data["value"].replace([np.inf, np.nan], 0)

        proxy_data.rename(columns={"value": var_name}, inplace=True)

        # NOTE: if a proxy value is missing and was not imputed using machine learning, then its 0 with confidence_level - MISSING (1).
        # However, as a proxy, its confidence_level is changed to VERY LOW (2)
        # Reason: After disaggregation of a target value using the proxy, the disaggregated value will get a confidence_level that is a minimum
        # of the proxy confidence, the target value confidence, and the confidence in the strength of proxy to spatially represent the target variable.
        # If the proxy confidence_level_id is 1, then the disaggregated target value will be shown as MISSING at the end.
        # We don't want that. We want it to show as VERY LOW.
        proxy_data.loc[
            proxy_data["confidence_level_id"] == 1, "confidence_level_id"
        ] = 2

        if result is None:
            result = proxy_data
        else:
            result = pd.merge(result, proxy_data, on=["region_code", "region_id"])

            # aggregate confidence_level_id : depends on the poorest quality rating, hence "min".
            result["confidence_level_id"] = result[
                ["confidence_level_id_x", "confidence_level_id_y"]
            ].min(axis=1)

            # aggregate year: most old data. Hence "min"
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

    equation = equation.replace("\n", " ")
    result = result.eval(f"value = {equation}")

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
    """
    Apply the passed binary criteria to the proxy dataset - set the proxy values
    to 0, where the criteria does not hold.

    :param proxy_data: The proxy dataset
    :type proxy_data: pd.DataFrame

    :param binary_disaggregation_criteria: The binary disaggregation criteria. e.g. "population>500"
    :type binary_disaggregation_criteria: str

    :param target_resolution: The spatial resolution of the proxy data.
    :type target_resolution: str, one of {"NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"}

    :returns: out_proxy_data
    :rtype: pd.DataFrame
    """
    out_proxy_data = proxy_data.copy()

    expr = binary_disaggregation_criteria.strip()
    binary_criteria_var_names = set(re.findall(r"\b[a-zA-Z_]\w*\b", expr))

    for var in binary_criteria_var_names:
        _proxy_data = get_proxy_data(var, target_resolution)
        _proxy_data = _proxy_data[["region_id", "value"]].copy()
        _proxy_data.rename(columns={"value": var}, inplace=True)

        out_proxy_data = pd.merge(out_proxy_data, _proxy_data, on="region_id")

    try:
        out_proxy_data["__mask__"] = out_proxy_data.eval(expr)
    except Exception as e:
        raise ValueError(
            f"Failed to evaluate criteria '{binary_disaggregation_criteria}': {e}"
        )

    drop_columns = list(binary_criteria_var_names)
    drop_columns.append("__mask__")

    out_proxy_data.loc[~out_proxy_data["__mask__"].fillna(False), "value"] = 0
    out_proxy_data.drop(columns=drop_columns, inplace=True)

    return out_proxy_data


def disaggregate_value(target_value, proxy_data, source_region_code):
    """
    Spatially disaggregate a value to its child/target regions.

    :param target_value: The value to be disaggregated.
    :type target_value: int/float

    :param proxy_data: The spatial proxy to be used.
    :type proxy_data: pd.DataFrame

    :param source_region_code: The region code of the source value, which is to be disaggregated.
    :type source_region_code: str

    :returns: disagg_data
    :rtype: pd.DataFrame
    """
    disagg_data = proxy_data.copy(deep=True)

    total = disagg_data["value"].values.sum()

    if total == 0 and target_value != 0:
        raise ValueError(
            f"The proxy values are all 0. Cannot distribute target value of {target_value} of region {source_region_code}"
        )

    elif target_value == 0:
        disagg_data = disagg_data.drop(["value"], axis=1)
        disagg_data["value"] = 0

    else:
        # disaggregte
        disagg_data["share"] = disagg_data["value"] / total
        disagg_data["disagg_value"] = disagg_data["share"] * target_value

        # clean up columns
        disagg_data = disagg_data.drop(columns=["value", "share"]).rename(
            columns={"disagg_value": "value"}
        )

    return disagg_data


def disaggregate_data(target_data, proxy_data, proxy_confidence_level):
    """
    Spatially disaggregate the passed `target_data` to a target resolution.
    Use `proxy_data` to obtain shares in each target region.

    :param target_data: The data to be disaggregated
    :type target_data: pd.DataFrame

    :param proxy_data: Data containing values in each target region
    :type proxy_data: pd.DataFrame

    :param proxy_confidence_level: The confidence in the spatial proxy used.
    :type proxy_confidence_level: int

    :returns: disagg_data
    :rtype: pd.DataFrame
    """
    # disaggregate value in each source region to the corresponding target regions
    disagg_df_list = []
    for _, row in target_data.iterrows():
        source_region_code = row["region_code"]
        _proxy_data = proxy_data[proxy_data["match_region_code"] == source_region_code]

        disagg_df = disaggregate_value(row["value"], _proxy_data, source_region_code)

        # Calculate confidence_level_id by taking the minimum of
        # confidence_level_id of proxy values, confidence_level_id of target value, and proxy_confidence_level
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

    return final_disagg_df
