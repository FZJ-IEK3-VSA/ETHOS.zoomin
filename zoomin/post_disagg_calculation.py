import warnings
import pandas as pd

from zoomin.db_access import (
    get_primary_key,
    get_col_values,
    add_to_processed_data,
    get_data_for_calculations,
)


def merge_dfs(df_1, df_2, calc_var_type):
    if calc_var_type in ["soi", "collected_var"]:
        result_df = pd.merge(
            df_1,
            df_2,
            on="region_id",
            how="outer",
        )

    elif calc_var_type == "eucalc_var":
        result_df = pd.merge(
            df_1,
            df_2,
            on=["region_id", "pathway_id", "year"],
            how="outer",
        )

    else:
        raise ValueError(f"Unknown calc_var_type - {calc_var_type}")

    return result_df


def solve_dfs(df_1, df_2, operator, calc_var_type):
    result_df = merge_dfs(df_1, df_2, calc_var_type)

    if operator == "/":
        result_df["value"] = result_df["value_x"].astype("float64") / result_df[
            "value_y"
        ].astype("float64")

        # NOTE: divide by 0 leads to NAs. These are set to 0
        if result_df["value"].isna().any():
            warnings.warn("NAs present in calculated data. These are set to 0")
            result_df["value"].fillna(value=0, inplace=True)

    elif operator == "-":
        result_df["value"] = result_df["value_x"] - result_df["value_y"]

    elif operator == "+":
        result_df["value"] = result_df["value_x"] + result_df["value_y"]

    elif operator == "*":
        result_df["value"] = result_df["value_x"] * result_df["value_y"]

    else:
        raise ValueError("Unknown operation")

    result_df.drop(columns=["value_x", "value_y"], inplace=True)

    # resolve quality_rating
    result_df["quality_rating_id"] = result_df[
        ["quality_rating_id_x", "quality_rating_id_y"]
    ].min(axis=1)
    result_df.drop(columns=["quality_rating_id_x", "quality_rating_id_y"], inplace=True)

    return result_df


def add_vars(equation, calc_var_type, pathway_name):

    vars_to_add = equation.split("+")

    for i, var_name in enumerate(vars_to_add):
        var_data = get_data_for_calculations(var_name, calc_var_type, pathway_name)

        if i == 0:
            result = var_data
        else:
            result = solve_dfs(result, var_data, "+", calc_var_type)

    return result


def divide_vars(equation, calc_var_type, pathway_name):
    [var_1, var_2] = equation.split("/")

    df_var_1 = get_data_for_calculations(var_1, calc_var_type, pathway_name)
    df_var_2 = get_data_for_calculations(var_2, calc_var_type, pathway_name)

    result_df = solve_dfs(df_var_1, df_var_2, "/", calc_var_type)

    return result_df


def multiply_vars(equation, calc_var_type, pathway_name):
    [var_1, var_2] = equation.split("*")

    df_var_1 = get_data_for_calculations(var_1, calc_var_type, pathway_name)
    df_var_2 = get_data_for_calculations(var_2, calc_var_type, pathway_name)

    result_df = solve_dfs(df_var_1, df_var_2, "*", calc_var_type)

    return result_df


def perform_post_disagg_calculation(
    var_name: str, calc_var_type: str, pathway_name=None
) -> pd.DataFrame:
    def _calculate(_eq):
        if "/" in _eq:
            result_df = divide_vars(_eq, calc_var_type, pathway_name)

        elif "+" in _eq:
            result_df = add_vars(_eq, calc_var_type, pathway_name)

        elif "*" in _eq:
            result_df = multiply_vars(_eq, calc_var_type, pathway_name)

        else:
            result_df = get_data_for_calculations(_eq, calc_var_type, pathway_name)

        return result_df

    equation = get_col_values(
        "var_details",
        "post_disagg_calculation_eq_for_code",
        {"var_name": var_name},
    )

    eq_parts = equation.split("|")

    for i, eq_part in enumerate(eq_parts):
        if i == 0:
            result_df = _calculate(eq_part)
        else:
            operator = eq_part[0]
            _eq_part = eq_part[1:]

            if _eq_part.isdigit():
                if operator == "*":
                    result_df["value"] = result_df["value"] * float(_eq_part)
                elif operator == "/":
                    result_df["value"] = result_df["value"] / float(_eq_part)
                else:
                    raise ValueError("Unknown operation")
            else:
                _result_df = _calculate(_eq_part)
                result_df = solve_dfs(result_df, _result_df, operator, calc_var_type)

    var_detail_id = get_primary_key("var_details", {"var_name": var_name})
    result_df["var_detail_id"] = var_detail_id

    add_to_processed_data(result_df)
