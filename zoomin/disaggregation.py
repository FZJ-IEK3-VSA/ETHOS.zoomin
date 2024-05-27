"""Functions to help disaggregate values to LAU and populate DB with data."""
import os
import numpy as np
import pandas as pd

from zoomin.db_access import (
    get_regions,
    get_col_values,
    add_to_processed_data,
    get_primary_key,
    get_table,
)
from zoomin import disaggregation_utils as disagg_utils

# number of chars to consider based on a resolution
char_dict = {"NUTS3": 5, "NUTS2": 4, "NUTS1": 3, "NUTS0": 2}


def get_relative_spatial_levels(spatial_level, spatial_direction):
    hierarchy = ["NUTS0", "NUTS1", "NUTS2", "NUTS3", "LAU"]

    level_index = hierarchy.index(spatial_level)

    if spatial_direction == "down":
        return hierarchy[
            level_index + 1 : -1
        ]  # LAU is not considered, because it is the level to which disaggregation happens
    elif spatial_direction == "up":
        return hierarchy[:level_index]


def aggregate_data(var_data, var_name, source_resolution, data_type):
    def _perform_aggregation(data_group):
        # aggregate the value
        if agg_method == "sum":
            agg_val = data_group["value"].sum()
        elif agg_method == "mean":
            agg_val = data_group["value"].mean()
        elif agg_method == "max":
            agg_val = data_group["value"].max()
        else:
            raise ValueError("Unknown var aggregation method")

        data_dict = {
            "value": agg_val,
            "var_detail_id": data_group["var_detail_id"].unique().item(),
            "region_code": data_group["region_code"].unique().item(),
            "proxy_detail_id": data_group["proxy_detail_id"].unique().item(),
        }

        # Calculate quality rating
        ## get the most repeated quality_rating
        agg_quality_rating = data_group["quality_rating_id"].value_counts().idxmax()
        data_dict.update({"quality_rating_id": agg_quality_rating})

        ## year
        if len(data_group["year"].unique()) == 1:
            data_dict.update({"year": data_group["year"].unique().item()})

        else:
            agg_year = (
                data_group["year"].value_counts().idxmax()
            )  # taking the most repeated year in case of mixed years
            data_dict.update({"year": agg_year})

        if "climate_experiment_id" in data_group.columns:
            data_dict.update(
                {
                    "climate_experiment_id": data_group["climate_experiment_id"]
                    .unique()
                    .item()
                }
            )

        if "pathway_id" in data_group.columns:
            data_dict.update({"pathway_id": data_group["pathway_id"].unique().item()})

        return pd.Series(data_dict)

    if data_type == "disaggregated_data":
        spatial_direction = "down"
    else:
        spatial_direction = "up"

    agg_resolutions = get_relative_spatial_levels(source_resolution, spatial_direction)

    agg_method = get_col_values(
        "var_details", "var_aggregation_method", {"var_name": var_name}
    )

    if len(agg_resolutions) > 0:
        agg_df_list = []
        for agg_resolution in agg_resolutions:
            agg_df = var_data.copy(deep=True)

            n_char = char_dict[agg_resolution]
            agg_df["region_code"] = agg_df["region_code"].str[:n_char]

            group_vars = ["region_code"]

            if "climate_experiment_id" in var_data.columns:
                group_vars.append("climate_experiment_id")

            if "pathway_id" in var_data.columns:
                group_vars.append("pathway_id")

            agg_df = agg_df.groupby(group_vars).apply(_perform_aggregation)

            agg_df.reset_index(drop=True, inplace=True)

            ## replace region_code with region_id
            regions_df = get_regions(agg_resolution)

            agg_df = pd.merge(
                agg_df,
                regions_df,
                on="region_code",
                how="left",
            )

            agg_df.rename(columns={"id": "region_id"}, inplace=True)
            agg_df.drop(
                columns=[
                    "region_code",
                ],
                inplace=True,
            )

            agg_df_list.append(agg_df)
        
        final_agg_df = pd.concat(agg_df_list)
        add_to_processed_data(final_agg_df)


def distribute_data_equally(
    var_data, var_name, source_resolution, disaggregation_quality_rating
):
    # TODO: docstrings
    # STEP1: Disaggregate
    regions_df = get_regions("LAU")

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
    ## Calculate final quality_rating_id by taking the minimum between quality_rating_id of values
    ## and the disaggregation_quality_rating
    lau_db_df = final_df.copy(deep=True)
    lau_db_df.drop(
        columns=[
            "region_code_x",
            "region_code_y",
            "match_region_code",
        ],
        inplace=True,
    )
    lau_db_df.rename(columns={"id": "region_id"}, inplace=True)

    lau_db_df["quality_rating_id"] = np.minimum(
        lau_db_df["quality_rating_id"], disaggregation_quality_rating
    )

    add_to_processed_data(lau_db_df)

    # STEP 2: Aggregate disaggregated data till source_resolution-1 spatial level
    # - reason: quality rating depends on proxy and proxy data
    data_to_agg = final_df.copy(deep=True)
    data_to_agg.drop(
        columns=["region_code_y", "match_region_code", "id"],
        inplace=True,
    )
    data_to_agg.rename(columns={"region_code_x": "region_code"}, inplace=True)

    aggregate_data(data_to_agg, var_name, source_resolution, "disaggregated_data")


def perform_random_forest_based_disaggregation(
    var_name,
    data_to_disagg,
    rf_model,
    disagg_proxy,
    source_resolution,
    disaggregation_quality_rating,
):
    # TODO: docstrings
    # STEP1: Disaggregate
    # predict values as LAU
    file_name = f"predictor_df_for_{source_resolution}.csv"
    predictor_df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "..", "data", file_name)
    )

    region_code = predictor_df["region_code"].values
    predictor_df.drop(columns="region_code", inplace=True)

    y_pred = rf_model.predict(predictor_df)

    disagg_df = pd.DataFrame({"region_code": region_code, "value": y_pred})

    # assess quality rating of predictor data (based on top 3 predictors)
    lau_df = get_regions("LAU")
    lau_region_ids = tuple(lau_df["id"])

    top_3_predictors = disagg_proxy.split(": ")[1].split(", ")
    predictor_qr_df_final = None
    for predictor in top_3_predictors:
        predictor_var_detail_id = get_primary_key(
            "var_details", {"var_name": predictor}
        )

        if var_name.startswith("cproj_"):

            climate_experiment_id = get_primary_key(
                "climate_experiments", {"climate_experiment": "RCP2.6"}
            )
            sql_cmd = f"""r.region_code, d.quality_rating_id
                        FROM processed_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE var_detail_id = {predictor_var_detail_id}
                            AND r.id in {lau_region_ids}
                            AND year=2020 
                            AND climate_experiment_id={climate_experiment_id}"""
        else:
            sql_cmd = f"""SELECT r.region_code, d.quality_rating_id
                        FROM processed_data d
                        JOIN regions r ON d.region_id = r.id
                        WHERE var_detail_id = {predictor_var_detail_id}
                            AND r.id in {lau_region_ids}
            """

        predictor_qr_df = get_table(sql_cmd)

        if predictor_qr_df_final is None:
            predictor_qr_df_final = predictor_qr_df
        else:
            predictor_qr_df_final = pd.merge(
                predictor_qr_df_final, predictor_qr_df, on="region_code", how="outer"
            )

    predictor_qr_df_final["quality_rating_id"] = predictor_qr_df_final[
        ["quality_rating_id_x", "quality_rating_id_y", "quality_rating_id"]
    ].min(axis=1)

    predictor_qr_df_final.drop(
        columns=["quality_rating_id_x", "quality_rating_id_y"], inplace=True
    )

    disagg_df = pd.merge(disagg_df, predictor_qr_df_final, on="region_code", how="left")

    n_char = char_dict[source_resolution]
    disagg_df["match_region_code"] = disagg_df["region_code"].str[:n_char]

    data_to_disagg.rename(columns={"region_code": "match_region_code"}, inplace=True)

    # prepare final df with final quality rating
    final_df = pd.merge(data_to_disagg, disagg_df, on="match_region_code", how="left")

    ## quality rating - minimum of source and target data
    final_df["quality_rating_id"] = final_df[
        ["quality_rating_id_x", "quality_rating_id_y"]
    ].min(axis=1)

    ## final quality rating - minimum of above and disaggregation_quality_rating
    final_df["quality_rating_id"] = np.minimum(
        final_df["quality_rating_id"], disaggregation_quality_rating
    )

    final_df.drop(
        columns=["match_region_code", "quality_rating_id_x", "quality_rating_id_y"],
        inplace=True,
    )

    final_df = pd.merge(final_df, lau_df, on="region_code", how="left")

    final_df.rename(columns={"id": "region_id"}, inplace=True)
    # TODO: the sum of values, per parent region, in the final_df should be equal to the parent region
    # TODO: the values should be integers for integer type data . For example: population
    lau_db_df = final_df.copy(deep=True)
    lau_db_df.drop(columns="region_code", inplace=True)

    add_to_processed_data(lau_db_df)

    # STEP 2: Aggregate disaggregated data till source_resolution-1 spatial level
    # - reason: quality rating depends on proxy and proxy data
    aggregate_data(final_df, var_name, source_resolution, "disaggregated_data")


def perform_proxy_based_disaggregation(
    var_data,
    var_name,
    source_resolution,
    disagg_proxy,
    disagg_binary_criteria,
    disaggregation_quality_rating
):
    # TODO: docstrings
    # STEP1: Disaggregate
    proxy_data = disagg_utils.solve_proxy_equation(disagg_proxy)
    
    if len(proxy_data) == 0:
        raise ValueError("Proxy data not found in the database.")

    proxy_data = disagg_utils.match_source_target_resolutions(
        source_resolution, proxy_data
    )

    if isinstance(disagg_binary_criteria, str):
        proxy_data = disagg_utils.apply_binary_disaggregation_criteria(
            proxy_data, disagg_binary_criteria
        )

    final_df, is_bad_proxy_list = disagg_utils.disaggregate_data(
        var_data, proxy_data, disaggregation_quality_rating
    )

    lau_db_df = final_df.copy(deep=True)
    lau_db_df.drop(columns=["region_code", "match_region_code"], inplace=True)

    # TODO: the values should be integers for integer type data . For example: population
    add_to_processed_data(lau_db_df)

    # STEP 2: Aggregate disaggregated data till source_resolution-1 spatial level
    # - reason: quality rating depends on proxy and proxy data
    disagg_db_df = final_df.copy(deep=True)
    disagg_db_df.drop(columns=["match_region_code"], inplace=True)

    aggregate_data(disagg_db_df, var_name, source_resolution, "disaggregated_data")

    if any(is_bad_proxy_list):
        return "bad_proxy"
