import os
import pandas as pd
from sklearn.feature_selection import VarianceThreshold
from dotenv import find_dotenv, load_dotenv
from zoomin import db_access

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

mini_db = int(os.environ.get("MINI_DB"))

collected_vars_for_mini_db = [
    "population",
    "employment",
    "gross_value_added",
    "gross_value_added_nace_sector_a",
    "gross_domestic_product",
    "relative_gross_value_added_nace_sector_a",
    "foreign_born_population",
    "statistical_area",
    "heat_production_with_lignite",
    "heat_production_with_coal",
    "heat_production_with_natural_gas",
    "number_of_dairy_cows",
    "live_births",
    "active_citizenship",
    "dump_sites_cover",
    "plastic_waste",
    "people_at_risk_of_poverty_or_social_exclusion",
    "income_of_households",
    "non_residential_energy_demand_space_cooling",
    "people_with_tertiary_education",
    "number_of_people_affected_by_natural_disasters",
    "deaths",
]

eucalc_vars_for_mini_db = [
    "eucalc_ind_material_production_cement_wet_kiln",
    "eucalc_ind_energy_demand_cement_electricity",
    "eucalc_ind_energy_demand_cement_gas_bio",
    "eucalc_ind_energy_demand_cement_gas_ff_natural",
    "eucalc_ind_energy_demand_cement_hydrogen",
    "eucalc_ind_energy_demand_cement_liquid_bio",
    "eucalc_ind_energy_demand_cement_liquid_ff_oil",
    "eucalc_ind_energy_demand_cement_solid_bio",
    "eucalc_ind_energy_demand_cement_solid_ff_coal",
    "eucalc_ind_energy_demand_cement_solid_waste",
    "eucalc_ind_energy_demand_cement_total",
    "eucalc_dhg_emissions_co2_heat_co_product_from_power",
    "eucalc_ccu_capex_depleted_oil_gas_reservoirs",
    "eucalc_ind_energy_demand_cement_gas_ff_natural_ei",
    "eucalc_bld_energy_demand_non_residential_appliances_electricity",
    "eucalc_ccu_capex_unmineable_coal_seams",
    "eucalc_elc_old_capacity_fossil_coal",
]


def get_climate_vars():
    return_list = []

    if mini_db == 1:
        years = ["2020", "2099"]
    else:
        years = [str(i) for i in range(2020, 2100)]

    sql_cmd = f"""SELECT var_name FROM var_details 
                WHERE 
                    var_name LIKE 'cimp_%%'
                    OR var_name LIKE 'cproj_%%'"""

    var_names = db_access.get_table(sql_cmd)

    for var_name in var_names["var_name"]:

        for year in years:
            if "cproj_" in var_name:
                for year in years:
                    return_list.append(f"{var_name}-{year}")
            else:
                return_list.append(var_name)

    return return_list


def get_collected_vars(priority_level):

    sql_cmd = f"""SELECT var_name FROM var_details 
                WHERE 
                    (var_name NOT LIKE 'eucalc_%%' AND 
                    var_name NOT LIKE 'cimp_%%' AND 
                    var_name NOT LIKE 'cproj_%%')
                    AND priority_level={priority_level}"""

    var_names = db_access.get_table(sql_cmd)

    return_list = var_names["var_name"]

    if (mini_db == 1) & (
        priority_level not in [1, 2]
    ):  # if its LAU or NUTS3, we need all variables for random forest
        return_list = list(
            set(return_list).intersection(set(collected_vars_for_mini_db))
        )

    return return_list


def get_pathways():
    pathways = list(db_access.get_col_values("pathways", "pathway_file_name"))
    if mini_db == 1:
        return [pathway for pathway in pathways if pathway.startswith("pt")]
    else:
        return [pathway for pathway in pathways if pathway.startswith("de")]


def get_eucalc_vars(priority_level):

    sql_cmd = f"""SELECT var_name FROM var_details 
                WHERE 
                var_name LIKE 'eucalc_%%' 
                AND priority_level={priority_level}"""

    var_names = db_access.get_table(sql_cmd)

    return_list = var_names["var_name"]

    if mini_db == 1:
        return_list = list(set(return_list).intersection(set(eucalc_vars_for_mini_db)))

    return return_list


def save_predictor_df_for_nuts3():
    lau_region_ids = tuple(
        db_access.get_col_values("regions", "id", {"resolution": "LAU"})
    )

    non_point_vars = (
        "percentage_of_people_very_satisfied_with_public_transport",
        "percentage_of_people_rather_satisfied_with_public_transport",
        "percentage_of_people_rather_unsatisfied_with_public_transport",
        "percentage_of_people_not_at_all_satisfied_with_public_transport",
        "percentage_of_people_with_unknown_satifactory_level_with_public_transport",
    )

    sql_cmd = f"""SELECT v.var_name, d.value, r.region_code
                FROM processed_data d
                JOIN regions r ON d.region_id = r.id
                JOIN var_details v ON d.var_detail_id = v.id
                WHERE region_id IN {lau_region_ids}
                AND v.var_name NOT IN {non_point_vars}
                AND v.var_name NOT LIKE 'cimp_%%' 
                AND v.var_name NOT LIKE 'cproj_%%';"""

    lau_df = db_access.get_table(sql_cmd)

    x_df = None
    for var_name, sub_df in lau_df.groupby("var_name"):
        sub_df.drop(columns=["var_name"], inplace=True)
        sub_df.rename(columns={"value": var_name}, inplace=True)

        if x_df is None:
            x_df = sub_df
        else:
            x_df = pd.merge(x_df, sub_df, on="region_code", how="inner")

    # Isolate the 'region_code' column from data
    region_code_x = x_df["region_code"]
    x_df_numeric = x_df.drop("region_code", axis=1)

    # REMOVE 0 Variance data
    # Create VarianceThreshold object
    selector = VarianceThreshold()

    # Fit to data and transform the data
    x_df_reduced = selector.fit_transform(x_df_numeric)

    # Convert the result back to a DataFrame
    # This step is necessary because the output of VarianceThreshold is a NumPy array
    x_df_reduced = pd.DataFrame(
        x_df_reduced, columns=x_df_numeric.columns[selector.get_support()]
    )

    x_df_reduced["region_code"] = region_code_x
    x_df_reduced = x_df_reduced.reindex(sorted(x_df_reduced.columns), axis=1)

    x_df_reduced.to_csv(
        os.path.join(
            os.path.dirname(__file__), "..", "data", "predictor_df_for_NUTS3.csv"
        ),
        index=False,
    )


def save_predictor_df_for_nuts2():
    lau_region_ids = tuple(
        db_access.get_col_values("regions", "id", {"resolution": "LAU"})
    )
    # non-climate vars
    non_point_vars = (
        "percentage_of_people_very_satisfied_with_public_transport",
        "percentage_of_people_rather_satisfied_with_public_transport",
        "percentage_of_people_rather_unsatisfied_with_public_transport",
        "percentage_of_people_not_at_all_satisfied_with_public_transport",
        "percentage_of_people_with_unknown_satifactory_level_with_public_transport",
    )

    sql_cmd = f"""SELECT v.var_name, d.value, r.region_code
                FROM processed_data d
                JOIN regions r ON d.region_id = r.id
                JOIN var_details v ON d.var_detail_id = v.id
                WHERE region_id IN {lau_region_ids}
                AND v.var_name NOT IN {non_point_vars}
                AND v.var_name NOT LIKE 'cimp_%%' 
                AND v.var_name NOT LIKE 'cproj_%%';"""

    lau_df = db_access.get_table(sql_cmd)

    x_df = None
    for var_name, sub_df in lau_df.groupby("var_name"):
        sub_df.drop(columns=["var_name"], inplace=True)
        sub_df.rename(columns={"value": var_name}, inplace=True)

        if x_df is None:
            x_df = sub_df
        else:
            x_df = pd.merge(x_df, sub_df, on="region_code", how="inner")

    # climate vars
    exclude_vars = (
        "cproj_annual_mean_temperature_cooling_degree_days",
        "cproj_annual_minimum_temperature_cooling_degree_days",
    )

    climate_experiment_id = db_access.get_primary_key(
        "climate_experiments", {"climate_experiment": "RCP4.5"}
    )

    sql_cmd = f"""SELECT v.var_name, d.value, r.region_code
                FROM processed_data d
                JOIN regions r ON d.region_id = r.id
                JOIN var_details v ON d.var_detail_id = v.id
                WHERE region_id IN {lau_region_ids}
                AND (v.var_name LIKE 'cimp_%%' 
                OR v.var_name LIKE 'cproj_%%')
                AND v.var_name NOT IN {exclude_vars}
                AND d.year=2020
                AND d.climate_experiment_id={climate_experiment_id};"""

    lau_df = db_access.get_table(sql_cmd)

    for var_name, sub_df in lau_df.groupby("var_name"):
        sub_df.drop(columns=["var_name"], inplace=True)
        sub_df.rename(columns={"value": var_name}, inplace=True)

        x_df = pd.merge(x_df, sub_df, on="region_code", how="inner")

    # Isolate the 'region_code' column from data
    region_code_x = x_df["region_code"]
    x_df_numeric = x_df.drop("region_code", axis=1)

    # REMOVE 0 Variance data
    # Create VarianceThreshold object
    selector = VarianceThreshold()

    # Fit to data and transform the data
    x_df_reduced = selector.fit_transform(x_df_numeric)

    # Convert the result back to a DataFrame
    # This step is necessary because the output of VarianceThreshold is a NumPy array
    x_df_reduced = pd.DataFrame(
        x_df_reduced, columns=x_df_numeric.columns[selector.get_support()]
    )

    x_df_reduced["region_code"] = region_code_x
    x_df_reduced = x_df_reduced.reindex(sorted(x_df_reduced.columns), axis=1)

    x_df_reduced.to_csv(
        os.path.join(
            os.path.dirname(__file__), "..", "data", "predictor_df_for_NUTS2.csv"
        ),
        index=False,
    )
