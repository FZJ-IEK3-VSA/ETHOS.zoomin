import os
import json
import pandas as pd
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
    "eucalc_tra_vehicle_fleet_freight_hdvm_phev_diesel_ei"
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


def get_collected_vars(spatial_level):
    """spatial_level: could be LAU, NUTS3, NUTS2, NUTS0 or with_post_disagg_calc"""

    if spatial_level in ["LAU", "NUTS3", "NUTS2", "NUTS0"]:
        original_resolution_id = db_access.get_primary_key("original_resolutions", 
                                                        {"original_resolution": spatial_level})
        sql_cmd = f"""SELECT var_name FROM var_details 
                    WHERE 
                        (var_name NOT LIKE 'eucalc_%%' AND 
                        var_name NOT LIKE 'cimp_%%' AND 
                        var_name NOT LIKE 'cproj_%%')
                        AND original_resolution_id={original_resolution_id}
                        AND post_disagg_calculation_eq_for_code IS NULL;"""
        
    else:
        sql_cmd = f"""SELECT var_name FROM var_details 
                    WHERE 
                        (var_name NOT LIKE 'eucalc_%%' AND 
                        var_name NOT LIKE 'cimp_%%' AND 
                        var_name NOT LIKE 'cproj_%%')
                        AND post_disagg_calculation_eq_for_code IS NOT NULL;"""
        

    var_names = db_access.get_table(sql_cmd)

    return_list = var_names["var_name"]

    if (mini_db == 1) & (
        spatial_level not in ["LAU", "NUTS3"]
    ):  # if its LAU or NUTS3, we need all variables for random forest
        return_list = list(
            set(return_list).intersection(set(collected_vars_for_mini_db))
        )

    return return_list


def get_eucalc_pathways():
    pathways = list(db_access.get_col_values("pathways", "pathway_file_name"))
    return pathways


def get_eucalc_vars(var_type):

    if var_type == "disagg":
        sql_cmd = f"""SELECT var_name FROM var_details 
                    WHERE 
                    var_name LIKE 'eucalc_%%' 
                    AND post_disagg_calculation_eq_for_code IS NULL;"""

    else:
        sql_cmd = f"""SELECT var_name FROM var_details 
                    WHERE 
                    var_name LIKE 'eucalc_%%' 
                    AND post_disagg_calculation_eq_for_code IS NOT NULL;"""

    var_names = db_access.get_table(sql_cmd)

    return_list = var_names["var_name"]

    if mini_db == 1:
        return_list = list(set(return_list).intersection(set(eucalc_vars_for_mini_db)))

    return return_list


def save_predictor_df(spatial_level):
    lau_region_ids = tuple(
        db_access.get_col_values("regions", "id", {"resolution": "LAU"})
    )

    with open(
        os.path.join(os.path.dirname(__file__), "..", "data", f"predictor_vars_{spatial_level}.json")
    ) as f:
        predictor_vars = tuple(json.load(f))

    climate_experiment_id  = db_access.get_primary_key("climate_experiments", {"climate_experiment": "RCP4.5"})

    final_df = None
    for var_name in predictor_vars:
        print(var_name)
        if "cproj_" in var_name:
            sql_cmd = f"""SELECT v.var_name, d.value, r.region_code
                FROM processed_data d
                JOIN regions r ON d.region_id = r.id
                JOIN var_details v ON d.var_detail_id = v.id
                WHERE region_id IN {lau_region_ids}
                AND d.year=2020
                AND d.climate_experiment_id={climate_experiment_id}
                AND v.var_name = '{var_name}';"""

        else:
            sql_cmd = f"""SELECT v.var_name, d.value, r.region_code
                FROM processed_data d
                JOIN regions r ON d.region_id = r.id
                JOIN var_details v ON d.var_detail_id = v.id
                WHERE region_id IN {lau_region_ids}
                AND v.var_name = '{var_name}';"""
        
        predictor_df = db_access.get_table(sql_cmd)

        predictor_df.drop(columns=["var_name"], inplace=True)
        predictor_df.rename(columns={"value": var_name}, inplace=True)

        if final_df is None:
            final_df = predictor_df
        else:
            final_df = pd.merge(final_df, predictor_df, on="region_code", how="inner")

    final_df.to_csv(
        os.path.join(
            os.path.dirname(__file__), "..", "data", f"predictor_df_for_{spatial_level}.csv"
        ),
        index=False,
    )
    
