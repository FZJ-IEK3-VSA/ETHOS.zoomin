import os
from dotenv import find_dotenv, load_dotenv
from zoomin import db_access
from zoomin.db_access import with_db_connection

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

mini_db = int(os.environ.get("MINI_DB"))
db_name = os.environ.get("DB_NAME")

collected_vars_for_mini_db = [
    "relative_gross_value_added_nace_sector_a",
    "road_transport_of_freight",
    "foreign_born_population",
    "active_citizenship",
    "plastic_waste",
    "percentage_of_people_at_risk_of_poverty_or_social_exclusion",
    "income_of_households",
    "non_residential_energy_demand_space_cooling",
    "people_with_tertiary_education",
    "number_of_people_affected_by_natural_disasters",
    "air_transport_of_freight",
    "maritime_transport_of_freight",
    "electricity_prices_for_household_consumers",
    "population",
    "statistical_area",
    "heat_demand_residential",
    "heat_demand_non_residential",
    "gross_value_added_nace_sector_a",
    "gross_value_added",
    "road_network",
    "permanently_irrigated_land_cover",
    "rice_fields_cover",
    "vineyards_cover",
    "fruit_trees_and_berry_plantations_cover",
    "olive_groves_cover",
    "pastures_cover",
    "permanent_crops_cover",
    "complex_cultivation_patterns_cover",
    "agriculture_with_natural_vegetation_cover",
    "agro_forestry_areas_cover",
    "number_of_cattle",
    "deaths",
    "number_of_non_ferrous_metals_industries",
    "number_of_chemical_industries",
]

eucalc_vars_for_mini_db = [
    "eucalc_agr_energy_demand_liquid_ff_gasoline_ei",
    "eucalc_agr_input_use_emissions_co2_fuel",
    "eucalc_agr_energy_demand_liquid_ff_gasoline",
    "eucalc_agr_energy_demand_gas_ff_natural",
    "eucalc_agr_energy_demand_liquid_ff_diesel",
    "eucalc_agr_energy_demand_liquid_ff_fuel_oil",
    "eucalc_agr_energy_demand_liquid_ff_gasoline",
    "eucalc_agr_energy_demand_liquid_ff_lpg",
    "eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei",
    "eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk",
    "eucalc_agr_domestic_production_liv_abp_dairy_milk",
    "eucalc_tra_emissions_co2e_freight_total",
    "eucalc_tra_emissions_co2e_freight_hdv",
    "eucalc_tra_emissions_co2e_freight_iww",
    "eucalc_tra_emissions_co2e_freight_aviation",
    "eucalc_tra_emissions_co2e_freight_marine",
    "eucalc_tra_emissions_co2e_freight_rail",
    "eucalc_elc_energy_production_res_bio_gas_ei",
    "eucalc_elc_emissions_co2e_res_bio",
    "eucalc_elc_energy_production_res_bio_gas",
    "eucalc_elc_energy_production_res_bio_mass",
    "eucalc_dhg_emissions_co2_heat_co_product_from_power",
    "eucalc_ccu_capex_depleted_oil_gas_reservoirs",
    "eucalc_ccu_capex_unmineable_coal_seams",
    "eucalc_elc_old_capacity_fossil_coal",
    "eucalc_bld_emissions_co2_non_residential_hw_electricity",
    "eucalc_ind_energy_demand_copper_electricity",
    "eucalc_ind_energy_demand_chemicals_liquid_ff_oil",
]

climate_vars_for_mini_db = [
    "cproj_annual_mean_temperature_cooling_degree_days",
    "cimp_historical_probability_of_very_high_fire_risk_mean",
]


def get_climate_vars():

    if mini_db == 1:
        years = ["2020", "2099"]
    else:
        years = [str(i) for i in range(2020, 2100)]

    if mini_db == 1:
        var_list = climate_vars_for_mini_db
    else:
        sql_cmd = f"""SELECT var_name FROM var_details 
                    WHERE 
                        var_name LIKE 'cimp_%%'
                        OR var_name LIKE 'cproj_%%'"""

        var_list = db_access.get_values(sql_cmd)

    return_list = []
    for var_name in var_list:
        for year in years:
            if "cproj_" in var_name:
                for year in years:
                    return_list.append(f"{var_name}-{year}")
            else:
                return_list.append(var_name)

    return return_list


def get_collected_vars(spatial_level):
    """spatial_level: could be LAU, NUTS3, NUTS2, or NUTS0"""

    sql_cmd = f"""SELECT var_name FROM var_details 
                WHERE 
                    (var_name NOT LIKE 'eucalc_%%' AND 
                    var_name NOT LIKE 'cimp_%%' AND 
                    var_name NOT LIKE 'cproj_%%')
                    AND original_resolution_id=(SELECT id FROM original_resolutions WHERE original_resolution = '{spatial_level}')
                    AND post_disagg_calculation_eq IS NULL;"""

    return_list = db_access.get_values(sql_cmd)

    if mini_db == 1:
        return_list = list(
            set(return_list).intersection(set(collected_vars_for_mini_db))
        )

    return return_list


def get_eucalc_vars():
    sql_cmd = f"""SELECT var_name FROM var_details 
                WHERE 
                var_name LIKE 'eucalc_%%' 
                AND post_disagg_calculation_eq IS NULL;"""

    return_list = db_access.get_values(sql_cmd)

    if mini_db == 1:
        return_list = list(set(return_list).intersection(set(eucalc_vars_for_mini_db)))

    return return_list


def get_post_disagg_calc_vars():

    sql_cmd = f"""SELECT var_name FROM var_details 
                WHERE post_disagg_calculation_eq IS NOT NULL;"""

    return_list = db_access.get_values(sql_cmd)

    if mini_db == 1:
        return_list = list(
            set(return_list).intersection(
                set(collected_vars_for_mini_db).union(set(eucalc_vars_for_mini_db))
            )
        )

    return return_list


@with_db_connection()
def clear_rows_from_processed_data(
    cursor, var_name, target_resolution=None, year=None, pathway=None
):
    if "cproj_" in var_name:
        [var_name, year] = var_name.split("-")

    sql_cmd = f"""DELETE FROM processed_data WHERE 
                    var_detail_id = (SELECT id FROM var_details WHERE var_name = '{var_name}')"""

    if target_resolution is not None:
        sql_cmd = f"{sql_cmd} AND region_id IN (SELECT id FROM regions WHERE resolution = '{target_resolution}')"

    if year is not None:
        sql_cmd = f"{sql_cmd} AND year = year"

    if pathway is not None:
        sql_cmd = f"{sql_cmd} AND pathway = '{pathway}'"

    cursor.execute(sql_cmd)
