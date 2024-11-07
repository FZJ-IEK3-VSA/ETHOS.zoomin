from zoomin.db_access import get_values
import numpy as np


def test_eucalc_var_disagg():

    nuts0_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS0')
        AND pathway = 'national'
        AND year = 2030"""
    )

    nuts3_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS3')
        AND pathway = 'national'
        AND year = 2030"""
    )

    assert np.round(nuts0_value, 5) == np.sum(nuts3_disagg_values).round(5)


def test_collected_var_disagg_from_nuts0_to_nuts3():

    nuts0_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'non_residential_energy_demand_space_cooling') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS0')"""
    )

    nuts3_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'non_residential_energy_demand_space_cooling') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS3')"""
    )

    assert np.round(nuts0_value, 5) == np.sum(nuts3_disagg_values).round(5)


def test_collected_var_disagg_from_nuts2_to_nuts3():

    nuts2_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'air_transport_of_freight') 
        AND region_id = (SELECT id FROM regions WHERE region_code = 'ITG2')"""
    )

    nuts3_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'air_transport_of_freight') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE 'ITG2%%' AND resolution = 'NUTS3')"""
    )

    assert np.round(nuts2_value, 5) == np.sum(nuts3_disagg_values).round(5)
