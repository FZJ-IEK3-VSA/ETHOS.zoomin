from zoomin.db_access import get_values
import numpy as np


def test_eucalc_var_disagg():

    nuts0_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_agr_input_use_emissions_co2_fuel') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS0')
        AND pathway = 'national'
        AND year = 2030"""
    )

    nuts2_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_agr_input_use_emissions_co2_fuel') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS2')
        AND pathway = 'national'
        AND year = 2030"""
    )

    assert np.round(nuts0_value, 5) == np.sum(nuts2_disagg_values).round(5)


def test_collected_var_disagg_from_nuts0_to_nuts2():

    nuts0_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'non_residential_energy_demand_space_cooling') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS0')"""
    )

    nuts2_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'non_residential_energy_demand_space_cooling') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS2')"""
    )

    assert np.round(nuts0_value, 5) == np.sum(nuts2_disagg_values).round(5)
