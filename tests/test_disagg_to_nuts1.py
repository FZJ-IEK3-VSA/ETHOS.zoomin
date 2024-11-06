from zoomin.db_access import get_values
import numpy as np


def test_eucalc_var_disagg():

    nuts0_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_dhg_emissions_co2_heat_co_product_from_power') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS0')
        AND pathway = 'national'
        AND year = 2030"""
    )

    nuts1_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_dhg_emissions_co2_heat_co_product_from_power') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS1')
        AND pathway = 'national'
        AND year = 2030"""
    )

    assert nuts0_value == np.sum(nuts1_disagg_values)


def test_collected_var_disagg():

    nuts0_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'number_of_people_affected_by_natural_disasters') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS0')"""
    )

    nuts1_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'number_of_people_affected_by_natural_disasters') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS1')"""
    )

    assert nuts0_value == np.sum(nuts1_disagg_values)
