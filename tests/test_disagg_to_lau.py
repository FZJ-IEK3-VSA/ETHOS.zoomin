from zoomin.db_access import get_values
import numpy as np


def test_eucalc_var_disagg():

    nuts0_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_tra_emissions_co2e_freight_hdv') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'NUTS0')
        AND pathway = 'national'
        AND year = 2030"""
    )

    lau_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_tra_emissions_co2e_freight_hdv') 
        AND region_id IN (SELECT id FROM regions WHERE resolution = 'LAU')
        AND pathway = 'national'
        AND year = 2030"""
    )

    assert np.round(nuts0_value, 5) == np.sum(lau_disagg_values).round(5)


def test_climate_projection_var_disagg_from_nuts3_to_lau():

    nuts3_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cproj_annual_mean_temperature_cooling_degree_days') 
        AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF32')
        AND climate_experiment = 'RCP4.5'
        AND year = 2020"""
    )

    lau_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cproj_annual_mean_temperature_cooling_degree_days') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE 'ITF32%%')
        AND climate_experiment = 'RCP4.5'
        AND year = 2020"""
    )

    assert np.round(nuts3_value, 5) == np.mean(lau_disagg_values).round(5)


def test_climate_impact_var_disagg_from_nuts3_to_lau():

    nuts3_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cimp_historical_probability_of_very_high_fire_risk_mean') 
        AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF32')"""
    )

    lau_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cimp_historical_probability_of_very_high_fire_risk_mean') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE 'ITF32%%')"""
    )

    assert np.round(nuts3_value, 5) == np.mean(lau_disagg_values).round(5)


def test_collected_var_disagg_from_nuts0_to_lau():

    nuts3_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'non_residential_energy_demand_space_cooling') 
        AND region_id = (SELECT id FROM regions WHERE resolution = 'NUTS0')"""
    )

    lau_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'non_residential_energy_demand_space_cooling') 
         AND region_id IN (SELECT id FROM regions WHERE resolution  = 'LAU')"""
    )

    assert np.round(nuts3_value, 5) == np.sum(lau_disagg_values).round(5)


def test_collected_var_disagg_from_nuts2_to_nuts3():

    nuts2_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'air_transport_of_freight') 
        AND region_id = (SELECT id FROM regions WHERE region_code = 'ITG2')"""
    )

    lau_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'air_transport_of_freight') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE 'ITG2%%' AND resolution = 'LAU')"""
    )

    assert np.round(nuts2_value, 5) == np.sum(lau_disagg_values).round(5)


def test_collected_var_disagg_from_nuts3_to_lau():

    nuts3_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'gross_value_added') 
        AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')"""
    )

    lau_disagg_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'gross_value_added') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE 'ITF34%%' AND resolution = 'LAU')"""
    )

    assert np.round(nuts3_value, 5) == np.sum(lau_disagg_values).round(5)
