from zoomin import disaggregation_manager as disagg_manager
from zoomin.post_disagg_calculation import perform_post_disagg_calculation
from zoomin import snakemake_utils

def test_process_climate_projection():

    disagg_manager.process_climate_var('cproj_annual_mean_precipitation-2020')

def test_process_climate_impact():

    disagg_manager.process_climate_var('cimp_time_frame_of_heavy_precipitation_mean')

def test_process_collected_lau_var():

    disagg_manager.process_collected_var('port_areas_cover')

def test_process_collected_nuts3_with_rf():

    disagg_manager.process_collected_var('employment')

def test_process_collected_nuts2_with_rf():

    disagg_manager.process_collected_var('gross_value_added')

def test_process_collected_nuts0_no_rf():

    disagg_manager.process_collected_var('production_growth')

def test_process_collected_nuts0_no_rf():

    disagg_manager.process_collected_var('production_growth')

def test_add_gdp():

    disagg_manager.process_collected_var('gross_domestic_product')

def test_post_disagg_calc_of_collected_var():
    perform_post_disagg_calculation("relative_gross_value_added", "collected_var")

def test_years_of_life_lost_due_to_air_pollution():
    disagg_manager.process_collected_var('years_of_life_lost_due_to_air_pollution')

def test_road_transport_of_freight():
    disagg_manager.process_collected_var('road_transport_of_freight')

def test_save_predictor_df():
    snakemake_utils.save_predictor_df("NUTS2")
    
    
