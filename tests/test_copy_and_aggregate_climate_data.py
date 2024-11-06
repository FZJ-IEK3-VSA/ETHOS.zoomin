from zoomin.db_access import get_values
import numpy as np
import pytest


@pytest.mark.parametrize(
    "region",
    [
        ("ITH3"),
        ("ITC"),
        ("IT"),
    ],
)
def test_climate_projection_data_aggregation(region):
    agg_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cproj_annual_mean_maximum_temperature') 
        AND region_id = (SELECT id FROM regions WHERE region_code = '{region}') 
        AND climate_experiment = 'RCP4.5'
        AND year = 2020"""
    )

    nuts3_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cproj_annual_mean_maximum_temperature') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE '{region}%%' AND resolution = 'NUTS3')
        AND climate_experiment = 'RCP4.5'
        AND year = 2020"""
    )

    assert agg_value == np.mean(nuts3_values).round(5)


@pytest.mark.parametrize(
    "region",
    [
        ("ITH3"),
        ("ITC"),
        ("IT"),
    ],
)
def test_climate_impact_data_aggregation(region):
    agg_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cimp_time_frame_of_heatwaves_mean') 
        AND region_id = (SELECT id FROM regions WHERE region_code = '{region}') 
        AND climate_experiment = 'RCP4.5'"""
    )

    nuts3_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'cimp_time_frame_of_heatwaves_mean') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE '{region}%%' AND resolution = 'NUTS3')
        AND climate_experiment = 'RCP4.5'"""
    )

    assert agg_value == np.max(nuts3_values)
