from zoomin.db_access import get_values
import pytest
import numpy as np


def test_nuts0_data_copy():
    agg_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'percentage_of_people_with_access_to_electricity');"""
    )

    assert isinstance(agg_value, float)


@pytest.mark.parametrize(
    "region",
    [
        ("ITC"),
        ("IT"),
    ],
)
def test_nuts2_data_aggregation(region):
    agg_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'gross_value_added_growth') 
        AND region_id = (SELECT id FROM regions WHERE region_code = '{region}')"""
    )

    nuts2_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'gross_value_added_growth') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE '{region}%%' AND resolution = 'NUTS2')"""
    )

    assert agg_value == np.mean(nuts2_values).round(5)


@pytest.mark.parametrize(
    "region",
    [
        ("ITH3"),
        ("ITC"),
        ("IT"),
    ],
)
def test_nuts3_data_aggregation(region):
    agg_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'employment_in_nace_sector_g_i') 
        AND region_id = (SELECT id FROM regions WHERE region_code = '{region}')"""
    )

    nuts3_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'employment_in_nace_sector_g_i') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE '{region}%%' AND resolution = 'NUTS3')"""
    )

    assert agg_value == np.sum(nuts3_values)


@pytest.mark.parametrize(
    "region",
    [
        ("ITH34"),
        ("ITH3"),
        ("ITC"),
        ("IT"),
    ],
)
def test_lau_data_aggregation(region):
    agg_value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'agriculture_with_natural_vegetation_cover') 
        AND region_id = (SELECT id FROM regions WHERE region_code = '{region}')"""
    )

    lau_values = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'agriculture_with_natural_vegetation_cover') 
        AND region_id IN (SELECT id FROM regions WHERE region_code LIKE '{region}%%' AND resolution = 'LAU')"""
    )

    assert agg_value == np.sum(lau_values).round(5)
