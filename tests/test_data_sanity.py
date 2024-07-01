import pytest
import numpy as np
from zoomin.db_access import with_db_connection, get_table


@with_db_connection()
def get_values(cursor, sql_cmd):
    cursor.execute(sql_cmd)
    result = cursor.fetchall()
    result_list = [res[0] for res in result]
    return result_list


def test_categorical():
    output_values = get_values(
        "SELECT value FROM processed_data WHERE var_detail_id IN (SELECT id FROM var_details WHERE var_unit='(categorical)')"
    )
    allowed_values = {1, 2, 0, -1, -5, -10}
    assert set(output_values).issubset(allowed_values)


testdata = [
    ("cooling degree days", 0, 5000),  # Noah's message long ago
    (
        "heating degree days",
        0,
        9000,
    ),  # Noah's message long ago, adjusted based on errors: maximum in Italy data 8561.35
    ("degree celsius", -50, 50),  # temperatures EU
    ("Euro/kWh", 0.1, 0.4),  # Energy prices EU
    (
        "Euros",
        0,
        4297.19e3,
    ),  # SUM(income_of_households) raw data - reduced by a factor of 6
    ("hectare", 0, 160),  # area of organic farm area EU - reduced by a factor of 5
    ("index", 0, 100),  # best guess
    ("kilogram", 0, 2.2e3),  # waste in total in the EU - reduced by a factor of 6
    (
        "kilometer",
        0,
        642.85009,
    ),  # length of all the roads in the world - reduced by a factor of 5
    ("lsu", 0, 115e2),  # total livestock units in the EU - reduced by a factor of 4
    ("meter", 0, 3682),  # average ocean depth in the world
    ("million Euros", 0, 15e3),  # GDP of EU - reduced by a factor of 9
    (
        "mm",
        0,
        3000,
    ),  # highest annual precipitation in the EU, adjusted based on errors: maximum in Italy data 2612.95
    ("mm/day", 0, 700),  # highest mean precipitation in the EU
    (
        "Mt",
        0,
        59.72e2,
    ),  # min_value: guess work (some emissions such as biogenic are negative in EUCalc pathways), max_value: weight of the earth - reduced by a factor of 22
    (
        "Mt/square kilometer",
        0,
        143.76,  # division of units
    ),
    ("Mt/vehicle", 0, 40),  # maximum capacity of a vehicle in tons
    ("MtCO2-eq/TWh", 0, 0.383296),  # maximum emission factor (peat)
    (
        "MW",
        0,
        510,
    ),  # installed renewable capacity of the world - reduced by a factor of 3
    (
        "GW",
        0,
        5.10,
    ),  # installed renewable capacity of the world - reduced by a factor of 2
    ("MWh", 0, 10491),  # total energy consumption EU - reduced by a factor of 9
    (
        "TWh",
        0,
        10491,
    ),  # min_value: guess work (some values such as agr_bioenergy-demand_liquid_eth_cereal[TWh] are negative and the fuel_demand in paper and printing industries is sometimes negative. Donno why but that is how it is in the raw data),
    # max_value: total energy consumption EU - reduced by a factor of 3
    ("number", 0, 5e6),  # best guess for population of a NUTS3 region
    ("percentage", 0, 100),  # percentage!
    (
        "square kilometer",
        0,
        41.54,
    ),  # min_value: guess work (eucalc_bld_floor_area_demolished_exi has negative values), max_area: area of the netherlands
    (
        "ug/m3",
        0,
        113.9,
    ),  # max_value: (o3 level is substantially higher than PM2.5 and no2 in the raw data, need to check it out but the database is down)
    ("vehicle-km/year", 0, 200000),  # approx maximum utilization rate in the data
    ("years", 0, 50),  # maximum vehicle lifetime in the data
    ("billion_EUR", 0, 10),  # guess work based on numbers in EUCalc pathways
    ("GJ", 0, 136568857),  # guess work based on numbers in EUCalc pathways
    ("kcal", 0, 292288363030810),  # guess work based on numbers in EUCalc pathways
    ("m3", 0, 58.2e3),  # total water consumption in the EU - reduced by a factor of 6
    (
        "million m3",
        0,
        58.2,
    ),  # total water consumption in the EU - reduced by a factor of 3
    ("Mt/GJ", 0, 4.372885686522221e17),  # above Mt/GJ numbers
    ("Mt/TWh", 0, 5.692498331903536e18),  # above Mt/TWh numbers
    ("pkm", 0, 87e3),  # total pkm in the EU (metro) - reduced by a factor of 6
]


@pytest.mark.parametrize("var_unit, expected_min, expected_max", testdata)
def test_ranges(var_unit, expected_min, expected_max):
    output_values = get_values(
        f"SELECT value FROM processed_data WHERE var_detail_id IN (SELECT id FROM var_details WHERE var_unit='{var_unit}') AND region_id IN (SELECT id FROM regions WHERE resolution='NUTS3');"
    )

    if len(output_values) > 0:
        output_min = np.min(output_values)
        output_max = np.max(output_values)

        assert output_min >= expected_min
        assert output_max <= expected_max

    else:
        print(f"nothing to check no values for unit {var_unit}")


def test_land_use_and_land_cover():
    var_names = (
        "continuous_urban_fabric_cover",
        "discontinuous_urban_fabric_cover",
        "industrial_or_commercial_units_cover",
        "road_and_rail_networks_cover",
        "port_areas_cover",
        "airports_cover",
        "mineral_extraction_sites_cover",
        "dump_sites_cover",
        "construction_sites_cover",
        "green_urban_areas_cover",
        "sport_and_leisure_facilities_cover",
        "non_irrigated_arable_land_cover",
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
        "broad_leaved_forest_cover",
        "coniferous_forest_cover",
        "mixed_forest_cover",
        "natural_grasslands_cover",
        "moors_and_heathland_cover",
        "sclerophyllous_vegetation_cover",
        "transitional_woodland_shrub_cover",
        "beaches_dunes_and_sand_cover",
        "bare_rocks_cover",
        "sparsely_vegetated_areas_cover",
        "burnt_areas_cover",
        "glaciers_and_perpetual_snow_cover",
        "inland_marshes_cover",
        "peat_bogs_cover",
        "salt_marshes_cover",
        "salines_cover",
        "intertidal_flats_cover",
        "water_courses_cover",
        "water_bodies_cover",
        "coastal_lagoons_cover",
        "estuaries_cover",
        "sea_and_ocean_cover",
        "land_use_no_data_cover",
    )

    # sum of NUTS3 values lesser than or equal to the area of Italy

    sql_cmd = f"""SELECT value FROM processed_data 
                    WHERE var_detail_id in (SELECT id FROM var_details WHERE var_name IN {var_names}) 
                    AND region_id IN (SELECT id FROM regions WHERE resolution='NUTS3');"""

    nuts3_df = get_table(sql_cmd)

    assert nuts3_df.value.sum() <= 302.073  # FOR ITALY

    # sum of NUTS3 values equals the value for Italy
    sql_cmd = f"""SELECT value FROM processed_data 
                    WHERE var_detail_id in (SELECT id FROM var_details WHERE var_name IN {var_names}) 
                    AND region_id = (SELECT id FROM regions WHERE region_code='IT');"""

    country_df = get_table(sql_cmd)
    assert nuts3_df.value.sum() == country_df.value.sum()
