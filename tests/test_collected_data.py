from zoomin.db_access import get_table


def test_land_use_and_land_cover():
    var_names = [
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
    ]

    sql_cmd = f"SELECT value processed_data WHERE var_detail_id in (SELECT id FROM var_details WHERE var_name IN {var_names});"

    data_df = get_table(sql_cmd)

    assert data_df.value.sum() <= 302.073  # FOR ITALY
