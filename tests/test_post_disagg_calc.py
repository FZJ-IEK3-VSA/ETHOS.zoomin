from zoomin.db_access import get_table, get_col_values, get_primary_key
import pytest


def test_relative_gross_value_added_nace_sector_a():

    numerator_var_detail_id = get_primary_key(
        "var_details", {"var_name": "gross_value_added_nace_sector_a"}
    )
    denominator_var_detail_id = get_primary_key(
        "var_details", {"var_name": "gross_domestic_product"}
    )

    numerator_value = get_col_values(
        "processed_data",
        "value",
        {"var_detail_id": numerator_var_detail_id, "region_id": 10},
    )
    denominator_value = get_col_values(
        "processed_data",
        "value",
        {"var_detail_id": denominator_var_detail_id, "region_id": 10},
    )

    numerator_quality_rating_id = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": numerator_var_detail_id, "region_id": 10},
    )
    denominator_quality_rating_id = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": denominator_var_detail_id, "region_id": 10},
    )

    output_var_detail_id = get_primary_key(
        "var_details", {"var_name": "relative_gross_value_added_nace_sector_a"}
    )
    output_value = get_col_values(
        "processed_data",
        "value",
        {"var_detail_id": output_var_detail_id, "region_id": 10},
    )
    output_quality_rating_id = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": output_var_detail_id, "region_id": 10},
    )

    expected_value = eval(f"({numerator_value} / {denominator_value}) * 100")
    assert output_value == expected_value

    expected_quality_rating_id = min(
        numerator_quality_rating_id, denominator_quality_rating_id
    )
    assert output_quality_rating_id == expected_quality_rating_id


def test_heat_production_with_natural_gas():

    var_detail_id_a = get_primary_key(
        "var_details", {"var_name": "heat_demand_residential"}
    )
    var_detail_id_b = get_primary_key(
        "var_details", {"var_name": "heat_demand_non_residential"}
    )
    var_detail_id_c = get_primary_key(
        "var_details", {"var_name": "heat_production_with_lignite"}
    )
    var_detail_id_d = get_primary_key(
        "var_details", {"var_name": "heat_production_with_coal"}
    )

    value_a = get_col_values(
        "processed_data", "value", {"var_detail_id": var_detail_id_a, "region_id": 10}
    )
    value_b = get_col_values(
        "processed_data", "value", {"var_detail_id": var_detail_id_b, "region_id": 10}
    )
    value_c = get_col_values(
        "processed_data", "value", {"var_detail_id": var_detail_id_c, "region_id": 10}
    )
    value_d = get_col_values(
        "processed_data", "value", {"var_detail_id": var_detail_id_d, "region_id": 10}
    )

    quality_rating_id_a = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": var_detail_id_a, "region_id": 10},
    )
    quality_rating_id_b = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": var_detail_id_b, "region_id": 10},
    )
    quality_rating_id_c = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": var_detail_id_c, "region_id": 10},
    )
    quality_rating_id_d = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": var_detail_id_d, "region_id": 10},
    )

    output_var_detail_id = get_primary_key(
        "var_details", {"var_name": "heat_production_with_natural_gas"}
    )
    output_value = get_col_values(
        "processed_data",
        "value",
        {"var_detail_id": output_var_detail_id, "region_id": 10},
    )
    output_quality_rating_id = get_col_values(
        "processed_data",
        "quality_rating_id",
        {"var_detail_id": output_var_detail_id, "region_id": 10},
    )

    expected_value = eval(f"({value_a} + {value_b}) - ({value_c} - {value_d})")
    assert output_value == expected_value

    expected_quality_rating_id = min(
        quality_rating_id_a,
        quality_rating_id_b,
        quality_rating_id_c,
        quality_rating_id_d,
    )
    assert output_quality_rating_id == expected_quality_rating_id


def test_eucalc_agr_energy_demand_liquid_ff_gasoline_ei():

    vars_involved = [
        "eucalc_agr_input_use_emissions_co2_fuel",
        "eucalc_agr_energy_demand_liquid_ff_gasoline",
        "eucalc_agr_energy_demand_gas_ff_natural",
        "eucalc_agr_energy_demand_liquid_ff_diesel",
        "eucalc_agr_energy_demand_liquid_ff_fuel_oil",
        "eucalc_agr_energy_demand_liquid_ff_gasoline",
        "eucalc_agr_energy_demand_liquid_ff_lpg",
    ]

    df_dicts = {}

    for var in vars_involved:
        df = get_table(
            f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='{var}')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
        )

        df_dicts[var] = df

    out_df = get_table(
        f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='eucalc_agr_energy_demand_liquid_ff_gasoline_ei')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
    )

    assert len(out_df) > 0

    for pathway_id in [1, 2]:
        for year in [2020, 2030]:
            for check_type in ["value", "quality_rating_id"]:

                output = out_df[
                    (out_df["pathway_id"] == pathway_id) & (out_df["year"] == year)
                ][check_type].item()

                df = df_dicts["eucalc_agr_input_use_emissions_co2_fuel"]
                a = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_agr_energy_demand_liquid_ff_gasoline"]
                b = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_agr_energy_demand_gas_ff_natural"]
                c = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_agr_energy_demand_liquid_ff_diesel"]
                d = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_agr_energy_demand_liquid_ff_fuel_oil"]
                e = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_agr_energy_demand_liquid_ff_lpg"]
                f = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                if check_type == "value":
                    expected = eval(f"{a} * {b} / ({b} + {c} + {d} + {e} + {f})")

                else:
                    expected = min(a, b, c, d, e, f)

                assert output == expected


def test_eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei():

    vars_involved = [
        "eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk",
        "eucalc_agr_domestic_production_liv_abp_dairy_milk",
    ]

    df_dicts = {}

    for var in vars_involved:
        df = get_table(
            f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='{var}')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
        )

        df_dicts[var] = df

    out_df = get_table(
        f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
    )

    assert len(out_df) > 0

    for pathway_id in [1, 2]:
        for year in [2020, 2030]:
            for check_type in ["value", "quality_rating_id"]:

                output = out_df[
                    (out_df["pathway_id"] == pathway_id) & (out_df["year"] == year)
                ][check_type].item()

                df = df_dicts["eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk"]
                a = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_agr_domestic_production_liv_abp_dairy_milk"]
                b = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                if check_type == "value":
                    expected = eval(f"{a} / {b}")

                else:
                    expected = min(a, b)

                assert output == expected


def test_eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei():

    vars_involved = [
        "eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk",
        "eucalc_agr_domestic_production_liv_abp_dairy_milk",
    ]

    df_dicts = {}

    for var in vars_involved:
        df = get_table(
            f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='{var}')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
        )

        df_dicts[var] = df

    out_df = get_table(
        f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
    )

    assert len(out_df) > 0

    for pathway_id in [1, 2]:
        for year in [2020, 2030]:
            for check_type in ["value", "quality_rating_id"]:

                output = out_df[
                    (out_df["pathway_id"] == pathway_id) & (out_df["year"] == year)
                ][check_type].item()

                df = df_dicts["eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk"]
                a = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_agr_domestic_production_liv_abp_dairy_milk"]
                b = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                if check_type == "value":
                    expected = eval(f"{a} / {b}")

                else:
                    expected = min(a, b)

                assert output == expected


def test_eucalc_tra_emissions_co2e_freight_total():

    vars_involved = [
        "eucalc_tra_emissions_co2e_freight_hdv",
        "eucalc_tra_emissions_co2e_freight_iww",
        "eucalc_tra_emissions_co2e_freight_aviation",
        "eucalc_tra_emissions_co2e_freight_marine",
        "eucalc_tra_emissions_co2e_freight_rail",
    ]

    df_dicts = {}

    for var in vars_involved:
        df = get_table(
            f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='{var}')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
        )

        df_dicts[var] = df

    out_df = get_table(
        f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='eucalc_tra_emissions_co2e_freight_total')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
    )

    assert len(out_df) > 0

    for pathway_id in [1, 2]:
        for year in [2020, 2030]:
            for check_type in ["value", "quality_rating_id"]:

                output = out_df[
                    (out_df["pathway_id"] == pathway_id) & (out_df["year"] == year)
                ][check_type].item()

                df = df_dicts["eucalc_tra_emissions_co2e_freight_hdv"]
                a = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_tra_emissions_co2e_freight_iww"]
                b = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_tra_emissions_co2e_freight_aviation"]
                c = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_tra_emissions_co2e_freight_marine"]
                d = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_tra_emissions_co2e_freight_rail"]
                e = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                if check_type == "value":
                    expected = eval(f"{a} + {b} + {c} + {d} + {e}")

                else:
                    expected = min(a, b, c, d, e)

                assert output == expected


def test_eucalc_elc_energy_production_res_bio_gas_ei():

    vars_involved = [
        "eucalc_elc_emissions_co2e_res_bio",
        "eucalc_elc_energy_production_res_bio_gas",
        "eucalc_elc_energy_production_res_bio_mass",
    ]

    df_dicts = {}

    for var in vars_involved:
        df = get_table(
            f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='{var}')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
        )

        df_dicts[var] = df

    out_df = get_table(
        f"""SELECT pathway_id, year, value, quality_rating_id 
              FROM processed_data 
              WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name='eucalc_elc_energy_production_res_bio_gas_ei')
                    AND year in (2020, 2030) 
                    AND region_id = 10"""
    )

    assert len(out_df) > 0

    for pathway_id in [1, 2]:
        for year in [2020, 2030]:
            for check_type in ["value", "quality_rating_id"]:

                output = out_df[
                    (out_df["pathway_id"] == pathway_id) & (out_df["year"] == year)
                ][check_type].item()

                df = df_dicts["eucalc_elc_emissions_co2e_res_bio"]
                a = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_elc_energy_production_res_bio_gas"]
                b = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                df = df_dicts["eucalc_elc_energy_production_res_bio_mass"]
                c = df[(df["pathway_id"] == pathway_id) & (df["year"] == year)][
                    check_type
                ].item()

                if check_type == "value":
                    expected = eval(f"{a} / ({b} + {c})")

                else:
                    expected = min(a, b, c)

                assert output == expected
