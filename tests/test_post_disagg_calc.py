from zoomin.db_access import get_table, get_values
import numpy as np


def test_relative_gross_value_added_nace_sector_a():
    numerator_value = get_values(
        """SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'gross_value_added_nace_sector_a')
                                 AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34');"""
    )

    denominator_value = get_values(
        """SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'gross_domestic_product')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34');"""
    )

    numerator_confidence_level_id = get_values(
        """SELECT confidence_level_id FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'gross_value_added_nace_sector_a')
                                 AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34');"""
    )

    denominator_confidence_level_id = get_values(
        """SELECT confidence_level_id FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'gross_domestic_product')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34');"""
    )

    output_value = get_values(
        """SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'relative_gross_value_added_nace_sector_a')
                                 AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34');"""
    )

    output_confidence_level_id = get_values(
        """SELECT confidence_level_id FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'relative_gross_value_added_nace_sector_a')
                                 AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34');"""
    )

    expected_value = eval(f"({numerator_value} / {denominator_value}) * 100")
    assert output_value == expected_value

    expected_confidence_level_id = min(
        numerator_confidence_level_id, denominator_confidence_level_id
    )
    assert output_confidence_level_id == expected_confidence_level_id


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

    output_dict = {}

    for var in vars_involved:
        value = get_values(
            f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        confidence_level_id = get_values(
            f"""SELECT confidence_level_id FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        output_dict[var] = {"value": value, "confidence_level_id": confidence_level_id}

    output_value = get_values(
        f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'eucalc_agr_energy_demand_liquid_ff_gasoline_ei')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    output_confidence_level_id = get_values(
        f"""SELECT confidence_level_id FROM processed_data 
                                WHERE var_detail_id = (SELECT id FROM var_details 
                                        WHERE var_name = 'eucalc_agr_energy_demand_liquid_ff_gasoline_ei')
                                AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                            AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    a = output_dict["eucalc_agr_input_use_emissions_co2_fuel"]["value"]
    b = output_dict["eucalc_agr_energy_demand_liquid_ff_gasoline"]["value"]
    c = output_dict["eucalc_agr_energy_demand_gas_ff_natural"]["value"]
    d = output_dict["eucalc_agr_energy_demand_liquid_ff_diesel"]["value"]
    e = output_dict["eucalc_agr_energy_demand_liquid_ff_fuel_oil"]["value"]
    f = output_dict["eucalc_agr_energy_demand_liquid_ff_lpg"]["value"]

    expected_value = eval(f"{a} * {b} / ({b} + {c} + {d} + {e} + {f})")

    assert np.round(output_value, 5) == np.round(expected_value, 5)

    a = output_dict["eucalc_agr_input_use_emissions_co2_fuel"]["confidence_level_id"]
    b = output_dict["eucalc_agr_energy_demand_liquid_ff_gasoline"][
        "confidence_level_id"
    ]
    c = output_dict["eucalc_agr_energy_demand_gas_ff_natural"]["confidence_level_id"]
    d = output_dict["eucalc_agr_energy_demand_liquid_ff_diesel"]["confidence_level_id"]
    e = output_dict["eucalc_agr_energy_demand_liquid_ff_fuel_oil"][
        "confidence_level_id"
    ]
    f = output_dict["eucalc_agr_energy_demand_liquid_ff_lpg"]["confidence_level_id"]

    assert output_confidence_level_id == min(a, b, c, d, e, f)


def test_eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei():

    vars_involved = [
        "eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk",
        "eucalc_agr_domestic_production_liv_abp_dairy_milk",
    ]

    output_dict = {}

    for var in vars_involved:
        value = get_values(
            f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        confidence_level_id = get_values(
            f"""SELECT confidence_level_id FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        output_dict[var] = {"value": value, "confidence_level_id": confidence_level_id}

    output_value = get_values(
        f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    output_confidence_level_id = get_values(
        f"""SELECT confidence_level_id FROM processed_data 
                                WHERE var_detail_id = (SELECT id FROM var_details 
                                        WHERE var_name = 'eucalc_agr_co2e_liv_applied_abp_dairy_milk_ei')
                                AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                            AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    a = output_dict["eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk"]["value"]
    b = output_dict["eucalc_agr_domestic_production_liv_abp_dairy_milk"]["value"]

    expected_value = eval(f"{a} / {b}")

    assert np.round(output_value, 5) == np.round(expected_value, 5)

    a = output_dict["eucalc_agr_emissions_co2e_liv_applied_abp_dairy_milk"][
        "confidence_level_id"
    ]
    b = output_dict["eucalc_agr_domestic_production_liv_abp_dairy_milk"][
        "confidence_level_id"
    ]

    assert output_confidence_level_id == min(a, b)


def test_eucalc_tra_emissions_co2e_freight_total():

    vars_involved = [
        "eucalc_tra_emissions_co2e_freight_hdv",
        "eucalc_tra_emissions_co2e_freight_iww",
        "eucalc_tra_emissions_co2e_freight_aviation",
        "eucalc_tra_emissions_co2e_freight_marine",
        "eucalc_tra_emissions_co2e_freight_rail",
    ]

    output_dict = {}

    for var in vars_involved:
        value = get_values(
            f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        confidence_level_id = get_values(
            f"""SELECT confidence_level_id FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        output_dict[var] = {"value": value, "confidence_level_id": confidence_level_id}

    output_value = get_values(
        f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'eucalc_tra_emissions_co2e_freight_total')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    output_confidence_level_id = get_values(
        f"""SELECT confidence_level_id FROM processed_data 
                                WHERE var_detail_id = (SELECT id FROM var_details 
                                        WHERE var_name = 'eucalc_tra_emissions_co2e_freight_total')
                                AND region_id = (SELECT id FROM regions WHERE region_code = 'ITF34')
                                            AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    a = output_dict["eucalc_tra_emissions_co2e_freight_hdv"]["value"]
    b = output_dict["eucalc_tra_emissions_co2e_freight_iww"]["value"]
    c = output_dict["eucalc_tra_emissions_co2e_freight_aviation"]["value"]
    d = output_dict["eucalc_tra_emissions_co2e_freight_marine"]["value"]
    e = output_dict["eucalc_tra_emissions_co2e_freight_rail"]["value"]

    expected_value = eval(f"{a} + {b} + {c} + {d} + {e}")

    assert np.round(output_value, 5) == np.round(expected_value, 5)

    a = output_dict["eucalc_tra_emissions_co2e_freight_hdv"]["confidence_level_id"]
    b = output_dict["eucalc_tra_emissions_co2e_freight_iww"]["confidence_level_id"]
    c = output_dict["eucalc_tra_emissions_co2e_freight_aviation"]["confidence_level_id"]
    d = output_dict["eucalc_tra_emissions_co2e_freight_marine"]["confidence_level_id"]
    e = output_dict["eucalc_tra_emissions_co2e_freight_rail"]["confidence_level_id"]

    assert output_confidence_level_id == min(a, b, c, d, e)


def test_eucalc_elc_energy_production_res_bio_gas_ei():

    vars_involved = [
        "eucalc_elc_emissions_co2e_res_bio",
        "eucalc_elc_energy_production_res_bio_gas",
        "eucalc_elc_energy_production_res_bio_mass",
    ]

    output_dict = {}

    for var in vars_involved:
        value = get_values(
            f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITG2A')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        confidence_level_id = get_values(
            f"""SELECT confidence_level_id FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = '{var}')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITG2A')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
        )

        output_dict[var] = {"value": value, "confidence_level_id": confidence_level_id}

    output_value = get_values(
        f"""SELECT value FROM processed_data 
                                 WHERE var_detail_id = (SELECT id FROM var_details 
                                            WHERE var_name = 'eucalc_elc_energy_production_res_bio_gas_ei')
                                   AND region_id = (SELECT id FROM regions WHERE region_code = 'ITG2A')
                                   AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    output_confidence_level_id = get_values(
        f"""SELECT confidence_level_id FROM processed_data 
                                WHERE var_detail_id = (SELECT id FROM var_details 
                                        WHERE var_name = 'eucalc_elc_energy_production_res_bio_gas_ei')
                                AND region_id = (SELECT id FROM regions WHERE region_code = 'ITG2A')
                                            AND year = 2020 
                                   AND pathway = 'with_behavioural_changes';"""
    )

    a = output_dict["eucalc_elc_emissions_co2e_res_bio"]["value"]
    b = output_dict["eucalc_elc_energy_production_res_bio_gas"]["value"]
    c = output_dict["eucalc_elc_energy_production_res_bio_mass"]["value"]

    try:
        expected_value = eval(f"{a} / ({b} + {c})")
    except:
        expected_value = 0

    assert np.round(output_value, 5) == np.round(expected_value, 5)

    a = output_dict["eucalc_elc_emissions_co2e_res_bio"]["confidence_level_id"]
    b = output_dict["eucalc_elc_energy_production_res_bio_gas"]["confidence_level_id"]
    c = output_dict["eucalc_elc_energy_production_res_bio_mass"]["confidence_level_id"]

    assert output_confidence_level_id == min(a, b, c)
