from zoomin.db_access import get_values


def test_eucalc_data_copy():
    value = get_values(
        f"""SELECT value FROM processed_data 
        WHERE var_detail_id = (SELECT id FROM var_details WHERE var_name = 'eucalc_agr_emissions_co2e_crop_burnt_residues')
        AND pathway = 'national' AND year = 2030;"""
    )

    assert isinstance(value, float)
