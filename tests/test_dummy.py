import os
from zoomin import disaggregation_manager as disagg_manager
from dotenv import load_dotenv, find_dotenv

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

db_country = os.environ.get("DB_COUNTRY")
db_version = os.environ.get("DB_VERSION")

db_name = f"{db_country.lower()}_v{db_version}"


def test_dummy():
    # disagg_manager.disaggregate_eucalc_var("eucalc_dhg_energy_demand_added_district_heat_solid_ff_coal",
    #                                         "national",
    #                                         2050,
    #                                         "NUTS2")

    bad_proxy = disagg_manager.disaggregate_collected_var(
        "non_residential_energy_demand_space_cooling",
        source_resolution="NUTS0",
        target_resolution="LAU",
    )
