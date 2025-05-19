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

# {
#  'business_investment',
#  'cost_of_final_residential_energy_consumption_compared_to_gross_family_income',

#  'final_energy_consumption_from_aluminium_production_using_coking_coal',
#  'final_energy_consumption_from_aluminium_production_using_fuel_oil',
#  'final_energy_consumption_from_aluminium_production_using_gas_oil_and_diesel_oil',
#  'final_energy_consumption_from_aluminium_production_using_geothermal',
#  'final_energy_consumption_from_aluminium_production_using_lignite',
#  'final_energy_consumption_from_aluminium_production_using_motor_gasoline',
#  'final_energy_consumption_from_aluminium_production_using_other_bituminous_coal',
#  'final_energy_consumption_from_aluminium_production_using_solar_thermal',
#  'final_energy_consumption_from_aluminium_production_using_sub_bituminous_coal',
#  'final_energy_consumption_from_manufacture_of_basic_metals_using_coking_coal',
#  'final_energy_consumption_from_manufacture_of_basic_metals_using_gas_oil_and_diesel_oil',
#  'final_energy_consumption_from_manufacture_of_basic_metals_using_geothermal',
#  'final_energy_consumption_from_manufacture_of_basic_metals_using_solar_thermal',
#  'final_energy_consumption_from_manufacture_of_cement_lime_and_plaster_using_liquefied_petroleum_gases',
#  'final_energy_consumption_from_manufacture_of_chemicals_using_lignite',
#  'final_energy_consumption_from_manufacture_of_glass_and_glass_products_using_other_bituminous_coal',
#  'final_energy_consumption_from_manufacture_of_glass_and_glass_products_using_solar_thermal',
#  'final_energy_consumption_from_manufacture_of_transport_equipment_using_fuel_oil',
#  'final_energy_consumption_from_manufacture_of_transport_equipment_using_gas_oil_and_diesel_oil',
#  'final_energy_consumption_from_manufacture_of_transport_equipment_using_liquefied_petroleum_gases',
#  'final_energy_consumption_from_manufacture_of_wood_and_wood_products_using_fuel_oil',
#  'final_energy_consumption_from_manufacture_of_wood_and_wood_products_using_gas_oil_and_diesel_oil',
#  'final_energy_consumption_from_manufacture_of_wood_and_wood_products_using_liquefied_petroleum_gases',
#  'final_energy_consumption_from_manufacture_of_wood_and_wood_products_using_motor_gasoline',
#  'final_energy_consumption_from_manufacturing_of_iron_and_steel_using_coking_coal',
#  'final_energy_consumption_from_manufacturing_of_iron_and_steel_using_lignite',
#  'final_energy_consumption_from_manufacturing_of_iron_and_steel_using_motor_gasoline',
#  'final_energy_consumption_from_manufacturing_of_iron_and_steel_using_other_bituminous_coal',
#  'final_energy_consumption_from_manufacturing_of_iron_and_steel_using_solar_thermal',
#  'final_energy_consumption_from_manufacturing_of_iron_and_steel_using_sub_bituminous_coal',
#  'final_energy_consumption_from_other_manufacturing_industries_using_gas_oil_and_diesel_oil',
#  'final_energy_consumption_from_other_manufacturing_industries_using_geothermal',
#  'final_energy_consumption_from_other_manufacturing_industries_using_lignite',
#  'final_energy_consumption_in_commerce',
#  'final_energy_consumption_in_construction',
#  'final_energy_consumption_in_households',
#  'final_energy_consumption_in_non_metallic_minerals_industries',
#  'final_energy_consumption_in_rail_transport',
#  'final_energy_consumption_in_road_transport',
#  'final_energy_consumption_in_textile_and_leather_industries',
#  'final_energy_consumption_in_wood_and_wood_products_industries',
#  'foreign_born_population',


#  'labour_productivity_in_agriculture',
#  'number_of_households',
#  'percentage_of_housing_cost_overburden_rate_by_poverty_status',
#  'production_in_manufacturing',

#  'total_investment',

#  'volume_of_freight_transport_relative_to_gdp'}


def test_dummy():
    # disagg_manager.disaggregate_eucalc_var("eucalc_dhg_energy_demand_added_district_heat_solid_ff_coal",
    #                                         "national",
    #                                         2050,
    #                                         "NUTS2")
    #  for var in [
    #     "employment_in_manufacturing",
    #             "employment_in_construction",
    #             "employment_in_nace_sector_g_i",
    #             "employment_in_nace_sector_j",
    #             "employment_in_nace_sector_k",
    #             "employment_in_nace_sector_l",
    #             "employment_in_nace_sector_m_n",
    #             "employment_in_nace_sector_o_q",
    #             "employment_in_nace_sector_r_u",]:

    bad_proxy = disagg_manager.disaggregate_collected_var(
        "ghg_emissions_from_fc_in_commerce",
        source_resolution="NUTS0",
        target_resolution="LAU",
    )
