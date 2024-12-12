from PostgresSQLLoader import PostgresSQLLoader
from etls.world_population_etl import WorldPopulationETL
from etls.co2_emissions_etl import CO2EmissionsETL
from etls.energy_mix_etl import EnergyMixETL
from etls.co2_land_use_etl import CO2LandUseETL

def main():

    loader = PostgresSQLLoader()

    # ETL para arquivo local
    world_pop_etl = WorldPopulationETL(output_path="./data/data_cleaned")
    world_pop_data = world_pop_etl.extract("./data/data_raw/un_data_pop_world.csv")
    world_pop_transformed = world_pop_etl.transform(world_pop_data)
    world_pop_etl.load(world_pop_transformed, "cleaned_un_data_pop_world.csv")
    
    #Carregar dados no PostgreSQL
    loader.load(world_pop_transformed, "world_population")

    # ETL para CO2 Emissions
    co2_api_url = "https://ourworldindata.org/grapher/co2-emissions-per-capita.csv?useColumnShortNames=true"
    co2_emissions_etl = CO2EmissionsETL(co2_api_url, output_path="./data/data_cleaned")
    co2_emissions_data = co2_emissions_etl.extract()
    co2_emissions_transformed = co2_emissions_etl.transform(co2_emissions_data)
    co2_emissions_etl.load(co2_emissions_transformed, "cleaned_co2_emissions_per_capita.csv")

    #Carregar dados no PostgreSQL
    loader.load(co2_emissions_transformed, "co2_emissions")

    # ETL para CO2 Land Use
    co2_land_use_api_url = "https://ourworldindata.org/grapher/co2-fossil-plus-land-use.csv?v=1&csvType=full&useColumnShortNames=true"
    co2_land_use_etl = CO2LandUseETL(co2_land_use_api_url, output_path="./data/data_cleaned")
    co2_land_use_data = co2_land_use_etl.extract()
    co2_land_use_transformed = co2_land_use_etl.transform(co2_land_use_data)
    co2_land_use_etl.load(co2_land_use_transformed, "cleaned_co2_land_use_total_emissions.csv")

    #Carregar dados no PostgreSQL
    loader.load(co2_land_use_transformed, "co2_land_use")

    # ETL para Energy Mix
    energy_api_url = "https://ourworldindata.org/grapher/per-capita-energy-stacked.csv?v=1&csvType=full&useColumnShortNames=true"
    energy_mix_etl = EnergyMixETL(energy_api_url, output_path="./data/data_cleaned")
    energy_mix_data = energy_mix_etl.extract()
    energy_mix_transformed = energy_mix_etl.transform(energy_mix_data)
    energy_mix_etl.load(energy_mix_transformed, "cleaned_energy_mix_sources.csv")

    #Carregar dados no PostgreSQL
    loader.load(energy_mix_transformed, "energy_mix")


if __name__ == "__main__":
    main()