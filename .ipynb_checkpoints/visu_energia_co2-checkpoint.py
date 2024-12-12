import pandas as pd
import plotly.express as px

# Carregar os dados de energia
energy_mix_path = "./data/data_cleaned/cleaned_energy_mix_sources.parquet"
energy_data = pd.read_parquet(energy_mix_path)

# Carregar os dados de emissões de CO2
co2_emissions_path = "./data/data_cleaned/cleaned_co2_emissions_per_capita.parquet"
co2_data = pd.read_parquet(co2_emissions_path)

# Renomear colunas para facilitar o merge
co2_data.rename(columns={"Entity": "Country", "Year": "Year", "emissions_total_per_capita": "CO2 Emissions Per Capita"}, inplace=True)

# Unir os dois datasets
merged_data = pd.merge(
    energy_data,
    co2_data,
    left_on=["Entity", "Year"],
    right_on=["Country", "Year"],
    how="inner"
)

# Derreter os dados de consumo energético para formato longo
melted_energy = pd.melt(
    merged_data,
    id_vars=["Entity", "Year", "CO2 Emissions Per Capita"],
    value_vars=[
        "coal_per_capita__kwh",
        "oil_per_capita__kwh",
        "gas_per_capita__kwh",
        "nuclear_per_capita__kwh__equivalent",
        "hydro_per_capita__kwh__equivalent",
        "wind_per_capita__kwh__equivalent",
        "solar_per_capita__kwh__equivalent",
        "other_renewables_per_capita__kwh__equivalent",
    ],
    var_name="Energy Source",
    value_name="Consumption (kWh per capita)"
)

# Ajustar os nomes das fontes de energia para exibição amigável
melted_energy["Energy Source"] = melted_energy["Energy Source"].str.replace("__kwh", "").str.replace("_equivalent", "").str.replace("_", " ").str.title()

# Filtrar os países desejados
countries = ["United States", "China", "India", "Germany", "Brazil"]
melted_energy = melted_energy[melted_energy["Entity"].isin(countries)]

# Gráfico interativo com Plotly
fig = px.bar(
    melted_energy,
    x="Entity",
    y="Consumption (kWh per capita)",
    color="Energy Source",
    animation_frame="Year",
    hover_data=["CO2 Emissions Per Capita"],
    title="Energy Consumption by Source and CO2 Emissions Per Capita",
    labels={"Entity": "Country", "Year": "Year"},
    barmode="stack",
    height=600,
)
fig.update_layout(
    xaxis_title="Country",
    yaxis_title="Consumption (kWh per capita)",
    legend_title="Energy Source",
)
fig.show(renderer="browser")
