import requests
from etls.etl_base import ETL
import pandas as pd
from io import StringIO

class CO2LandUseETL(ETL):
    
    def __init__(self, api_url, output_path):
        super().__init__(output_path)
        self.api_url = api_url
    
    def extract(self):
        print(f"Fetching data from API: {self.api_url}")
        response = requests.get(self.api_url)
        if response.status_code == 200:
            return pd.read_csv(StringIO(response.text))
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    def transform(self, data):
        print("Transforming API data...")

        data = data.drop(columns=['Code'])

        data = data.dropna(subset=['emissions_total_including_land_use_change', 'emissions_from_land_use_change', 'emissions_total'])

        numeric_columns = [
            "emissions_total_including_land_use_change",
            "emissions_from_land_use_change",
            "emissions_total"
        ]

        # Converter as colunas para numérico (float), tratando erros como NaN
        for column in numeric_columns:
            if column in data.columns:
                # Remove caracteres inválidos e ajusta separadores
                data[column] = data[column].astype(str)  # Garante que é string antes
                data[column] = data[column].str.replace(r"[^\d.-]", "", regex=True)
                data[column] = data[column].str.replace(",", ".")
                data[column] = pd.to_numeric(data[column], errors="coerce")       
        
        return data 
