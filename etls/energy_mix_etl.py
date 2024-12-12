import requests
from etls.etl_base import ETL
import pandas as pd
from io import StringIO
import logging

class EnergyMixETL(ETL):
    
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
        logging.info(f"Dados recebidos para transformação: {data.head()}")

        if data.empty:
            logging.warning("O DataFrame está vazio após a extração.")
            return None

        data = data.drop(columns=['Code'])

        # Lista de colunas que precisam ser convertidas para numérico
        numeric_columns = [
            "coal_per_capita__kwh",
            "oil_per_capita__kwh",
            "gas_per_capita__kwh",
            "nuclear_per_capita__kwh__equivalent",
            "hydro_per_capita__kwh__equivalent",
            "wind_per_capita__kwh__equivalent",
            "solar_per_capita__kwh__equivalent",
            "other_renewables_per_capita__kwh__equivalent"
        ]

        # Converter as colunas para numérico (float), tratando erros como NaN
        for column in numeric_columns:
            if column in data.columns:
                # Remove caracteres inválidos e ajusta separadores
                data[column] = data[column].astype(str)  # Garante que é string antes
                data[column] = data[column].str.replace(r"[^\d.-]", "", regex=True)
                data[column] = data[column].str.replace(",", ".")
                data[column] = pd.to_numeric(data[column], errors="coerce")

        data = data.fillna(0)

        logging.info(f"Dados transformados: {data.head()}")

        return data 
