import requests
from etls.etl_base import ETL
import pandas as pd
from io import StringIO

class CO2EmissionsETL(ETL):
    
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

        if 'emissions_total_per_capita' in data.columns:
            data['emissions_total_per_capita'] = data['emissions_total_per_capita'].astype(str)
            data['emissions_total_per_capita'] = data['emissions_total_per_capita'].str.replace(r"[^\d.-]", "", regex=True)
            data['emissions_total_per_capita'] = data['emissions_total_per_capita'].str.replace(",", ".")
            data['emissions_total_per_capita'] = pd.to_numeric(data['emissions_total_per_capita'], errors='coerce')

        return data 
