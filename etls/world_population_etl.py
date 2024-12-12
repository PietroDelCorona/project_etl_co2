from etls.etl_base import ETL
import pandas as pd

class WorldPopulationETL(ETL):
    def extract(self, file_path):
        print(f"Extracting data from {file_path}")
        return pd.read_csv(file_path)
    
    def transform(self, data):
        print("Transforming data...")

        data = data.drop(columns=['Record Type','Reliability','Source Year','Value Footnotes'])

        data = data.dropna(subset=['Value'])

        data.rename(columns={
        "Country or Area": "country_or_area",
        "Year": "year",
        "Area": "area",
        "Values": "values"
        }, inplace=True)

        data = data[data["Sex"] == "Both Sexes"]

        data = data.drop(columns=['Sex'])

        if "values" in data.columns:
            data['values'] = data['values'].astype(str)
            data['values'] = data['values'].str.replace(r"[^\d.-]", "", regex=True)
            data['values'] = data['values'].str.replace(",", ".")
            data['values'] = pd.to_numeric(data['values'], errors="coerce")

        return data
    
