import pandas as pd
import os

class ETL:
    def __init__(self, output_path):
        self.output_path = output_path
    
    def extract(self):
        raise NotImplementedError("Extract Method not Implemented!")
    
    def transform(self):
        raise NotImplementedError("Transform Method not Implemented!")
    
    def load(self,data, filename):
        
        os.makedirs(self.output_path, exist_ok=True)

        csv_path = os.path.join(self.output_path, filename)
        data.to_csv(csv_path, index=False)
        print(f"Data saved to {csv_path}")

        parquet_filename = filename.replace(".csv", ".parquet")
        parquet_path = os.path.join(self.output_path, parquet_filename)
        data.to_parquet(parquet_path, index=False)
        print(f"Data saved to {parquet_path}")