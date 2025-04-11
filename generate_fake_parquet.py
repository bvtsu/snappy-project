import pandas as pd
import numpy as np
import os

# Create a test folder
os.makedirs("test_data", exist_ok=True)

# Generate and save 3 sample Parquet files with two columns
for i in range(1, 4):
    df = pd.DataFrame({
        'temperature': np.random.uniform(20, 100, size=50),
        'pressure': np.random.uniform(1, 5, size=50)
    })
    file_path = f"test_data/sample_{i}.snappy.parquet"
    df.to_parquet(file_path, index=False, compression='snappy')
    print(f"Saved: {file_path}")
