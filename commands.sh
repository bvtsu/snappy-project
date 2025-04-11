# set up virtual environment in directory to encapsulate dependencies
python3 -m venv .

# activate virtual environment
source ./bin/activate

# now that you are inside the venv, install dependencies
pip install pandas pyarrow matplotlib

# create dummy snappy.parquet files in test_data folder
python3 generate_fake_parquet.py

# Convert all Parquet files to CSV:
python parquet_to_csv_plot.py test_data

# Combine Parquet files and plot combined data:
python parquet_to_csv_plot.py test_data --combine --x temperature --y pressure --plot --combine-plot --save-fig combined_output.png

# Generate separate plots for each file:
python parquet_to_csv_plot.py test_data --plot --x temperature --y pressure --plot --separate-plots

# Note: Add --legend if you want to label/distinguish each sample within the combined plot.