#!/usr/bin/env python3

import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt

def get_parquet_files(folder_path):
    """Return all .snappy.parquet and .parquet files in the folder."""
    return [os.path.join(folder_path, f) for f in os.listdir(folder_path)
            if f.endswith('.snappy.parquet') or f.endswith('.parquet')]

def combine_parquet_files(parquet_files):
    """Combine multiple Parquet files into one DataFrame with source tracking."""
    combined = []
    reference_columns = None

    for file_path in parquet_files:
        try:
            df = pd.read_parquet(file_path)
            if reference_columns is None:
                reference_columns = df.columns.tolist()
            if df.columns.tolist() == reference_columns:
                df['source'] = os.path.basename(file_path)  # Add filename as source
                combined.append(df)
            else:
                print(f"Skipping {file_path}: column headers don't match.")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    if combined:
        return pd.concat(combined, ignore_index=True)
    else:
        print("No matching Parquet files could be combined.")
        return None

def convert_parquet_to_csv(folder_path, parquet_files):
    """Convert individual Parquet files to CSV, returns list of CSV paths."""
    csv_files = []

    for file_path in parquet_files:
        print(f"Converting: {file_path}")
        try:
            df = pd.read_parquet(file_path)
            csv_filename = os.path.basename(file_path).replace('.snappy.parquet', '.csv').replace('.parquet', '.csv')
            csv_path = os.path.join(folder_path, csv_filename)
            df.to_csv(csv_path, index=False)
            print(f"Saved CSV: {csv_path}")
            csv_files.append(csv_path)
        except Exception as e:
            print(f"Error converting {file_path}: {e}")

    return csv_files

def plot_data_from_files(csv_files, x_col, y_col, separate=False, save_path=None, legend=False):
    """Plot data from CSV files."""
    if separate:
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                if x_col in df.columns and y_col in df.columns:
                    plt.scatter(df[x_col], df[y_col], alpha=0.6)
                    plt.xlabel(x_col)
                    plt.ylabel(y_col)
                    plt.title(f"{os.path.basename(csv_file)}: {x_col} vs {y_col}")
                    plt.grid(True)
                    plt.tight_layout()
                    output_path = os.path.join(os.path.dirname(csv_file), f"{os.path.basename(csv_file).replace('.csv', '')}_scatter.png")
                    plt.savefig(output_path, format='png')
                    plt.clf()
                    print(f"Saved plot: {output_path}")
                else:
                    print(f"Missing columns in {csv_file}")
            except Exception as e:
                print(f"Error reading {csv_file}: {e}")
    else:
        color_list = plt.get_cmap("tab10").colors
        for idx, csv_file in enumerate(csv_files):
            try:
                df = pd.read_csv(csv_file)
                if x_col in df.columns and y_col in df.columns:
                    plt.scatter(df[x_col], df[y_col], alpha=0.6,
                                color=color_list[idx % len(color_list)],
                                label=os.path.basename(csv_file) if legend else None)
            except Exception as e:
                print(f"Error reading {csv_file}: {e}")
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.title(f"Combined Scatterplot of {x_col} vs {y_col}")
        plt.grid(True)
        plt.tight_layout()
        if legend:
            plt.legend()
        if save_path:
            plt.savefig(save_path, format='png')
            print(f"Saved combined plot: {save_path}")
        else:
            plt.show()

def plot_data_from_df(df, x_col, y_col, save_path=None, legend=False):
    """Plot a combined DataFrame, with coloring by source if available."""
    if x_col in df.columns and y_col in df.columns:
        if legend and 'source' in df.columns:
            color_list = plt.get_cmap("tab10").colors
            for idx, (source_name, group_df) in enumerate(df.groupby("source")):
                plt.scatter(group_df[x_col], group_df[y_col],
                            alpha=0.6,
                            color=color_list[idx % len(color_list)],
                            label=source_name)
        else:
            plt.scatter(df[x_col], df[y_col], alpha=0.6)

        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.title(f"Scatterplot of {x_col} vs {y_col}")
        plt.grid(True)
        plt.tight_layout()
        if legend and 'source' in df.columns:
            plt.legend()
        if save_path:
            plt.savefig(save_path, format='png')
            print(f"Saved plot: {save_path}")
        else:
            plt.show()
    else:
        print(f"Columns {x_col} and/or {y_col} not found in DataFrame.")

def main():
    parser = argparse.ArgumentParser(description="Convert Parquet to CSV with optional combining and plotting.")
    parser.add_argument("folder", help="Folder containing .parquet/.snappy.parquet files")
    parser.add_argument("--combine", action="store_true", help="Combine files into one before saving")
    parser.add_argument("--x", help="X-axis column for plotting")
    parser.add_argument("--y", help="Y-axis column for plotting")
    parser.add_argument("--plot", action="store_true", help="Enable scatterplot of selected columns")
    parser.add_argument("--separate-plots", action="store_true", help="Generate separate plots per file")
    parser.add_argument("--combine-plot", action="store_true", help="Generate a combined plot")
    parser.add_argument("--save-fig", help="Path to save combined plot (e.g., ./plot.png)")
    parser.add_argument("--legend", action="store_true", help="Enable legend in combined plot")

    args = parser.parse_args()
    folder = args.folder

    parquet_files = get_parquet_files(folder)
    if not parquet_files:
        print("No Parquet files found.")
        return

    combined_df = None
    csv_files = []

    if args.combine:
        combined_df = combine_parquet_files(parquet_files)
        if combined_df is not None:
            combined_csv_path = os.path.join(folder, "combined_output.csv")
            combined_df.to_csv(combined_csv_path, index=False)
            print(f"Combined CSV saved: {combined_csv_path}")
            csv_files = [combined_csv_path]
    else:
        csv_files = convert_parquet_to_csv(folder, parquet_files)

    if args.plot:
        if not args.x or not args.y:
            print("Error: --x and --y are required for plotting.")
            return

        if args.combine and combined_df is not None and args.combine_plot:
            plot_data_from_df(combined_df, args.x, args.y, save_path=args.save_fig, legend=args.legend)
        else:
            plot_data_from_files(csv_files, args.x, args.y,
                                 separate=args.separate_plots,
                                 save_path=args.save_fig,
                                 legend=args.legend)

if __name__ == "__main__":
    main()
