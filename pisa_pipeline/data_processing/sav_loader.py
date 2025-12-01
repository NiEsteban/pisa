import os
import pandas as pd
import pyreadstat
from pisa_pipeline.utils.io import load_sav_metadata


class SAVloader:
    def __init__(self, chunksize=100000):
        self.country_code = None
        self.chunksize = chunksize
    

    # -------------------------
    # Country-specific row extraction
    # -------------------------
    def find_country_rows(self, file_path, country_col="CNT"):
        """Return list of row indices where country_col == country_code."""
        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sav,
            file_path,
            chunksize=self.chunksize,
            usecols=[country_col]
        )
        row_offset = 0
        matching_rows = []

        for df, _ in reader:
            mask = df[country_col].apply(lambda x: str(x).upper() == self.country_code if pd.notnull(x) else False)
            matching_indices = df[mask].index + row_offset
            matching_rows.extend(matching_indices.tolist())
            row_offset += len(df)

        return matching_rows

    def load_rows_from_sav(self, file_path, row_indices):
        """Load only the rows specified in row_indices from a SPSS file."""
        if not row_indices:
            return pd.DataFrame()

        start, end = min(row_indices), max(row_indices) + 1
        selected_rows = []
        row_offset = 0
        reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sav, file_path, chunksize=10000)

        for chunk_df, _ in reader:
            chunk_start = row_offset
            chunk_end = row_offset + len(chunk_df)

            if chunk_end <= start:
                row_offset += len(chunk_df)
                continue
            if chunk_start >= end:
                break

            relative_start = max(0, start - chunk_start)
            relative_end = min(len(chunk_df), end - chunk_start)
            selected_rows.append(chunk_df.iloc[relative_start:relative_end])
            row_offset += len(chunk_df)

        return pd.concat(selected_rows, ignore_index=True)

    # -------------------------
    # CSV extraction
    # -------------------------
    def extract_country_csv(self, file_path, country_col="CNT"):
        """Extract rows for a specific country and save as CSV."""
        row_indices = self.find_country_rows(file_path, country_col)
        df = self.load_rows_from_sav(file_path, row_indices)
        if df.empty:
            print(f"[LOADER] No rows found for {self.country_code} in {file_path}")
            return None

        print(f"[LOADER] Loaded {len(df)} rows for country code {self.country_code}")
        return df

    # -------------------------
    # Apply SPSS labels
    # -------------------------
    def label_csv_with_sav(self, df, sav_path):
        """Apply SPSS labels to a DataFrame and save as CSV."""
        meta = load_sav_metadata(sav_path)
        df_labeled = pyreadstat.set_value_labels(
            df, meta, formats_as_category=True, formats_as_ordered_category=False
        )
        df_labeled.columns = meta.column_labels
        return df_labeled

    # -------------------------
    # Main run method for one file
    # -------------------------
    def run(self, sav_file,country_code="MEX"):
        print(f"[LOADER] Loading file {sav_file} using code:{country_code}")
        self.country_code= country_code.upper()
        # Extract country rows
        df = self.extract_country_csv(sav_file)
        if df is None:
            return None, None
        # Apply labels
        df_labeled = self.label_csv_with_sav(df, sav_file)

        return df_labeled,df
