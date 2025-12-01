import os
import re
import pandas as pd
from pisa_pipeline.utils.file_utils import read_lines, extract_data_file_path


class Column:
    def __init__(self, name, start, end, fmt):
        self.name = name.upper()
        self.label = None
        self.start = int(start)
        self.end = int(end)
        self.format = fmt
        self.answer = {}

    def __repr__(self):
        return f"{self.label or self.name}"


class SPSSloader:
    def __init__(self):
        """Empty constructor; syntax file is provided in run()"""
        self.syntax_file = None
        self.data_folder = None
        self.columns = {}
        self.data_file = None
        self._lines = []

    # --- Syntax parsing ---
    def _load_syntax_lines(self):
        all_lines = read_lines(self.syntax_file)
        if len(all_lines) < 2:
            raise ValueError(f"Syntax file too short: {self.syntax_file}")

        data_file_path = extract_data_file_path(self.syntax_file)
        if not data_file_path:
            raise ValueError(f"Cannot find datafile path in second row of {self.syntax_file}")

        self._lines = all_lines[2:]
        self.data_file = os.path.join(self.data_folder, data_file_path)
        if not os.path.exists(self.data_file):
            raise FileNotFoundError(f"Data file not found: {self.data_file}")

        print(f"[LOADER] Using syntax: {self.syntax_file}")
        print(f"[LOADER] Found data file: {self.data_file}")
        return self._lines

    def _parse_column_definitions(self):
        pattern = re.compile(r"^\s*(\w+)\s+(\d+)\s+-\s+(\d+)\s+(\(A\)|\(F,\d+\))")
        self.columns = {}
        for line in self._lines:
            match = pattern.match(line)
            if match:
                name, start, end, fmt = match.groups()
                self.columns[name.upper()] = Column(name, start, end, fmt)

    def _parse_labels_and_missing(self):
        for line in self._lines:
            if '"' in line:
                parts = line.split('"')
                key = parts[0].strip().upper()
                if key in self.columns:
                    self.columns[key].label = parts[1]
                continue
            if line.startswith("Missing values"):
                parts = line.split()
                key = parts[2].strip().upper()
                if key in self.columns:
                    for x in parts[-1].split(","):
                        val = x.strip("().")
                        if val.isdigit():
                            val = int(val)
                        self.columns[key].answer[val] = "Missing"

    def _parse_value_labels(self):
        value_label_section = False
        current_columns = []
        for line in self._lines:
            if line.startswith("value labels"):
                value_label_section = True
                continue
            if value_label_section:
                if line.startswith("."):
                    value_label_section = False
                    continue
                if line.startswith("/") and not line.startswith("//"):
                    current_columns = line.split("/")[1].strip().upper().split()
                    continue
                if not line:
                    continue
                parts = line.split(" ", 1)
                if len(parts) == 2:
                    val, label = parts
                    label = label.replace('"', '').replace("'", "").strip()
                    for c in current_columns:
                        if c in self.columns:
                            self.columns[c].answer[val] = label

    def parse_syntax(self):
        """Parse syntax and prepare columns"""
        self._load_syntax_lines()
        self._parse_column_definitions()
        self._parse_labels_and_missing()
        self._parse_value_labels()
        print(f"[LOADER] Parsed {len(self.columns)} columns.")
        return self.columns

    # --- Data loading ---
    def apply_labels(self, df):
        new_cols = {}
        for col in self.columns.values():
            new_name = col.label or col.name
            new_cols[new_name] = df[col.name].map(lambda v: col.answer.get(v, v))
        return pd.DataFrame(new_cols, index=df.index)

    def load_data(self, country_code="MEX"):
        rows = []
        for line in open(self.data_file, "r", encoding="cp1252"):
            if country_code and not line.startswith(country_code):
                continue
            row = [line[col.start - 1:col.end].strip() or None for col in self.columns.values()]
            rows.append(row)

        df = pd.DataFrame(rows, columns=[c.name for c in self.columns.values()])
        print(f"[LOADER] Loaded {len(df)} rows for country code '{country_code}'")
        return df

    # --- Run single file ---
    def run(self, syntax_file, data_folder=None, country_code="MEX"):
        """
        Run loader for a single syntax file.

        1. Extracted CSV (MEX rows, no labels) in `extract_folder`
        2. Labeled CSV (MEX rows with labels) in `labeled_folder`
        """
        # Set paths
        
        print(f"[LOADER] Loading file {syntax_file} using code:{country_code}")
        self.syntax_file = syntax_file
        self.data_folder = data_folder or os.path.dirname(syntax_file)

        # Parse syntax
        self.parse_syntax()

        # Load rows for the country
        df = self.load_data(country_code=country_code)
        if df.empty:
            print(f"[LOADER] No rows found for country code '{country_code}'")
            return None, None
        
        # --- Apply labels and save labeled CSV ---
        df_labeled = self.apply_labels(df)



        return df_labeled,df
