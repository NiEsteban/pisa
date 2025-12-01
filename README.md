# PISA Pipeline

A modular and extensible Python pipeline designed to load, clean, transform, and analyze PISA questionnaire datasets. It supports `.sav` (SPSS) and `.csv` formats and provides a complete workflow for preparing the data for statistical analysis and machine learning.

---

## Features

- **Flexible Data Loading** – Load PISA `.sav` files (SPSS) and `.csv` files with a unified interface
- **Automated Cleaning** – Missing values handling, variable filtering, type normalization, and dataset consistency checks
- **Data Transformation Tools** – Apply recoding, feature engineering, merging, and column restructuring
- **Country Filtering** – Extract data for specific countries using ISO codes (e.g., `MEX` for Mexico)
- **Machine Learning Ready** – Export datasets for models such as Random Forest, Gradient Boosting, and Decision Trees
- **Modular Architecture** – Each step (load → clean → transform → save) is encapsulated to allow independent testing and updates
- **GUI Interface** – User-friendly graphical interface for non-technical users
- **Command Line Script** – Powerful CLI tool for batch processing and automation

---

## Project Structure

```
pisa_pipeline/
│── data_processing/
│   ├── spss_loader.py
│   ├── sav_loader.py
│   ├── cleaner.py
│   ├── transformer.py
│   └── process_results.py
│── utils/
│   ├── io.py
│   └── file_utils.py
│── interface/
│   └── main.py              # GUI Application
│── script.py                # Command Line Script
│── requirements.txt
│── README.md
```

---

## Installation

Clone the repository:

```bash
git clone https://github.com/NiEsteban/pisa.git
cd pisa
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Usage

### Option 1: GUI Interface (Recommended for Beginners)

Launch the graphical interface:

```bash
python interface/main.py
```

The interface allows you to:
- Browse and select PISA data files or folders
- Configure cleaning parameters with sliders and dropdowns
- Specify column names or use auto-detection
- Monitor processing progress in real-time
- View output folders and results

### Option 2: Command Line Script (Recommended for Automation)

Run the pipeline script with default settings:

```bash
python script.py
```

Process a specific folder or file with custom parameters:

```bash
python script.py -f raw_data/2018 -c MEX -scr PV1MATH -stu CNTSTUID -sch CNTSCHID
```

---

## Script Command Line Arguments

| Argument | Short | Type | Default | Description |
|----------|-------|------|---------|-------------|
| `--folder` | `-f` | str | `raw_data` | Root folder or single file to process |
| `--country_code` | `-c` | str | `MEX` | ISO country code to filter data |
| `--score_col` | `-scr` | str | Auto-detect | Name of the score column |
| `--student_id_col` | `-stu` | str | Auto-detect | Name of the student ID column |
| `--school_id_col` | `-sch` | str | Auto-detect | Name of the school ID column |
| `--missing_threshold` | `-mt` | float | `1.0` | Threshold to drop columns with missing values (0-1) |
| `--uniform_threshold` | `-ut` | float | `1.0` | Threshold to drop columns with uniform values (0-1) |
| `--save_unlabel` | `-s` | flag | `False` | Save intermediate unlabeled CSV files |

### Script Examples

**Process Mexican data from 2018:**
```bash
python script.py -f raw_data/2018 -c MEX
```

**Process with stricter cleaning thresholds:**
```bash
python script.py -f raw_data -mt 0.3 -ut 0.95
```

**Process a single file:**
```bash
python script.py -f raw_data/2018/CY07_MSU_STU_QQQ.sav -c USA
```

**Save intermediate unlabeled data:**
```bash
python script.py -f raw_data/2018 -c MEX -s
```

---

## Pipeline Workflow

1. **Load** – Reads `.sav` or `.csv` files and filters by country code
2. **Extract Labels** – Separates labeled (with metadata) and unlabeled data
3. **Clean** – Removes columns with excessive missing or uniform values
4. **Transform** – Applies feature engineering and data restructuring (optional)
5. **Save** – Exports cleaned datasets to organized folders

### Output Structure

```
raw_data/
└── 2018/
    ├── mexican/                          # Unlabeled data (if --save_unlabel used)
    ├── mexican_labeled/                  # Labeled raw data
    ├── mexican_labeled_cleaned/          # Cleaned data
    └── mexican_labeled_cleaned_leveled/  # Transformed data (if enabled)
```

---

## Programmatic Usage

You can also use the pipeline components directly in your Python code:

```python
from pisa_pipeline.data_processing.sav_loader import SAVloader
from pisa_pipeline.data_processing.cleaner import CSVCleaner
from pisa_pipeline.data_processing.transformer import Transformer

# Load data
loader = SAVloader()
df_labeled, df_unlabeled = loader.run("data.sav", country_code="MEX")

# Clean data
cleaner = CSVCleaner()
df_cleaned = cleaner.run(df_labeled, "student", ["CNTSTUID", "CNTSCHID"], 0.3, 0.95)

# Transform data
transformer = Transformer()
df_transformed = transformer.run({"student": df_cleaned}, score_col="PV1MATH")
```

---

## Goals

- Provide a reproducible workflow for analyzing PISA questionnaire data
- Standardize preprocessing across different PISA cycles (2015, 2018, 2022, etc.)
- Enable predictive modeling using educational variables
- Support multi-country comparative studies
- Make PISA data accessible to both technical and non-technical users

---

## Requirements

- Python 3.7+
- pandas
- numpy
- pyreadstat (for .sav file support)
- tkinter (usually included with Python, needed for GUI)

---

## License

IPN-CIC License

---

## Troubleshooting

**GUI won't launch:**
- Ensure tkinter is installed: `python -m tkinter`
- On Linux: `sudo apt-get install python3-tk`

**Script can't find files:**
- Check that file paths are correct
- Use absolute paths if relative paths don't work
- Verify file extensions match `.sav`, `.csv`, or `.text`

**Out of memory errors:**
- Process one year at a time
- Increase missing/uniform thresholds to reduce data size
- Use the script with specific file paths instead of entire folders
