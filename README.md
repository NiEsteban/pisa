# PISA Data Pipeline

A modular, robust, and user-friendly pipeline for processing PISA (Programme for International Student Assessment) datasets. Designed for researchers and data scientists to easily standardize, clean, and transform educational data for analysis and machine learning.

![App Screenshot Placeholder](docs/img/main_window.png)

## ✨ Key Features
- **Clean Architecture**: Modularity first, with strict separation between Logic and UI.
- **Smart ID Detection**: Auto-detects Score/School/Student columns (with Lock option).
- **Lazy Loading**: Handles giant file trees instantly without freezing.
- **Thread-safe**: Long operations run in background; UI remains responsive.

## 📸 Screenshots

| **Main Interface** | **Data Cleaning** |
|:---:|:---:|
| ![Main Window](docs/img/main_window.png) | ![Cleaning Settings](docs/img/cleaning_tab.png) |
| *Intuitive Tabbed Interface* | *Granular Threshold Controls* |

| **Terminal Output** | **File Tree** |
|:---:|:---:|
| ![Console Log](docs/img/console_log.png) | ![File Tree](docs/img/file_tree.png) |
| *Real-time Thread-safe Logging* | *Lazy-loaded Directory Store* |

---

## 🚀 Quick Start: Command Line Interface (CLI)

For researchers who prefer terminal commands or need to process data in batches, `script.py` is the power tool.

**Basic Usage:**
```bash
python script.py -f "raw_data/2018" -c MEX
```

**Advanced Usage:**
```bash
python script.py -f "raw_data" -mt 0.2 -ut 0.1 -ct 0.95 -sd
```

**Arguments Guide:**
-   `-f, --folder`: Input file or folder path (recursive detection).
-   `-s, --save_unlabel`: Save intermediate unlabeled CSVs.
-   `-c, --country_code`: Country code to filter (e.g., `MEX`, `USA`, `ESP`).
-   `-mt, --missing_threshold`: Drop columns with more than X% missing values (default: 1.0).
-   `-ut, --uniform_threshold`: Drop columns with more than X% same values (default: 1.0).
-   `-ct, --correlation_threshold`: Drop highly correlated columns (default: 1.0 = disabled).
-   `-sd, --split_dataset`: Enable splitting the dataset.
-   `-sr, --split_ranges`: Define split ranges (e.g., `"0:50, 50:100"`).
-   `--only-label` / `--only-clean` / `--only-transform`: Run specific steps only.

---

## 🛠️ Exhaustive GUI User Guide

Launch the app with:
```bash
python main.py
```

### 1. File Selection & Navigation
You can process data in two ways:
-   **Folder Mode**: Select a root folder (e.g., `2015/`). The app will intelligently find all relevant files inside it.
-   **File Mode**: Select specific files (Ctrl+Click or Shift+Click) in the file tree to process only those.

**Supported Formats:**
-   **Modern PISA**: `.sav` (SPSS Data Files).
-   **Legacy PISA (e.g., 2012)**: `.sps` (SPSS Syntax Files) paired with `.txt` data files.
    *   *Note: If you have a `.txt` data file, please select the corresponding `.sps` syntax file to load it correctly.*
-   **Processed Data**: `.csv` (Comma Separated Values).

### 2. Step 1: Load & Label
**Goal:** Convert raw binary data into human-readable CSVs.
> **⚠️ IMPORTANT:** You must always run this step first for new raw data.

-   **How to run:**
    1.  Select your raw data folder or specific `.sav`/`.sps` files.
    2.  Enter the **Country Code** (e.g., `MEX`) in the settings panel.
    3.  Click **"Load & Label"**.
-   **What happens:**
    -   Reads the raw file (handling both `.sav` and `.txt`+`.sps`).
    -   Filters only the rows for your selected country.
    -   Decodes all metadata (e.g., converts `1` → `Female`, `2` → `Male`).
    -   **Output:** Creates a `labeled/` folder containing the decoded CSVs.

### 3. Step 2: Clean
**Goal:** Improve data quality by removing garbage columns.
> **⚠️ INFO:** Operates on the CSV files generated in Step 1.

-   **Settings:**
    -   **Missing Threshold (0-1)**: Drop columns that are mostly empty (e.g., `0.9` drops columns with >90% missing data).
    -   **Uniform Threshold (0-1)**: Drop columns where everyone has the same answer (e.g., `0.95` drops if >95% values are identical).
    -   **Correlation Threshold (0-1)**: Drop columns that are redundant/duplicates (e.g., `0.99` drops one of a pair if they are 99% correlated).
-   **Output:** Creates a `cleaned/` folder with the refined datasets.

### 4. Step 3: Transform
**Goal:** Create the final "Master Table" for Machine Learning.
> **⚠️ INFO:** this leverages the cleaned files to merge students with their schools.

-   **How to run:**
    1.  Ensure you have processed Student (`STU`) and School (`SCH`) files.
    2.  (Optional) Verify the detected **ID Columns** (Student, School, Score) in the inputs are correct.
    3.  Click **"Transform"**.
-   **What happens:**
    -   **Merging**: Matches every Student to their School using the `School ID`.
    -   **Leveling**: Converts numerical scores into PISA Proficiency Levels (e.g., `Level 2`, `Level 3`).
    -   **Splitting (Optional)**: If checked, splits the dataset based on your ranges.
-   **Output:** Creates a `leveled/` folder with the final merged CSV.

### 5. Review & Manual Refinement
**Goal:** Inspect your data and manually drop specific columns.
-   **Visualizing**: Double-click ANY file in the tree (raw or processed) to see its columns in the right panel.
-   **Manual Drop**:
    1.  Check the boxes next to the columns you want to remove.
    2.  Click **"Drop Checked Columns"**.
    3.  The file is updated immediately.
-   **Undo**: Made a mistake? Click **"Undo Last Drop"** to restore the file instantly.

---

## 📦 Installation

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/NiEsteban/pisa.git
    cd pisa_clean
    ```

2.  **Install Dependencies**
    Ensure you have Python 3.10+ installed.
    ```bash
    pip install -r requirements.txt
    ```

---

## 📂 Output Structure

The pipeline uses a non-destructive folder hierarchy:

```text
MyData/
├── 2018/
│   ├── CY07_MST_STU_QQQ.sav        (Raw Input)
│   ├── labeled/
│   │   └── CY07_MST_STU_QQQ.csv    (Step 1 Output)
│   ├── cleaned/
│   │   └── CY07_MST_STU_QQQ.csv    (Step 2 Output)
│   └── leveled/
│       └── CY07_MST_STU_QQQ.csv    (Final Output)
```

---

## 🧩 Architecture

This project uses a clean **Service-Oriented Architecture** with a strict separation between UI and Logic:

-   **PipelineService**: The brain. Handles all data processing logic (loading, cleaning, transforming).
-   **PipelineActions**: The bridge. Connects the GUI to the Service and manages background threads.
-   **Utils**: Shared logic for File Scanning and Thread-safe Logging.
-   **ThreadSafeConsole**: The reporter. Uses the Logger utility to show logs safely in the GUI.

For a technical deep dive, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 📄 License
IPN-CIC License.
