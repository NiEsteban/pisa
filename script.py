import os
import argparse
from pisa_pipeline.data_processing.spss_loader import SPSSloader
from pisa_pipeline.data_processing.transformer import Transformer
from pisa_pipeline.data_processing.sav_loader import SAVloader
from pisa_pipeline.data_processing.cleaner import CSVCleaner
from pisa_pipeline.data_processing.process_results import ProcessResults
from pisa_pipeline.utils.file_utils import resolve_folder_path, is_file
from pisa_pipeline.utils.io import save_dataframe_to_csv

# -------------------------------------------------------------------------
# Global constants
# -------------------------------------------------------------------------
folder_extracted = "mexican"
folder_labeled = "mexican_labeled"
folder_cleaned = "mexican_labeled_cleaned"
folder_leveled = "mexican_labeled_cleaned_leveled"
type_file = [".sav", ".text", ".csv"]


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def filebasename(filepath: str) -> str:
    """
    Return a shrinked base name for a file.
    - If the filename contains at least 2 underscores, return the last two parts joined with '_'.
    - Otherwise, return the standard base name without extension.
    """
    base = os.path.basename(filepath)  # strip folder path
    name_parts = os.path.splitext(base)[0].split("_")  # remove extension, split by '_'

    if len(name_parts) >= 2:
        shrinked_name = "_".join(name_parts[-2:])
    else:
        shrinked_name = name_parts[0]

    return shrinked_name

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main(save_unlabel=False, transform=False, folder="raw_data",
         score_col=None, school_id_col=None, student_id_col=None,country_code="MEX",
         missing_threshold=1, uniform_threshold=1):

    folder = resolve_folder_path(folder)
    print(f"[INFO] Using folder/file: {folder}")

    if is_file(folder):
        # Case: single file provided
        print("[INFO] Detected a single file input")
        process_file(
            folder, 0, save_unlabel,
            score_col, school_id_col, student_id_col,
            country_code,
            missing_threshold, uniform_threshold
        )
        return

    # List folder contents
    items = [os.path.join(folder, f) for f in os.listdir(folder)]
    subfolders = [f for f in items if os.path.isdir(f)]
    files = [f for f in items if os.path.isfile(f)]

    # Case A: Folder with year subfolders
    if subfolders and all(os.path.basename(sf).isdigit() for sf in subfolders):
        print("[INFO] Detected structure: year subfolders")
        for year_folder in subfolders:
            year = int(os.path.basename(year_folder))
            process_year_folder(
                year_folder, year, save_unlabel, transform,
                score_col, school_id_col, student_id_col,
                country_code,
                missing_threshold, uniform_threshold
            )

    # Case B: Folder name is a year or contains files → treat as year folder
    elif os.path.basename(folder).isdigit() or files:
        if not files:
            print(f"[WARN] Year folder {folder} contains no files.")
            return
        print(folder)
        year = int(os.path.basename(folder)) if os.path.basename(folder).isdigit() else 0
        print(f"[INFO] Detected structure: year folder with files ({year})")
        process_year_folder(
            folder, year, save_unlabel, transform,
            score_col, school_id_col, student_id_col,
            country_code,
            missing_threshold, uniform_threshold
        )

    else:
        print("[INFO] Warning: No files or subfolders found.")



# -------------------------------------------------------------------------
# Helper: Process one year folder
# -------------------------------------------------------------------------
def process_year_folder(folder_year, year, save_unlabel, transform,
                        score_col, school_id_col, student_id_col,code_country, missing_threshold, uniform_threshold):

    print(f"[INFO] Processing year {year} in folder {folder_year}")
    dict_df ={}
    for file in os.listdir(folder_year):
        if not file.endswith(tuple(type_file[:2])):
            continue

        file_path = os.path.join(folder_year, file)
        print(f"[INFO] Processing file: {file}")

        base_name = filebasename(file)
        
        df, score_col_i, school_id_col_i, student_id_col_i = process_file(file_path,year,save_unlabel,score_col, 
                                                                   school_id_col, student_id_col,code_country, missing_threshold, uniform_threshold)
        if df is None:
            continue
        
        if score_col is None and score_col_i is not None:
            score_col = score_col_i
        if school_id_col is None and school_id_col_i is not None:
            school_id_col = school_id_col_i
        if student_id_col is None and student_id_col_i is not None:
            student_id_col = student_id_col_i

        dict_df[base_name] = df
    
    # --- Transform ---
    if transform:
        transformer = Transformer()
        dict_df = transformer.run(
            dict_df,
            score_col=score_col,
            ids_col=[student_id_col,school_id_col]
        )
    for name, df_t in dict_df.items():
        save_dataframe_to_csv(df_t, f"{folder_year}/{folder_leveled}/{name}_{year}.csv")


# -------------------------------------------------------------------------
# Helper: flat folder case
# -------------------------------------------------------------------------
def process_file(file_path, year, save_unlabel,
                            score_col, school_id_col, student_id_col, 
                            country_code,
                            missing_threshold=.1, uniform_threshold=.1):
        print(f"[INFO] Processing file: {file_path}")
        folder = os.path.dirname(file_path)
        base_name = filebasename(file_path)

        # --- Load ---
        loader = SAVloader()
        df_labeled, df_unlabeled = loader.run(file_path,country_code)
        if df_labeled is None:
            print(f"[INFO] Skipping {os.path.basename(file_path)} (no labeled data, code ={country_code} ).")
            return None, None, None, None

        # --- Save intermediate data ---
        if save_unlabel:
            save_dataframe_to_csv(df_unlabeled, f"{folder}/{folder_extracted}/{base_name}_{year}.csv")

        save_dataframe_to_csv(df_labeled, f"{folder}/{folder_labeled}/{base_name}_{year}.csv")

        # --- Clean ---
        cleaner = CSVCleaner()
        df_cleaned = cleaner.run(
            df_labeled,
            base_name,
            [student_id_col, school_id_col],
            missing_threshold, uniform_threshold
        )
        save_dataframe_to_csv(df_cleaned, f"{folder}/{folder_cleaned}/{base_name}_{year}.csv")
        auto_score, auto_school, auto_student = cleaner.detect_columns(df_cleaned)
        score_col = score_col or auto_score
        school_id_col = school_id_col or auto_school
        student_id_col = student_id_col or auto_student

        return df_cleaned, score_col, school_id_col, student_id_col
# -------------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de datos PISA")
    parser.add_argument(
        "-s", "--save_unlabel",
        action="store_true",
        help="Guardar el CSV sin etiquetas después de cargarlo"
    )
    parser.add_argument(
        "-f", "--folder",
        type=str,
        default="raw_data",
        help="Root folder of the raw data (years or flat)"
    )
    parser.add_argument("-scr","--score_col", type=str, help="Name of the score column")
    parser.add_argument("-sch","--school_id_col", type=str, help="Name of the school ID column")
    parser.add_argument("-stu","--student_id_col", type=str, help="Name of the student ID column")
    parser.add_argument("-c","--country_code", type=str,default="MEX", help="Code of the country of students ")
    parser.add_argument("-mt","--missing_threshold", type=float, default=1, help="Threshold to suppress columns with average of missing value above")
    parser.add_argument("-ut","--uniform_threshold", type=float, default=1,help="hreshold to suppress columns with same value above, regarless of missing value")

    args = parser.parse_args()
    main(
        folder=args.folder,
        save_unlabel=args.save_unlabel,
        score_col=args.score_col,
        school_id_col=args.school_id_col,
        student_id_col=args.student_id_col,
        country_code = args.country_code,
        missing_threshold=args.missing_threshold,
        uniform_threshold=args.uniform_threshold
    )
