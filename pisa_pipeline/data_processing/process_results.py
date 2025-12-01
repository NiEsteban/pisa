import os
import re
import pandas as pd
from typing import List, Tuple, Dict, Any, Optional


def clean_column_name(name: str) -> str:
    """Remove control characters and clean spacing."""
    name = re.sub(r"(\\v|[\x00-\x1F]|\\)+", "", name)
    return name.strip()


def parse_ranking_file(filepath: str) -> List[Tuple[float, int, str]]:
    """Parse a ranking file and normalize scores."""
    pattern = re.compile(r"([\d.]+)\s+(\d+)\s+(.+)")
    with open(filepath, "r") as f:
        matches = pattern.findall(f.read())

    if not matches:
        return []

    raw = [(float(m[0]), int(m[1]), m[2]) for m in matches]
    max_score = max(score for score, _, _ in raw)
    return [(score / max_score, rank, name) for score, rank, name in raw]


def extract_selection_attributes(filepath: str) -> List[Tuple[str, int]]:
    """Extract selected attributes (name, id) from Weka selection files."""
    selections = []
    with open(filepath, "r") as f:
        text = f.read()
    blocks = re.findall(r"Selected attributes:[\s\S]*?(?=\n\n|$)", text)
    for block in blocks:
        lines = block.splitlines()
        if not lines:
            continue
        ids = [int(x) for x in re.findall(r"\d+", lines[0])]
        names = [line.strip() for line in lines[1:] if line.strip()]
        for i, name in enumerate(names):
            attr_id = ids[i] if i < len(ids) else 0
            selections.append((name, attr_id))
    return selections


# ======================================================
# --- Core Single Context Logic ---
# ======================================================
def process_single_context(results_dir: str,
                           result_name: str,
                           ranking_filters: List[str],
                           selection_filters: List[str],
                           scoring_weights: List[float]) -> Dict[str, Any]:
    """
    Process one dataset context using result_name (e.g. STU2015).
    Looks for ranking and selection files containing that substring.
    """
    # 1️⃣ Find ranking files for this dataset
    ranking_files = [
        os.path.join(results_dir, f)
        for f in os.listdir(results_dir)
        if result_name.lower() in f.lower() and any(f.upper().endswith(rf.upper()) for rf in ranking_filters)
    ]
    if not ranking_files:
        print(f"⚠ No ranking files found for '{result_name}'")
        return {}

    # 2️⃣ Parse and normalize rankings
    rankings = [parse_ranking_file(f) for f in ranking_files]
    lookup = [{n: s for s, _, n in r} for r in rankings]
    all_names = {n for r in rankings for _, _, n in r}
    mean_scores = []
    for name in all_names:
        vals = [d.get(name) for d in lookup if name in d]
        mean = sum(vals) / len(vals)
        mean_scores.append((mean, len(mean_scores) + 1, name))
    mean_rankings = sorted(mean_scores, key=lambda x: x[0], reverse=True)

    # 3️⃣ Collect selection results (subset, wrapper)
    selections = [[] for _ in selection_filters]
    for filename in sorted(os.listdir(results_dir)):
        lower = filename.lower()
        if result_name.lower() not in lower:
            continue
        for sel_idx, sel_filter in enumerate(selection_filters):
            if sel_filter in lower:
                attrs = extract_selection_attributes(os.path.join(results_dir, filename))
                selections[sel_idx].extend(attrs)

    # 4️⃣ Compute final scores
    final_scores = []
    for score, attr_id, name in mean_rankings:
        final = score * scoring_weights[0]
        for sel_idx, sel_list in enumerate(selections):
            if any(name == s_name for s_name, _ in sel_list):
                final += scoring_weights[min(sel_idx + 1, len(scoring_weights) - 1)]
        final_scores.append((final, attr_id, name))
    final_scores.sort(key=lambda x: x[0], reverse=True)

    return {
        "result_name": result_name,
        "mean_rankings": mean_rankings,
        "selections": selections,
        "final_rankings": final_scores,
    }


def build_selected_dataframe(final_rankings: List[Tuple[float, int, str]],
                             essentials: List[str],
                             dataset_path: str,
                             num_selected: int = 20) -> Optional[pd.DataFrame]:
    """Build filtered DataFrame for the given dataset path."""
    if not os.path.exists(dataset_path):
        print(f"⚠ Dataset file not found: {dataset_path}")
        return None

    df = pd.read_csv(dataset_path, encoding="cp1252")
    df.columns = [clean_column_name(c) for c in df.columns]

    selected = [a[2] for a in final_rankings[:num_selected]]
    essentials_clean = [clean_column_name(a) for a in essentials]
    all_selected = [c for c in (selected + essentials_clean)]
    matched = [c for c in all_selected if c in df.columns]

    return df[matched].copy()


# ======================================================
# --- Main Class ---
# ======================================================
class ProcessResults:
    def __init__(self):
        pass

    def run(self,
            results_dir: str,
            dataset_path: str,
            essentials: Dict[str, List[str]],
            ranking_filters: List[str] = ("CORR", "GAIN", "RELIEFF"),
            selection_filters: List[str] = ("subset", "wrapper"),
            scoring_weights: List[float] = (0.25, 0.30, 0.45),
            num_selected: int = 20,
            results_name: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run ranking + selection pipeline for one or multiple datasets.
        - results_name: list of dataset substrings (e.g. ["STU2015", "SCH2015"])
        - dataset_path: path to CSV file (if only one dataset), or directory if multiple.
        """
        all_results = {}
        summary_rows = []

        if results_name is None:
            inferred = os.path.splitext(os.path.basename(dataset_path))[0]
            results_name = [inferred]
            print(f"📁 Auto-inferred result name: {inferred}")

        for name in results_name:
            print(f"\n🔹 Processing dataset: {name}")
            context = process_single_context(
                results_dir, name, ranking_filters, selection_filters, scoring_weights
            )
            if not context:
                print(f"⚠ No results for {name}")
                continue

            df_selected = build_selected_dataframe(
                context["final_rankings"],
                essentials.get(name, []),
                dataset_path,
                num_selected
            )

            summary = pd.DataFrame([
                {"Rank": i + 1, "Score": round(s, 4), "Attribute_ID": a, "Attribute_Name": n}
                for i, (s, a, n) in enumerate(context["final_rankings"][:num_selected])
            ])
            
            all_results[name] = {
                **context,
                "selected_df": df_selected,
                "summary": summary,
            }

            summary_rows.append({"Dataset": name, "Top Attribute": summary.iloc[0]["Attribute_Name"]})
            
            print(f"✅ Completed processing for {name}")
            print(f"   Top attribute: {summary.iloc[0]['Attribute_Name']}")
            print(f"   Total attributes ranked: {len(context['final_rankings'])}")

        # Create overall summary DataFrame
        overall_summary = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame()

        # Return results for ALL datasets
        return {
            "all_results": all_results,
            "overall_summary": overall_summary,
            "final_rankings": {name: res["final_rankings"] for name, res in all_results.items()},
            "summaries": {name: res["summary"] for name, res in all_results.items()},
            "selected_dfs": {name: res["selected_df"] for name, res in all_results.items()}
        }