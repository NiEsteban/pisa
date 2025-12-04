import re
import pandas as pd

def detect_columns(df: pd.DataFrame, detect_math_level=False):
    """
    Detect key columns: score, school_id, student_id, leveled_score.
    - Keyword scoring: strong match = 2, weak match = 1
    - Tiebreak by max unique values
    - Automatically strips whitespace
    """

    def normalize(text: str) -> str:
        return re.sub(r"[^a-z0-9]", "", text.lower())

    def keep_words(text: str) -> str:
        """Lowercase but keep letters, numbers, and spaces as word separators."""
        return re.sub(r"[^a-z0-9 ]", " ", text.lower())

    def score_column(column: str, keywords):
        raw = keep_words(column)       # keeps word boundaries
        norm = normalize(column)       # compressed version
        score = 0

        for kw in keywords:
            kw_norm = normalize(kw)
            kw_raw = keep_words(kw)

            # Strong match: whole word
            if re.search(rf"\b{re.escape(kw_raw)}\b", raw):
                score += 2
                continue
            # Weak match: substring fallback
            if kw_norm in norm:
                score += 1

        return score

    def find_best_column(keywords):
        best_cols = []
        max_score = 0

        for col in df.columns:
            s = score_column(col, keywords)
            if s > max_score:
                max_score = s
                best_cols = [col]
            elif s == max_score and s > 0:
                best_cols.append(col)

        if max_score == 0:
            return None

        # Tiebreak by max unique values
        if len(best_cols) == 1:
            return best_cols[0]

        return max(best_cols, key=lambda c: df[c].nunique())

    # ---------------- DETECT ----------------
    score_col = find_best_column(["plausible", "math", "value"])
    school_col = find_best_column(["school", "id"])
    student_col = find_best_column(["student", "id"])

    # Strip whitespace
    score_col = score_col.strip() if score_col else None
    school_col = school_col.strip() if school_col else None
    student_col = student_col.strip() if student_col else None

    # ---------------- LEVELED SCORE ----------------
    leveled_score_col = None
    if detect_math_level and score_col:
        ns = normalize(score_col)
        for col in df.columns:
            if ns in normalize(col) and "level" in normalize(col):
                leveled_score_col = col.strip()
                break

        return score_col, school_col, student_col, leveled_score_col

    return score_col, school_col, student_col
