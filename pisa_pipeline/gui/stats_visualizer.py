"""
Statistics visualizer for column statistics system.
Displays numeric histograms and categorical value distributions.
"""

import tkinter as tk
from tkinter import ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from typing import Dict, Any


class StatsVisualizerFactory:
    """Factory class to display column statistics in a popup window"""

    @staticmethod
    def show_stats(parent, stats: Dict[str, Any]):
        """
        Display statistics window depending on column type.

        Args:
            parent: parent tkinter root or window
            stats: dictionary with computed column statistics
        """
        col_type = stats.get("type", "unknown")
        if col_type == "numeric":
            StatsVisualizerFactory._show_numeric_stats(parent, stats)
        elif col_type == "categorical":
            StatsVisualizerFactory._show_categorical_stats(parent, stats)
        else:
            StatsVisualizerFactory._show_generic_stats(parent, stats)

    # ------------------------------------------------------------------
    # Numeric visualization
    # ------------------------------------------------------------------
    @staticmethod
    def _show_numeric_stats(parent, stats: Dict[str, Any]):
        """Display numeric column stats with histogram and summary."""
        window = tk.Toplevel(parent)
        window.title(f"Statistics - {stats['name']} (Numeric)")
        window.geometry("700x500")

        # Top frame for numeric stats
        top_frame = ttk.Frame(window)
        top_frame.pack(side="top", fill="x", padx=10, pady=10)

        # Summary statistics
        summary = (
            f"Type: {stats['dtype']}\n"
            f"Mean: {stats['mean']:.4f} | Std: {stats['std']:.4f} | Var: {stats['variance']:.4f}\n"
            f"Min: {stats['min']:.4f} | Q25: {stats['q25']:.4f} | Median: {stats['median']:.4f} | "
            f"Q75: {stats['q75']:.4f} | Max: {stats['max']:.4f}\n"
            f"Missing: {stats['missing_count']} ({stats['missing_percentage']:.2f}%)"
        )

        ttk.Label(top_frame, text=summary, justify="left", font=("Consolas", 10)).pack(anchor="w")

        # Histogram
        hist_data = stats.get("histogram_data", {})
        counts = hist_data.get("counts", [])
        bin_edges = hist_data.get("bin_edges", [])

        if counts and bin_edges:
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.bar(bin_edges[:-1], counts, width=(bin_edges[1] - bin_edges[0]), edgecolor="black")
            ax.set_title(f"Distribution of {stats['name']}")
            ax.set_xlabel(stats["name"])
            ax.set_ylabel("Frequency")
            fig.tight_layout()

            canvas = FigureCanvasTkAgg(fig, master=window)
            canvas.draw()
            canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

    # ------------------------------------------------------------------
    # Categorical visualization
    # ------------------------------------------------------------------
    @staticmethod
    def _show_categorical_stats(parent, stats: Dict[str, Any]):
        """Display categorical column stats with bar chart of top values."""
        window = tk.Toplevel(parent)
        window.title(f"Statistics - {stats['name']} (Categorical)")
        window.geometry("700x500")

        # Summary section
        top_frame = ttk.Frame(window)
        top_frame.pack(side="top", fill="x", padx=10, pady=10)

        summary = (
            f"Type: {stats['dtype']}\n"
            f"Unique values: {stats['unique_count']}\n"
            f"Most common: {stats.get('most_common')} "
            f"({stats.get('most_common_count', 0)} occurrences)\n"
            f"Missing: {stats['missing_count']} ({stats['missing_percentage']:.2f}%)"
        )

        ttk.Label(top_frame, text=summary, justify="left", font=("Consolas", 10)).pack(anchor="w")

        # Value counts
        value_counts = stats.get("value_counts", {})
        if not value_counts:
            ttk.Label(window, text="No data available for visualization.").pack(pady=10)
            return

        items = list(value_counts.items())
        labels = [str(k) for k, _ in items]
        values = [int(v) for _, v in items]

        # Bar chart
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.barh(labels[::-1], values[::-1])
        ax.set_title(f"Top Values in {stats['name']}")
        ax.set_xlabel("Count")
        ax.set_ylabel("Category")
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=window)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)

    # ------------------------------------------------------------------
    # Fallback visualization for unknown types
    # ------------------------------------------------------------------
    @staticmethod
    def _show_generic_stats(parent, stats: Dict[str, Any]):
        """Fallback text display if type is unknown."""
        window = tk.Toplevel(parent)
        window.title(f"Statistics - {stats['name']}")
        window.geometry("600x400")

        text_widget = tk.Text(window, wrap="word")
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)

        for k, v in stats.items():
            text_widget.insert("end", f"{k}: {v}\n")

        text_widget.config(state="disabled")
