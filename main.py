#!/usr/bin/env python3
"""Entry point for PISA Stepwise Pipeline"""
import tkinter as tk
from pisa_pipeline.gui.main_window import StepwisePipelineGUI

def main():
    root = tk.Tk()
    app = StepwisePipelineGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
    