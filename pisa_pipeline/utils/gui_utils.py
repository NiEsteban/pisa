import tkinter as tk
from tkinter import ttk

class TextRedirector:
    """Redirect stdout/stderr to a Tkinter Text widget."""
    def __init__(self, text_widget: tk.Text):
        self.text_widget = text_widget

    def write(self, message: str) -> None:
        if not message:
            return
        self.text_widget.config(state="normal")
        tag = "info"
        if "[ERROR]" in message or message.lower().startswith("error"):
            tag = "error"
        elif "[PIPELINE]" in message or message.lower().startswith("pipeline"):
            tag = "pipeline"
        if not message.endswith("\n"):
            message = message + "\n"
        try:
            self.text_widget.insert("end", message, tag)
        finally:
            self.text_widget.config(state="disabled")
            self.text_widget.see("end")

    def flush(self) -> None:
        pass


class MultiTextRedirector:
    """
    Redirect stdout/stderr to multiple Tkinter Text widgets based on context.
    Useful for tabbed interfaces where each tab has its own log.
    """
    def __init__(self):
        self.widgets = []  # List of (widget, condition_func) tuples
        self.default_widget = None

    def add_widget(self, text_widget: tk.Text, condition_func=None):
        """
        Add a text widget with an optional condition function.
        condition_func should return True when this widget should receive output.
        """
        self.widgets.append((text_widget, condition_func))

    def set_default(self, text_widget: tk.Text):
        """Set the default widget to use if no conditions match."""
        self.default_widget = text_widget

    def write(self, message: str) -> None:
        if not message:
            return
        
        # Find the appropriate widget
        target_widget = None
        for widget, condition in self.widgets:
            if condition and condition():
                target_widget = widget
                break
        
        # Fall back to default if no condition matched
        if target_widget is None and self.default_widget:
            target_widget = self.default_widget
        
        if target_widget is None:
            return  # No widget to write to
        
        # Write to the selected widget
        target_widget.config(state="normal")
        tag = "info"
        if "[ERROR]" in message or message.lower().startswith("error"):
            tag = "error"
        elif "[PIPELINE]" in message or message.lower().startswith("pipeline"):
            tag = "pipeline"
        
        if not message.endswith("\n"):
            message = message + "\n"
        
        try:
            target_widget.insert("end", message, tag)
        finally:
            target_widget.config(state="disabled")
            target_widget.see("end")

    def flush(self) -> None:
        pass