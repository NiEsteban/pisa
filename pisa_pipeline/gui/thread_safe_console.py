import queue
import tkinter as tk
import sys
import threading
from pisa_pipeline.utils.logger import LogQueue

class ThreadSafeConsole:
    """
    UI Bridge that polls the thread-safe LogQueue and updates the Tkinter widget.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ThreadSafeConsole, cls).__new__(cls)
            cls._instance.logger = LogQueue()
            cls._instance.target_widget = None
        return cls._instance

    def set_target(self, widget: tk.Text):
        """Deprecated: Use set_target_resolver for dynamic targeting."""
        self.target_widget = widget

    def set_target_resolver(self, resolver_func):
        """
        Set a function that returns the target tk.Text widget 
        based on current application state (e.g. active tab).
        """
        self.target_resolver = resolver_func

    # Delegate write/flush/redirect to Logger
    def write(self, msg):
        self.logger.write(msg)

    def flush(self):
        self.logger.flush()

    def redirect_sys_output(self):
        self.logger.redirect_sys_output()

    def start_polling(self, root: tk.Tk, interval_ms=100):
        """Start the periodic poll of the queue."""
        def poll():
            try:
                # Check if root is still alive
                if not root.winfo_exists():
                    return
                
                # Check queue
                q = self.logger.get_queue()
                while not q.empty():
                    try:
                        msg = q.get_nowait()
                        self._safe_append(msg)
                    except queue.Empty:
                        break
                
                # Schedule next
                root.after(interval_ms, poll)
                
            except (tk.TclError, RuntimeError, AttributeError):
                # Application is closing or closed.
                # "invalid command name" comes from Tcl when the widget is gone.
                # Safe to ignore and stop polling.
                return
            except Exception:
                pass
        
        # Initial kick-off
        try:
            root.after(interval_ms, poll)
        except tk.TclError:
            pass

    def _safe_append(self, msg):
        """Append to widget. Must call in main thread."""
        target = None
        if hasattr(self, 'target_resolver') and self.target_resolver:
            try:
                target = self.target_resolver()
            except:
                target = self.target_widget
        else:
            target = self.target_widget

        if not target: 
            return
        
        try:
            target.config(state="normal")
            
            # Simple tagging logic
            tag = "info"
            lower_msg = msg.lower()
            if "[error]" in lower_msg or "error" in lower_msg.split():
                tag = "error"
            elif "[pipeline]" in lower_msg:
                tag = "pipeline"
                
            target.insert("end", msg, tag)
            target.see("end")
            target.config(state="disabled")
        except tk.TclError:
            # Widget might be destroyed
            pass
