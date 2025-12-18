import queue
import tkinter as tk
import sys
import threading

class ThreadSafeConsole:
    """
    Singleton-like helper to redirect stdout/stderr to a queue,
    and allow a Tkinter widget to poll that queue safely.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ThreadSafeConsole, cls).__new__(cls)
            cls._instance.log_queue = queue.Queue()
            cls._instance.target_widget = None
            cls._instance.is_redirected = False
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

    def write(self, msg):
        self.log_queue.put(msg)

    def flush(self):
        pass

    def start_polling(self, root: tk.Tk, interval_ms=100):
        """Start the periodic poll of the queue."""
        def poll():
            try:
                # Check 1: Is the root widget still alive?
                try:
                    if not root.winfo_exists():
                        return
                except (tk.TclError, RuntimeError):
                    return

                while not self.log_queue.empty():
                    try:
                        msg = self.log_queue.get_nowait()
                        self._safe_append(msg)
                    except queue.Empty:
                        break
                
                # Check 2: Schedule next poll only if root is alive and no error occurred
                try:
                    if root.winfo_exists():
                        root.after(interval_ms, poll)
                except (tk.TclError, RuntimeError):
                    pass
            except Exception:
                # Last resort to prevent "invalid command name" noise on shutdown
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

    def redirect_sys_output(self):
        if not self.is_redirected:
            sys.stdout = self
            sys.stderr = self
            self.is_redirected = True
