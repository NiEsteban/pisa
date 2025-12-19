import queue
import sys

class LogQueue:
    """
    Singleton-like helper to redirect stdout/stderr to a queue.
    Decouples logging mechanism from Tkinter.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LogQueue, cls).__new__(cls)
            cls._instance.queue = queue.Queue()
            cls._instance.is_redirected = False
        return cls._instance

    def write(self, msg):
        self.queue.put(msg)

    def flush(self):
        pass
    
    def get_queue(self) -> queue.Queue:
        return self.queue

    def redirect_sys_output(self):
        if not self.is_redirected:
            sys.stdout = self
            sys.stderr = self
            self.is_redirected = True
