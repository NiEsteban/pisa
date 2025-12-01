import re
import os
import inspect
def get_first_line(filepath):
    """Return the first line (stripped) of a file."""
    with open(filepath, "r", encoding="cp1252") as f:
        return f.readline().strip()

def read_lines(filepath):
    """Read all lines of a text file into a list."""
    with open(filepath, "r", encoding="cp1252") as f:
        return [line.strip() for line in f]

def extract_data_file_path(syntax_file):
    """
    Extract the data file path from a SPSS syntax file.
    Looks for a line starting with 'DATA LIST FILE'.
    Returns the path as a string or None if not found.
    """
    pattern = re.compile(r'DATA LIST FILE\s+"([^"]+)"', re.IGNORECASE)
    with open(syntax_file, "r", encoding="cp1252") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                return match.group(1).split("\\")[-1]
    return None

def resolve_folder_path(folder: str) -> str:
    """
    Convierte una ruta relativa a absoluta basada en el script que llama
    y verifica que la carpeta exista.

    Args:
        folder (str): Ruta relativa o absoluta de la carpeta.

    Returns:
        str: Ruta absoluta de la carpeta.

    Raises:
        FileNotFoundError: Si la carpeta no existe.
    """
    if not os.path.isabs(folder):
        # Carpeta del script que llamó a esta función
        caller_frame = inspect.stack()[1]
        caller_file = caller_frame.filename
        base_folder = os.path.dirname(os.path.abspath(caller_file))
        folder = os.path.join(base_folder, folder)

    if not os.path.exists(folder):
        raise FileNotFoundError(f"La carpeta especificada no existe: {folder}")
    return folder

def is_file(path:str):
    return os.path.isfile(path)
from pathlib import Path

def get_parent_folder(input_path: str) -> str | None:
    path = Path(input_path).resolve()

    # If it's a file
    if path.suffix:
        path = path.parent

    # Go up one level
    grandparent = path.parent

    # Return last folder if there's no folder above
    return str(grandparent) if grandparent != path.anchor else path
