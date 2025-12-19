# Software Architecture

## Purpose
The **PISA Pipeline** is a specialized tool designed to process international educational data from the Programme for International Student Assessment (PISA). It bridges the gap between raw, complex datasets (SPSS/CSV) and machine learning readiness.

Its core purposes are:
1.  **Standardization**: Providing a consistent cleaning and leveling process across different PISA cycles (2015, 2018, 2022).
2.  **Accessibility**: Enabling non-technical researchers to process data via a GUI.
3.  **Reproducibility**: Ensuring that data cleaning steps (missing value handling, drop logic) are deterministic and reproducible.

## High-Level Architecture
The application follows a **Layered Architecture** with strict separation between the User Interface (UI) and Business Logic.

```mermaid
graph TD
    User[User] --> GUI[GUI Layer (tkinter)]
    GUI --> Actions[PipelineActions]
    Actions --> Service[PipelineService]
    
    subgraph "Core Logic Layer"
        Service --> Loader[SAV/CSV Loader]
        Service --> Cleaner[Data Cleaner]
        Service --> Transformer[Data Transformer]
    end
    
    subgraph "Data Layer"
        Loader --> Files[(File System)]
        Cleaner --> Files
        Transformer --> Files
    end
```

## Module Descriptions

### 1. GUI Layer (`pisa_pipeline/gui/`)
-   **`main_window.py`**: The entry point for the UI. Manages the layout, tabs, and component initialization.
-   **`tree_file_manager.py`**: Manages the file system view using a `ttk.Treeview`. Focuses PURELY on UI state (nodes, selection), delegating all file scanning to `utils.file_scanner`.
-   **`thread_safe_console.py`**: A UI Bridge that polls the `LogQueue`. It no longer manages the queue itself but simply updates the text widget.
-   **`pipeline_actions.py`**: The "Controller" that bridges the GUI and the Service. It handles:
    -   Spawning worker threads (**Command/Worker Pattern**).
    -   Updating UI state (progress bars, logs).
    -   Showing dialogs.

### 2. Utils Layer (`pisa_pipeline/utils/`)
-   **`file_scanner.py`**: Encapsulates all file system operations.
    -   `FileSystemScanner`: Handles recursive scanning, filtering of ignored folders, and permission checks.
-   **`logger.py`**: Centralized logging logic.
    -   `LogQueue` (**Singleton**): Handles `sys.stdout` redirection and thread-safe message queuing. Decoupled from Tkinter.

### 3. Service Layer (`pisa_pipeline/data_processing/pipeline_service.py`)
-   **`PipelineService`**: A pure logic class that orchestrates the data processing. It knows *how* to process files but knows nothing about the GUI. It returns results (paths, dataframes) to the caller.

### 4. Data Processing Layer (`pisa_pipeline/data_processing/`)
-   **`sav_loader.py`**: Wrapper around `pyreadstat` to load SPSS files efficiently.
-   **`spss_loader.py`**: (**Adapter Pattern**) Custom parser that adapts legacy `.sps` + `.txt` files into a standard DataFrame.
-   **`cleaner.py`**: Contains functional cleaning algorithms.
-   **`transformer.py`**: Handles feature engineering.

## Design Patterns Used

### 1. Singleton Pattern
Used in `pisa_pipeline.utils.logger.LogQueue` to ensure a single logging queue instance exists across all threads, which `ThreadSafeConsole` consumes.

### 2. Adapter Pattern
`SPSSLoader` acts as an adapter, allowing the system to process legacy text-based PISA data (2012) using the same interface as modern `.sav` files.

### 3. Command / Worker Pattern
Long-running operations (Loading, Cleaning) are encapsulated in `worker()` functions and executed in separate `threading.Thread` instances. This prevents the Main UI Thread from freezing (ANR).

### 4. Facade / Service Pattern
`PipelineService` acts as a facade over the complex subsystems (`cleaner`, `loader`, `transformer`), providing a simple API (`clean_file`, `load_and_label`) for the UI to consume.

### 5. Observer Pattern
The `ThreadSafeConsole` observes the output streams and notifies the GUI text widget when new logs are available.

## File System Organization
The pipeline enforces a structured output format to maintain order:

-   `source_folder/`
    -   `labeled/`: Contains CSVs with decoded metadata.
    -   `cleaned/`: Contains data cleaned of missing/uniform values.
    -   `leveled/`: Contains final merged and transformed datasets ready for ML.
    -   `.backups/`: (Hidden) Stores temporary backups for Undo operations.
