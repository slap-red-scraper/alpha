# Alpha Scraper

## Description
Alpha Scraper is an application designed to scrape websites for downline and bonus information. The scraped data is then stored locally in CSV files. The application features a graphical user interface for ease of use.

## Features
* Scrapes downline information from websites.
* Scrapes bonus and promotion details.
* Saves data in CSV format (`downlines.csv`, `bonuses.csv`).
* Configurable via `config.ini` for credentials, target URLs, and application settings.
* Graphical User Interface (GUI) for ease of use.
* Logging functionality for monitoring and debugging.
* Option to compile into a standalone executable.

## Technology Stack
* Python
* Kivy (for GUI)
* Requests (for HTTP communication)

## Getting Started

### Prerequisites
*   **Python 3:** Ensure Python 3 is installed and added to your system's PATH.
*   **pip:** The Python package installer, which usually comes with Python.

### Installation
1.  **Ensure you have the project files.** If you've cloned this repository, navigate to its root directory. If you downloaded it as a ZIP, extract it and navigate to the extracted folder.
2.  **Navigate to the project directory:** Open your terminal or command prompt and change to the root directory of the project.
3.  **(Recommended) Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    ```
    *   On Windows:
        ```bash
        .\venv\Scripts\activate
        ```
    *   On macOS/Linux:
        ```bash
        source venv/bin/activate
        ```
4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### Configuration
The application requires a `config.ini` file to store your credentials, target URLs, and other settings.

It's recommended to copy `config.example.ini` to `config.ini` (if an example file is provided) and update it with your details. Otherwise, create a new file named `config.ini` in the root directory of the project with the following structure:

```ini
[Credentials]
mobile = YOUR_MOBILE_NUMBER
password = YOUR_PASSWORD

[Settings]
url_file = urls.txt
downline_enabled = True

[Logging]
log_file = app.log
log_level = INFO
console = True
detail = False
```

**Explanation of `config.ini` settings:**

*   **`[Credentials]`**:
    *   `mobile`: Your mobile number used for logging into the website.
    *   `password`: Your password for the website.
*   **`[Settings]`**:
    *   `url_file`: The name of the file (e.g., `urls.txt`) in the root directory that contains the list of URLs to scrape, one URL per line.
    *   `downline_enabled`: Set to `True` to enable downline data scraping, `False` to disable.
*   **`[Logging]`**:
    *   `log_file`: The name of the file where logs will be saved (e.g., `app.log`).
    *   `log_level`: The logging level (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).
    *   `console`: Set to `True` to output logs to the terminal, `False` to disable.
    *   `detail`: Set to `True` for more detailed log messages, `False` for concise logs.

Make sure to replace placeholder values like `YOUR_MOBILE_NUMBER` and `YOUR_PASSWORD` with your actual credentials.

## Usage

### Running the Application
After completing the installation and configuration steps, you can start the application by running the following command from the project's root directory in your terminal:

```bash
python src/main.py
```

### User Interface
The application will launch a Graphical User Interface (GUI). The GUI allows you to initiate the scraping process and view progress and logs.

## Compilation (Building from Source)

### Overview
This project can be compiled into a standalone executable using PyInstaller. PyInstaller bundles the Python application and all its dependencies into a single package. It is listed in the `requirements.txt` file and should be installed when you set up the project dependencies.

### Compilation Steps
1.  **Ensure PyInstaller is installed:**
    If you haven't installed dependencies via `requirements.txt` or want to ensure PyInstaller is up-to-date, you can install it manually:
    ```bash
    pip install pyinstaller
    ```

2.  **Compile the application:**
    Navigate to the project's root directory in your terminal and run the following command:
    ```bash
    pyinstaller --onefile --windowed --name AlphaScraper --add-data "src:src" src/main.py
    ```

    **Explanation of PyInstaller flags:**
    *   `--onefile`: Bundles everything into a single executable file.
    *   `--windowed`: Prevents a terminal window from appearing when the GUI application is run. This is important for Kivy applications.
    *   `--name AlphaScraper`: Sets the name of the output executable (e.g., `AlphaScraper.exe` on Windows).
    *   `--add-data "src:src"`: This flag is crucial for Kivy applications. It tells PyInstaller to include all files from the `src` directory (like `.kv` files, `gui_theme_definition.txt`, images, etc.) into a directory named `src` within the bundled application. This ensures that the application can find its necessary resources when run as an executable. The format is `SOURCE:DESTINATION_IN_BUNDLE`.

    **Note on Kivy and Resources:** Kivy applications often rely on external files (e.g., `.kv` design files, images, fonts). If PyInstaller doesn't automatically include all necessary files, you might need to modify the `.spec` file that PyInstaller generates during its first run. The `--add-data` flag is a common way to explicitly include such files and directories.

### Output
After the compilation process completes, the standalone executable will be located in the `dist` directory, which PyInstaller creates in your project's root folder. For example, you will find `AlphaScraper.exe` (on Windows) or `AlphaScraper` (on macOS/Linux) inside the `dist` directory.

## Project Structure
```
project-root/
├── src/                    # Source code directory
│   ├── main.py             # Main application entry point, contains the Kivy App class, the Scraper class, and main execution logic.
│   ├── auth.py             # Handles authentication logic
│   ├── config.py           # Configuration loading logic (ConfigLoader class)
│   ├── logger.py           # Logging setup and utilities
│   ├── models.py           # Data models (e.g., Downline, Bonus dataclasses)
│   ├── gui.py              # Kivy GUI screen definitions and logic
│   ├── scraper.kv          # Kivy language file for GUI layout and styling
│   └── gui_theme_definition.txt # Defines theme elements for Kivy GUI
├── config.ini              # Configuration file (user-created, sensitive data)
├── requirements.txt        # Python package dependencies
├── urls.txt                # List of URLs to be processed (user-created or managed)
├── downlines.csv           # Output CSV file for downline data
├── bonuses.csv             # Output CSV file for bonus data
└── README.md               # This file
```

## Logging
The application implements a logging mechanism to record its operations and any potential errors encountered during runtime. This is crucial for monitoring the application's behavior and for troubleshooting issues.

The primary log output is directed to a file (e.g., `app.log`), the name and location of which can be configured in the `config.ini` file.

Logging behavior can be customized through the `[Logging]` section in `config.ini`:
*   **`log_file`**: Specifies the path to the log file where records will be stored.
*   **`log_level`**: Sets the minimum severity level for messages to be logged. Common levels include `DEBUG` (detailed information, typically of interest only when diagnosing problems), `INFO` (confirmation that things are working as expected), `WARNING` (an indication of an unexpected event or a problem that might occur in the future), and `ERROR` (due to a more serious problem, the software has not been able to perform some function).
*   **`console`**: A boolean value (`True` or `False`) that determines whether log messages should also be output to the terminal in addition to the log file.
*   **`detail`**: A boolean value (`True` or `False`) that controls the verbosity of log messages. If set to `True`, logs may include more detailed information such as timestamps, module names, or line numbers.

Reviewing these logs can provide insights into the scraping process, help identify patterns, and diagnose any problems that arise.
