import sys
import math
import json
import os

CACHE_FILE_PATH = "data/run_metrics_cache.json"

def load_run_cache():
    """
    Loads run metrics cache from a JSON file.
    Returns default structure if file not found or JSON is invalid.
    """
    default_cache = {"total_script_runs": 0, "sites": {}}
    if not os.path.exists(CACHE_FILE_PATH):
        print(f"Info: Cache file '{CACHE_FILE_PATH}' not found. Returning default cache.")
        return default_cache
    try:
        with open(CACHE_FILE_PATH, 'r') as f:
            data = json.load(f)
            # Basic validation for expected top-level keys
            if "total_script_runs" not in data or "sites" not in data:
                print(f"Warning: Cache file '{CACHE_FILE_PATH}' is missing expected keys. Returning default cache.")
                return default_cache
            return data
    except FileNotFoundError: # Should be caught by os.path.exists, but good for robustness
        print(f"Info: Cache file '{CACHE_FILE_PATH}' not found (FileNotFoundError). Returning default cache.")
        return default_cache
    except json.JSONDecodeError:
        print(f"Warning: Cache file '{CACHE_FILE_PATH}' contains invalid JSON. Returning default cache.")
        return default_cache
    except Exception as e:
        print(f"Warning: An unexpected error occurred while loading cache file '{CACHE_FILE_PATH}': {e}. Returning default cache.")
        return default_cache

def save_run_cache(data):
    """
    Saves run metrics cache to a JSON file.
    Ensures the directory exists.
    """
    try:
        os.makedirs(os.path.dirname(CACHE_FILE_PATH), exist_ok=True)
        with open(CACHE_FILE_PATH, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        # In a real app, this might go to a logger if available
        print(f"Error: Could not save cache file '{CACHE_FILE_PATH}': {e}")


def progress(value, length=40, title=" ", vmin=0.0, vmax=1.0):
    """
    Text progress bar

    Parameters
    ----------
    value : float
        Current value to be displayed as progress
    vmin : float
        Minimum value
    vmax : float
        Maximum value
    length: int
        Bar length (in character)
    title: string
        Text to be prepend to the bar
    """
    # Block progression is 1/8
    blocks = ["", "▏","▎","▍","▌","▋","▊","▉","█"]
    vmin = vmin or 0.0
    vmax = vmax or 1.0
    lsep, rsep = "▏", "▕" # Changed from "▏", "▕" to avoid potential rendering issues with some fonts for lsep. Using simple pipe.

    # Normalize value
    value = min(max(value, vmin), vmax)
    value = (value - vmin) / float(vmax - vmin) if (vmax - vmin) != 0 else 0.0 # Avoid division by zero

    v = value * length
    x = math.floor(v)  # integer part
    y = v - x  # fractional part
    base = 0.125  # 0.125 = 1/8
    prec = 3
    # Ensure index i is within the bounds of the blocks list
    i = min(len(blocks) - 1, int(round(base * math.floor(float(y) / base), prec) / base if base != 0 else 0)) # Avoid division by zero

    bar = "█" * x + blocks[i]
    n = length - len(bar)
    bar = lsep + bar + " " * n + rsep

    # Prepare the string but do not print it directly.
    # The caller in main.py will handle the actual printing.
    # This makes the utility function more flexible.
    # Return the formatted string instead of printing.
    return f"{title}{bar} {value*100:.1f}%"

if __name__ == '__main__':
    import time

    for i in range(1001): # Test up to 1000
        # progress_str = progress(i, vmin=0, vmax=1000, length=50, title="Test Progress: ")
        # sys.stdout.write("\r" + progress_str)
        # sys.stdout.flush()
        # The function now returns the string, so main would do:
        sys.stdout.write("\r" + progress(i, vmin=0, vmax=1000, length=50, title="Test Progress: "))
        sys.stdout.flush()
        time.sleep(0.0025)
    sys.stdout.write("\n")
