# This file makes the tests directory a proper Python package 
import sys
import os

def setup_test_imports():
    """
    Set up the Python path for test imports.
    Call this function at the beginning of each test file.
    """
    # Get the absolute path to the backend directory (parent of tests)
    backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # Add it to the Python path
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    
# For backward compatibility, call the function when importing __init__
setup_test_imports()
