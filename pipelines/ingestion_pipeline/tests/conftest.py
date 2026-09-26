
import os
import sys

_TRANSFORMATIONS_DIR = os.path.join(os.path.dirname(__file__), "..", "transformations")
sys.path.insert(0, os.path.abspath(_TRANSFORMATIONS_DIR))
