# ------------------------------
# Imports
# ------------------------------
import os
import pytest
import sys

# Add the parent folder to the path to import the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../summer/"))

from summer import *


# ------------------------------
# Tests
# ------------------------------
def test_Package():
    assert(1 == 1)
