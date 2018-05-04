# Quit the script if any part fails
set -e

echo "Running the tests"
pytest .

echo "Building the package"
pip wheel -w ./build .
