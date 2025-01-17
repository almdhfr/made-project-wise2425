#!/bin/bash
# Run all Python tests for the project

echo "Running automated tests..."
python3 -m unittest discover -s project/testcase -p "test_pipeline.py"

if [ $? -eq 0 ]; then
  echo "All tests passed successfully!"
else
  echo "Some tests failed. Check the logs above for details."
  exit 1
fi

