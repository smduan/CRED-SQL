#!/bin/bash

# Ensure the script stops if there is an error
set -e

rm -f evaluation/result.txt

echo "This is the evaluation result." > evaluation/result.txt


echo "Test the performance of 1,534 samples in the bird validation set"
python evaluation/evaluation.py --db_name "bird" --schema_file_path "evaluation/data/bird/schema.csv" --test_file_path "evaluation/data/bird/bird_test_data.json" --top_k "5,10" --need_vectorization True   


echo "All evaluations completed successfully!"
