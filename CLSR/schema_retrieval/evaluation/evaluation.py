import sys

sys.path.append('./')
from schema_linking.cschema_linking import CSchemaLinking
from embedding.cembedding import CEmbedding

import os
import pandas as pd
import argparse
import time
import json
from tqdm import tqdm


def compute_recall(ground_truth, predicted_res):
    if isinstance(ground_truth[0], str):
        ground_truth = [[item] for item in ground_truth]

    hits = 0
    for i, label in enumerate(ground_truth):
        if set(label) <= set(predicted_res[i]): 
        #if len(set(label) & set(predicted_res[i])) > 0:  
            hits += 1
    recall = hits / len(ground_truth)

    return recall

def evaluation(db_name, test_file_path, top_k):
    CEmbedding.init()
    CSchemaLinking.init()
    schema_linking = CSchemaLinking.build(db_name)

    s = time.time()
    res = []
    recall_table_num = max(top_k)
    with (open(test_file_path, 'r', encoding='utf-8') as file):
        data = json.load(file)
        ground_truth = []
        predicted_res = []
        for question in tqdm(data):
            query = question['question']
            if len(query) == 0:
                continue
            if len(question['table']) == 0:
                continue
            ground_truth.append(question['table'])
            result = schema_linking.search_schema(question=query, tok_k=recall_table_num)
            predicted_res.append([item["table_name"] for item in result])

        for k in top_k:
            recall = compute_recall(ground_truth=ground_truth, predicted_res=[item[:k] for item in predicted_res])
            res.append(recall)
            print("@{}Recall:{}".format(k, recall))

        e = time.time()
        print("Average inference time:{} s/question".format((e - s) / len(ground_truth)))

    CSchemaLinking.destroy()
    return res


def add_schemas(db_name, schema_file_path):
    '''
    Batch processing: Write all tables in the database to the vector database
    '''
    CEmbedding.init()
    CSchemaLinking.init()
    schema_linking = CSchemaLinking.build(db_name)

    df = pd.read_csv(schema_file_path)
    ddls = []
    for idx, row in df.iterrows():
        table_sql = row[df.columns[0]]
        ddls.append({"id": idx, "ddl": table_sql, "region":db_name})

    add_ret = schema_linking.add_schema_batch(ddls=ddls)
    assert add_ret is True
    CSchemaLinking.destroy()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Evaluation schema linking performance!')
    parser.add_argument('-d', '--db_name', type=str, help='Database name for vectorized storage', default="bird")
    parser.add_argument('-s', '--schema_file_path', type=str, help='chema file path, must be endwith .csv',
                        default="evaluation/data/bird/schema.csv")
    parser.add_argument('-t', '--test_file_path', type=str, help='Test dataset path',
                        default="evaluation/data/bird/bird_test_data.json")
    parser.add_argument('-k', '--top_k', type=str, help='Number list string  of recalled tables', default="1,5,10")
    parser.add_argument('-o', '--need_vectorization', type=bool, default=False,
                        help='False indicates that only effect testing is performed, without vectorization (the vectorization process has been completed beforehand), while True indicates that both vectorization and effect testing are carried out.')
    args = parser.parse_args()
    args.top_k = [int(i) for i in args.top_k.split(',')]

    if args.need_vectorization:
        print("Vectorized database:{}".format(args.db_name))
        s = time.time()
        add_schemas(db_name=args.db_name, schema_file_path=args.schema_file_path)
        print("Vectorization time:{}".format(time.time() - s))

    
    res = evaluation(db_name=args.db_name, test_file_path=args.test_file_path, top_k=args.top_k)
    print("Recall:", res)

    
    if os.path.exists("evaluation/result.txt"):
        with open("evaluation/result.txt", "a+", encoding="utf-8") as f:
            f.write("Result of file\'{}\':\n".format(args.test_file_path))
            for k, recall in zip(args.top_k, res):
                f.write("recall@{}={}\n".format(k, round(recall, 6)))
