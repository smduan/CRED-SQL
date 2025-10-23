import sys
import json
import csv
import re
import os

from llm import completion,completion_llama
from utils import prompt_create_table

SPIDER = "spider"
BIRD = "bird"
DEV_MODE = "dev"
TEST_MODE = "test"
GPT = "GPT-4o"
LLAMA = ""
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch
import argparse
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "max_split_size_mb:256"



global model_id
model_id = '/data/huggingface-models/Qwen/Qwen2.5-Coder-32B-Instruct'
global tokenizer
tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
global model
model = AutoModelForCausalLM.from_pretrained(model_id, use_flash_attention_2=True, torch_dtype=torch.bfloat16).cuda()

def completion(prompt):
    max_retries = 100
    retry_count = 0
    result = {}

    while retry_count < max_retries:
        try:
           
            messages = [
                {'role': 'user', 'content': prompt}
            ]

            inputs = tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(
                model.device)
            input_token_num = inputs.size(1)  
            outputs = model.generate(inputs, max_new_tokens=2000, do_sample=True, temperature=0.1, top_k=50,
                                     num_return_sequences=1, eos_token_id=tokenizer.eos_token_id,
                                     pad_token_id=tokenizer.eos_token_id)
            output_token_num = outputs.size(1) - input_token_num
            output_result = (tokenizer.decode(outputs[0][len(inputs[0]):], skip_special_tokens=True))
            #end_time = time.time()

            result = {}  

            
            result['prompt_tokens'] = input_token_num
            result['completion_tokens'] = output_token_num
            result['total_tokens'] = outputs.size(1)

            
            result['content'] = output_result

           

            break

        except Exception as e:
            print(f"An error occurred: {e}")
            retry_count += 1
            time.sleep(5)
            print(f"Retrying... (attempt {retry_count}/{max_retries})")

    return output_result


prompt = """
You are given a question and a list of databases with associated tables. Your task is to identify the most relevant database that contain the information needed to answer the question. The question will be related to specific topics that can be mapped to the tables provided. Select the database(s) that most likely contain the data needed to answer the question based on the keywords and context provided.

Question:
{Question}

Databases and Tables:
{Databases}

Output:
Please return the name of the single most relevant database in JSON format as follows:
{
  "selected_database": "database_name",
  "explanation": "Explain why this database was selected."
}
"""

def extract_db_ids(res,key="predicted_res"):
    if key == "predicted_res":
        db_ids = [item.split('&')[0] for item in res]
    elif key == "crush_pred":
        db_ids = set([item.split('.')[0] for item in res])

    # print(db_ids)
    return list(set(db_ids))

def extract_db_name(text):
    sql_query = text
    sql_match = re.search(r'"selected_database": "([^"]+)"', text, re.DOTALL)
    if sql_match:
        sql_query = sql_match.group(1).replace('\\n', '\n')
        sql_query = sql_query.replace("\n","")
    else:
        raise ValueError("extract_error")
    return sql_query

def get_bird_db(db_id):
    db = ""
    db_path = '/data/inspur/base_model_exp/test/Bird/dev_tables.json'
    db_data = json.load(open(db_path,'r'))
    for i,item in enumerate(db_data):
        if item['db_id'] == db_id:
            tables = prompt_create_table(item)
            db =  f"Tables in {db_id} db are below:\n\n" + tables
            # print(db)
            break
    if db == "":
        raise ValueError(f"db{db_id} not found!")
    return db

def get_db(db_id,dataset=BIRD,mode=DEV_MODE):
    db = ""
    if dataset == BIRD:
        db_path = "schema-choose/src/bird/dev_tables.json"
    elif dataset == SPIDER and mode == DEV_MODE:    
        db_path = "schema-choose/src/spider/tables.json"
    
    db_data = json.load(open(db_path,'r'))
    for i,item in enumerate(db_data):
        if item['db_id'] == db_id:
            tables = prompt_create_table(item)
            db =  f"Tables in {db_id} db are below:\n\n" + tables
            # print(db)
            break
    if db == "":
        raise ValueError(f"db:{db_id} not found!")
    return db
    


def choose(path,output_path,dataset="bird",mode="dev",model=GPT,key="predicted_res"):
    db_name = ""
    data = json.load(open(path,'r'))
    if os.path.exists(output_path):
        new_data = json.load(open(output_path,'r'))
    else:
        new_data = []
    
    for item in data:
        
        query = item["question"]
        res = item[key]
        db_ids = extract_db_ids(res,key)
        db_contents = [get_db(db_id,dataset,mode) for db_id in db_ids]
        db_str = "\n".join(db_contents)
        content = prompt.replace("{Question}",query).replace("{Databases}",db_str)
        result = completion(content)
        
        
        db_name = extract_db_name(result)
        item['pred db_id'] = db_name
        new_data.append(item)

    json.dump(new_data,open(output_path,'w'),indent=4)
    
    return db_name
    



def main():
    
    csv_rows = []
    with open('/data/inspur/wzr/schema_linking/result_bird.csv', 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            
            filtered_row = [item.strip() for item in row if item.strip() != '']
            
            replaced_row = [
                item.replace("california_schools_", "california_schools&")
                    .replace("student_club_", "student_club&")
                    .replace("card_games_", "card_games&")
                    .replace("european_football_2_", "european_football_2&")
                    .replace("formula_1_", "formula_1&")
                    .replace("debit_card_specializing_", "debit_card_specializing&")
                    .replace("financial_", "financial&")
                    .replace("thrombosis_prediction_", "thrombosis_prediction&")
                    .replace("superhero_", "superhero&")
                    .replace("toxicology_", "toxicology&")
                    .replace("codebase_community_", "codebase_community&")
                    
                    
                for item in filtered_row
            ]
            csv_rows.append(replaced_row)
    
    
    with open('schema_retrieval/evaluation/data/bird/bird_test_data.json', 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)
    

    if len(data) != len(csv_rows):
        raise ValueError(f"Row count mismatch: JSON has {len(data)} rows,CSV has{len(csv_rows)}rows")
    
    
    for i, item in enumerate(data):
        if i < len(csv_rows):  
            item["predicted_res"] = csv_rows[i]
    
    
    with open('schema_retrieval/evaluation/data/bird/bird_table_predict.json', 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=2, ensure_ascii=False)
    
    print("Successfully save in bird_table_predict.json")
    
    bird_path = "/data/inspur/wzr/schema_linking/evaluation/data/bird_show/bird_table_predict.json"
    output_bird_path = "/data/inspur/wzr/schema_linking/evaluation/data/bird_show/bird_qwen_choose_result.json"
    
    db_name_result = choose(bird_path,output_bird_path,BIRD,DEV_MODE,LLAMA)
    

if __name__ == "__main__":
    main()