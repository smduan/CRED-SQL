import json
from utils import create_prompt_schema, get_columns_schema


def get_table_dict(data, db_id):
    table_dict = {}
    table_names = data.keys()
    table_dict[db_id] = table_names
    return table_dict

def get_schema(CLSR_path,db_path,output_path):
    CLSR_data = json.load(open(CLSR_path,'r'))
    schema_data = []
    for item in CLSR_data:
        db_id = item['db_id']
        chosen = item['extracted_schema']
        for key, value in chosen.items():
            print(key, value)

        schema = get_columns_schema(db_path, db_id, chosen)
        item['input schema'] = schema
        schema_data.append(item)
        
    json.dump(schema_data,open(output_path,'w'),indent=4)

def get_choose_rate(CLSR_path,gold_path="output/gold_path.json"):
    gold_data = json.load(open(gold_path,'r'))
    CLSR_data = json.load(open(CLSR_path,'r'))
    assert len(gold_data) == len(CLSR_data),"Function: get_choose_rate ,length error!"
    num = 0
    for index,(g,d) in  enumerate(zip(gold_data,CLSR_data)):
        gold_tables = [t.split('&')[1] for t in g['table']]
        pred_tables = list(d["extracted_schema"].keys())
        pred_db = d['db_id']
        gold_db = g['db_id']
        
        if set(gold_tables) < set(pred_tables) and gold_db == pred_db:
            num +=1
            
        
    print(num/len(gold_data))
if __name__ == "__main__":

    spider_dev_db_path = 'spider/tables.json'
    spider_dev_path = 'spider/spider_dev_path.json'
    spider_dev_output_path = "output/spider_result.json"
    
    get_schema(spider_dev_path,spider_dev_db_path,spider_dev_output_path)
