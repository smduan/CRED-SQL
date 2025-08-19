from transformers import AutoTokenizer, AutoModelForCausalLM
import torch
import json
import re
import os
import argparse

def generate_sql(model_id, message_path, log_path,output_sql_path):
    with open(message_path, encoding='utf-8') as f:
        messages = json.load(f)
    log = open(log_path, 'w')
    print(log_path)
    output_sql = open(output_sql_path,'w')
    i = 0
    tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(model_id, use_flash_attention_2=True, torch_dtype=torch.bfloat16).cuda()
    last_sql = ""
    data_rewrite = []
    for message in messages:
        
        
        messages=[
            { 'role': 'user', 'content': message['instruction']}
        ]
        inputs = tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(model.device)
        # tokenizer.eos_token_id is the id of <|EOT|> token
        outputs = model.generate(inputs, max_new_tokens=512, do_sample=False, top_k=50, top_p=0.95, num_return_sequences=1, eos_token_id=tokenizer.eos_token_id)

        result = (tokenizer.decode(outputs[0][len(inputs[0]):], skip_special_tokens=True))

        log.write( f'{i} --------------{message["instruction"]} \n' + result + '\n')
        sql = result
        try :
            if '```sql' in sql:
                sql = sql.split('```sql',1)[1].split('```',1)[0].strip()
            elif '```' in sql:
                sql = sql.split('```',1)[1].split('```',1)[0].strip()
        except Exception:
            print('err in ',i,sql)
        sql = re.sub(r"\s+", ' ', sql)  
        last_sql = sql
        output_sql.write( sql + '\n')
        item = {"index":i,"EDL": sql}
        data_rewrite.append(item)
        print(sql)
        i += 1
        # if i == 10:
        #     break
    log.close()
    output_sql.close()
    with open("q_to_EDL_result.json", "w") as f:
        json.dump(data_rewrite, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SQL Generation from Text')

    parser.add_argument('--model_id', type=str, required=True, help='The model ID to be used.')
    parser.add_argument('--message_path', type=str, required=True, help='Path to the JSON file containing messages.')
    parser.add_argument('--output_path', type=str, required=True, help='Path to save the log file.')
    parser.add_argument('--test_name', type=str, required=True,
                        help='Path to save the generated SQL files. The script will check if the directory exists.')

    args = parser.parse_args()
    print(args.output_path)
    if not os.path.exists(args.output_path):
        os.mkdir(args.output_path)
    print(args.output_path)
    generate_sql(
        model_id=args.model_id,
        message_path=args.message_path,
        log_path=f"{args.output_path}/{args.test_name}-log.txt",
        output_sql_path=f"{args.output_path}/{args.test_name}.txt"
    )