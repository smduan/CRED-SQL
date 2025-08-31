from prompt_formatters import TableColumn, Table, RajkumarFormatter, ForeignKey
import json
import os
import re
import  sqlite3
import pickle

def prompt_create_table(data,table_names=[]):
    '''
    CREATE TABLE Highschooler(
        ID int primary key,
        name text,
        grade int)
        CREATE TABLE Friend(
        student_id int,
        friend_id int,
        primary key (student_id,friend_id),
        foreign key(student_id) references Highschooler(ID),
        foreign key (friend_id) references Highschooler(ID)
        )
    CREATE TABLE Likes(
        student_id int,
        liked_id int,
        primary key (student_id, liked_id),
        foreign key (liked_id) references Highschooler(ID),
        foreign key (student_id) references Highschooler(ID)
        )
    -- Using valid SQLite, answer the following questions for the tables provided above.
    -- What is Kyle’s id?
    SELECT
    '''
    tables = []
    for table_idx, table_name in enumerate(data["table_names_original"]):
        if table_name not in table_names and table_names != []:
            continue
        columns = []
        for col_idx, (idx, col_name) in enumerate(data["column_names_original"]):
            if idx == table_idx:
                columns.append(TableColumn(name=col_name, dtype=data["column_types"][col_idx]))  

        primary_keys = []
        for idxs in data["primary_keys"]:
            if isinstance(idxs,list):
                for idx in idxs:
                    primary_keys.append(idx)
            else:
                primary_keys.append(idxs)
        
        pks = [
            next(col for col in columns if data["column_names_original"][pk_idx][1] == col.name)  
            for pk_idx in primary_keys
            if data["column_names_original"][pk_idx][0] == table_idx
        ]
        fks = [
            ForeignKey(
                column=next(
                    col for col in columns if data["column_names_original"][col_from_idx][1] == col.name),
                
                references_name=data["table_names_original"][data["column_names_original"][col_to_idx][0]],  
                references_column=TableColumn(name=data["column_names_original"][col_to_idx][1],           
                                              dtype=data["column_types"][col_to_idx])  
            )
            for col_from_idx, col_to_idx in data["foreign_keys"]
            if data["column_names_original"][col_from_idx][0] == table_idx
        ]
        table = Table(name=table_name, columns=columns, pks=pks, fks=fks)
        tables.append(table)

        # RajkumarFormatter
        formatter = RajkumarFormatter(tables)
        prompt_table = formatter.table_str

        prompt_table.replace("\n\n", "\n") 

    return  prompt_table

def create_prompt_schema(db_path,tables_dict={}):
    db_data = json.load(open(db_path,'r'))
    final_schema = []
    all_table_names = []
    for db_id in tables_dict.keys():
        table_names = tables_dict[db_id]
        for i,item in enumerate(db_data):
            if item['db_id'] == db_id:
                table = prompt_create_table(item,table_names)
                for table_name in table_names:
                    if table_name.upper() not in all_table_names:
                        all_table_names.append(table_name.upper())
                    else:
                        create_table = "CREATE TABLE " + table_name
                        new_create_table = 'CREATE TABLE ' + db_id + " " + table_name
                        table = table.replace(create_table,new_create_table)
                    
                final_schema.append(table)
                # print(table)
                break
    return "\n".join(final_schema)

def check_db_id(select_table_names:list,all_table_names:list,db_id):
    print(all_table_names)
    print(select_table_names)
    for t in select_table_names:
        if t not in all_table_names:
            raise ValueError(f"db_id {db_id} not include {t} table")
    return 

def create_table_columns(data,table_names=[],column_names={}):
    if column_names == []:
        return prompt_create_table(data,table_names)
    
    tables = []
    for table_idx, table_name in enumerate(data["table_names_original"]):
        if table_name not in table_names and table_names != []:
            continue
        columns = []
        for col_idx, (idx, col_name) in enumerate(data["column_names_original"]):
            if idx == table_idx:
                # 
                if column_names[table_name] == "keep_all":
                    columns.append(TableColumn(name=col_name, dtype=data["column_types"][col_idx]))  
                else:
                    if col_name in list(column_names[table_name]):
                        columns.append(TableColumn(name=col_name, dtype=data["column_types"][col_idx]))  

        primary_keys = []
        for idxs in data["primary_keys"]:
            if isinstance(idxs,list):
                for idx in idxs:
                    primary_keys.append(idx)
            else:
                primary_keys.append(idxs)
        
        pks = [
            next((col for col in columns if data["column_names_original"][pk_idx][1] == col.name), None) 
            for pk_idx in primary_keys
            if data["column_names_original"][pk_idx][0] == table_idx
        ]
        for pk in pks.copy():
            if pk == None:
                pks.remove(pk)

        _columns = [col.name for col in columns]
        fks = [
            ForeignKey(
                column=next(
                    col for col in columns if data["column_names_original"][col_from_idx][1] == col.name),
                
                references_name=data["table_names_original"][data["column_names_original"][col_to_idx][0]],  
                references_column=TableColumn(name=data["column_names_original"][col_to_idx][1],           
                                              dtype=data["column_types"][col_to_idx])  
            )
            for col_from_idx, col_to_idx in data["foreign_keys"]
            if data["column_names_original"][col_from_idx][0] == table_idx and data["column_names_original"][col_from_idx][1] in _columns
        ]
  
        table = Table(name=table_name, columns=columns, pks=pks, fks=fks)
        tables.append(table)

        # RajkumarFormatter
        formatter = RajkumarFormatter(tables)
        prompt_table = formatter.table_str

        prompt_table.replace("\n\n", "\n") 

    return  prompt_table


def get_columns_schema(db_path,db_id,column_dict):
    db_data = json.load(open(db_path,'r'))
    final_schema = ""
    for item in db_data:
        if item["db_id"] == db_id:
            all_table_names = item["table_names_original"]
            select_table_names = list(column_dict.keys())
            check_db_id(select_table_names,all_table_names,db_id)
            final_schema = create_table_columns(item,select_table_names,column_dict)
            print(final_schema)
            break
    return final_schema


def read_pkl_file(path):
    with open(path,'rb') as f:
        data=pickle.load(f)
    
    return data

if __name__ == "__main__":
    data = read_pkl_file("CRUSH_DATA/bird_dev_predicts_openai.pkl")
    db_path = 'Bird/dev_tables.json'
    
    schema_path = ''
    for pred in data:
        # tables = [".".join(item.split(".")[:2]) for item in pred[:10]]
        
        table_dict = {}
        # init table_dict
        for table in pred[:10]:
            db_id = table.split(".")[0]
            table_dict[db_id] = []
        # 
        for table in pred[:10]:
            db_id = table.split(".")[0]
            table_name = table.split(".")[1]
            if table_name not in table_dict[db_id]:
                table_dict[db_id].append(table_name)
        
        schema = create_prompt_schema(db_path,table_dict)
        
