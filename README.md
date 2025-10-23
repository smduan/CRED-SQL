# CRED-SQL
This is the official repository for the paper "CRED-SQL: Enhancing Real-world Large Scale Database Text-to-SQL Parsing through Cluster Retrieval and Execution Description". CRED-SQL is a Text-to-SQL framework designed for large-scale databases that addresses the semantic mismatch between natural language questions and SQL queries through Cluster Retrieval and Execution Description.

### Core Challenges
Traditional Text-to-SQL approaches face two major challenges in real-world large-scale scenarios:
Schema Mismatch: In databases with thousands of tables, existing retrieval techniques struggle to accurately identify relevant tables and columns
Semantic Deviation: Direct mapping from natural language questions to SQL queries involves significant semantic gaps

### Innovative Solutions
CRED-SQL overcomes these limitations through a two-stage approach:
#### 🔄 Cluster-based Large-scale Schema Retrieval (CLSR)
* Clusters tables and columns based on semantic similarity
* Dynamic attribute-weighting strategy that boosts relevant attributes
* Significantly improves schema selection accuracy in large-scale databases

#### 📝 Execution Description Language (EDL)
* Novel natural language intermediate representation describing SQL execution intent
* Decomposes Text-to-SQL into Text-to-EDL and EDL-to-SQL subtasks
* Better leverages LLMs' general reasoning capabilities while reducing semantic deviation

## CRED-SQL
<img width="1590" height="595" alt="image" src="https://github.com/user-attachments/assets/79edb5e3-df37-458c-b093-e8ff2538559e" />

## Environment
Clone the repository and install required packages:
```
pip install -r requirements.txt
```
## Run
1. Install Vector Database Weaviate（ https://weaviate.io/ ）
```
cd /weaviate
sh docker_pull.sh
sh docker_run.sh
```
2. Download bge-m3 and copy it to /var/rag/models
```
cd /models
mkdir -p /var/rag/models
python download_bge_m3.py
cp -r ./bge-m3 /var/rag/models
```
3. Start CLSR schema retrieval
```
cd /CLSR/schema_retrieval/evaluation
sh run_evaluation.sh
cd /CLSR/schema-choose/src/
python llm_schema_choose.py
```
4. NLQ-to-EDL and EDL-to-SQL
   
Through few-shot or fine-tuning LLMs, generate EDL first and then generate SQL based on the selected database schema and question.
   
The link to the EDL dataset: https://huggingface.co/datasets/ZR00/Spider_EDL, and https://huggingface.co/datasets/ZR00/Bird_EDL
   
The best-performing fine-tuned LLM on the Spider dataset is open-sourced as follows https://huggingface.co/ZR00/spider_qwen32b_train_q_to_edl_orischema and https://huggingface.co/ZR00/spider_qwen32b_train_edl_to_sql.
```
cd /CLSR/EDL-generation/
python generate_spider.py
cd /CLSR/sql_mapping/
python generate_spider_edl_to_sql.py
```

# Cite
```
@misc{duan2025credsqlenhancingrealworldlarge,
      title={CRED-SQL: Enhancing Real-world Large Scale Database Text-to-SQL Parsing through Cluster Retrieval and Execution Description}, 
      author={Shaoming Duan and Zirui Wang and Chuanyi Liu and Zhibin Zhu and Yuhao Zhang and Peiyi Han and Liang Yan and Zewu Penge},
      year={2025},
      eprint={2508.12769},
      archivePrefix={arXiv},
      primaryClass={cs.CL},
      url={https://arxiv.org/abs/2508.12769}, 
}
```
