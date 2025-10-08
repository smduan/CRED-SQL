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
