import jieba
from transformers import AutoTokenizer, AutoModel
import os
import torch

class CEmbedding:
    model = None
    tokenizer = None
    device = None
    batch_size = 128

    @staticmethod
    def init():
        model_path = os.getenv("MODEL_PATH", "/app/models/bge-m3")
        if CEmbedding.device is None:
            # If there is a GPU, use GPU 0 by default.
            CEmbedding.device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        if CEmbedding.model is None:
            CEmbedding.model = AutoModel.from_pretrained(model_path, device_map=CEmbedding.device)
        if CEmbedding.tokenizer is None:
            CEmbedding.tokenizer = AutoTokenizer.from_pretrained(model_path)

    @staticmethod
    def destroy():
        CEmbedding.model = None
        CEmbedding.tokenizer = None
        CEmbedding.device = None

    @staticmethod
    def text2vec(text_list):

        """Convert text into vectors"""
        encoded_input = CEmbedding.tokenizer(text_list, padding=True, truncation=True, return_tensors='pt').to(
            CEmbedding.device)
        with torch.no_grad():
            model_output = CEmbedding.model(**encoded_input)
            
            embeddings = model_output[0][:, 0]
            # normalize
            embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
        return embeddings.cpu().numpy()

    @staticmethod
    def jieba_tokenize(text):
        
        if text is None or len(text.strip()) <= 0:
            return None
        return ' '.join(jieba.cut_for_search(text))


