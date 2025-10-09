import time
from huggingface_hub import login, snapshot_download

login(token='')#personal huggingface token

snapshot_download(repo_id="BAAI/bge-m3",
                  local_dir='./bge-m3',
                  local_dir_use_symlinks=False,
                  resume_download=True,
                  allow_patterns=["*.json", "*.bin", "*.model", "*.md"],#,"*.safetensors"
                  max_workers=2)
