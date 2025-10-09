docker run -d \
    --name weaviate1251 \
    -v /var/weaviate_data_1251/data:/var/lib/weaviate \
    -p 127.0.0.1:18081:18080 \
    -p 127.0.0.1:50052:50051 \
    --restart on-failure \
    -e QUERY_DEFAULTS_LIMIT=25 \
    -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
    -e PERSISTENCE_DATA_PATH='/var/lib/weaviate' \
    -e DEFAULT_VECTORIZER_MODULE=none \
    -e DISABLE_TELEMETRY=true \
    -e ENABLE_MODULES='text2vec-cohere,text2vec-huggingface,text2vec-palm,text2vec-openai,generative-openai,generative-cohere,generative-palm,ref2vec-centroid,reranker-cohere,qna-openai' \
    -e CLUSTER_HOSTNAME='node1' \
    semitechnologies/weaviate:1.25.1 \
    --host 0.0.0.0 \
    --port '18080' \
    --scheme http