import heapq
import os
import time
import weaviate
from weaviate import WeaviateClient
from weaviate.collections.classes.config import Property, DataType
from weaviate.collections.classes.filters import Filter
from weaviate.collections.classes.grpc import MetadataQuery
from weaviate.classes.query import HybridFusion
from weaviate.config import AdditionalConfig, Timeout

from embedding.cembedding import CEmbedding
from exceptions import RagArgumentsError, RagTableAlreadyExistError, RagKeyReferenceMissingError, RagDeleteSchemaError
from rag_logger import RAGLogger
from return_obj import RetObj
from schema_linking.cschema_parser import SchemaParse

logger = RAGLogger.get_logger("RAG")


class CSchemaLinking:
    Client: WeaviateClient = None
    cluster_similarity_threshold = 0.7
    search_similarity_threshold = 0.50

    def __init__(self, region_name, db_info_collection, table_collection=None, column_collection=None):
        self.region_name = region_name
        self.db_info_collection = db_info_collection
        self.table_collection = table_collection
        self.column_collection = column_collection

    @staticmethod
    def init(http_host="127.0.0.1", http_port=8010, grpc_host="127.0.0.1", grpc_port=8012):
        CSchemaLinking.Client = weaviate.connect_to_custom(
            http_host=os.getenv("WSC_HTTP_HOST", http_host),
            http_port=os.getenv("WSC_HTTP_PORT", http_port),
            http_secure=False,
            grpc_host=os.getenv("WSC_GRPC_HOST", grpc_host),
            grpc_port=os.getenv("WSC_GRPC_PORT", grpc_port),
            grpc_secure=False,
            auth_credentials=None,
            additional_config=AdditionalConfig(
                timeout=Timeout(init=30, query=60, insert=120)  # Values in seconds
            )
        )
        CSchemaLinking.beat()

    @staticmethod
    def beat():
        if not CSchemaLinking.Client.collections.exists("db_info_collection"):
            CSchemaLinking.Client.collections.create("db_info_collection", properties=[
                Property(name="name", data_type=DataType.TEXT),
                Property(name="rel_table_collection", data_type=DataType.TEXT),
                Property(name="rel_column_collection", data_type=DataType.TEXT)
            ])

    @staticmethod
    def destroy():
        if (CSchemaLinking.Client is not None) and CSchemaLinking.Client.is_connected():
            CSchemaLinking.Client.close()

    @staticmethod
    def gen_in_fixes_str():
        return str(time.perf_counter()).replace(".", "")

    @staticmethod
    def build(region_name: str = "default"):
        if region_name is None or len(region_name.strip()) <= 0:
            raise RagArgumentsError("region_name is None or empty")
        db_info_collection = CSchemaLinking.Client.collections.get("db_info_collection")
        response = db_info_collection.query.fetch_objects(
            filters=Filter.by_property("name").equal(region_name),
            limit=1
        )
        rel_db_info_object = response.objects[0] if len(response.objects) > 0 else None
        table_collection = CSchemaLinking.Client.collections.get(
            rel_db_info_object.properties['rel_table_collection']) if rel_db_info_object else None
        column_collection = CSchemaLinking.Client.collections.get(
            rel_db_info_object.properties['rel_column_collection']) if rel_db_info_object else None
        if table_collection is None or column_collection is None:
            table_collection, column_collection = \
                CSchemaLinking.create_rel_table_column_collection(db_info_collection, region_name)
        return CSchemaLinking(region_name,
                              db_info_collection,
                              table_collection,
                              column_collection)

    @staticmethod
    def create_rel_table_column_collection(db_info_collection, region_name):
        in_fixes = CSchemaLinking.gen_in_fixes_str()
        table_collection_name = "t_" + in_fixes + "_" + region_name
        CSchemaLinking.Client.collections.create(table_collection_name, properties=[
            Property(name="table_id", data_type=DataType.INT),
            Property(name="table_name", data_type=DataType.TEXT),
            Property(name="table_comment_cut", data_type=DataType.TEXT),
            Property(name="table_comment", data_type=DataType.TEXT)
        ])

        column_collection_name = "c_" + in_fixes + "_" + region_name
        CSchemaLinking.Client.collections.create(column_collection_name, properties=[
            Property(name="column_name", data_type=DataType.TEXT),
            Property(name="column_comment_cut", data_type=DataType.TEXT),
            Property(name="table_name", data_type=DataType.TEXT),
            Property(name="table_id", data_type=DataType.INT)
        ])

        # update
        db_info_collection.data.insert(
            {
                "name": region_name,
                "rel_table_collection": table_collection_name,
                "rel_column_collection": column_collection_name,
            }
        )
        # filter
        response = db_info_collection.query.fetch_objects(
            filters=Filter.by_property("name").equal(region_name),
            limit=1
        )
        rel_db_info_object = response.objects[0]
        table_collection = CSchemaLinking.Client.collections.get(rel_db_info_object.properties['rel_table_collection'])
        column_collection = CSchemaLinking.Client.collections.get(
            rel_db_info_object.properties['rel_column_collection'])
        return table_collection, column_collection

    @staticmethod
    def format_table_2_markdown(table_object):
        if table_object:
            table_name = table_object.properties['table_name']
            table_comment = table_object.properties['table_comment']
            column_info_list = table_object.properties['column_info_list']
            ddl_prefix = f"**{table_name}: {table_comment}**\n"
            content = "\n".join(
                f"- **`{item['column_name']}`**" + (f" ：{item['column_comment']}" if item['column_comment'] else "")
                for item in column_info_list
            )
            return ddl_prefix + content + "\n\n"
        return None

    def exists_table(self, table_name):
        if table_name:
            table_name = table_name if isinstance(table_name, list) else [table_name]
            response = self.table_collection.query.fetch_objects(
                filters=Filter.by_property("table_name").contains_any(table_name),
                limit=1
            )
            return len(response.objects) > 0
        return False

    def add_schema_batch(self, ddls: list):
        if ddls is None or (not isinstance(ddls, list)):
            raise RagArgumentsError(f"ddls is None or not a list!")

        table_id_list = []
        table_info_list = []
        column_info_list = []
        table_name_to_id = {}
        table_name_list = []
        for item in ddls:
            table_id_list.append(item['id'])
            table_info = SchemaParse.ddl_parse(item['ddl'])
            table_info_list.append(table_info)
            column_info_list.extend(table_info.column_info_list)
            table_name_to_id[table_info.table_name] = item['id']
            table_name_list.append(table_info.table_name)
        
        self.delete_schema_by_table_id_list(table_id_list)
        # Table Information Vectorization
        logger.info(f"Start vectorization for {len(ddls)} tables")
        failed_id_list = set()
        with self.table_collection.batch.dynamic() as batch:
            table_length = len(table_info_list)
            start = 0
            while start < table_length:
                end = min(table_length, (start + CEmbedding.batch_size))
                table_info_list_batch = table_info_list[start:end]
                tables_embeddings_list_batch = CEmbedding.text2vec(
                    [item.table_comment for item in table_info_list_batch])
                for table_info, table_embeddings in zip(table_info_list_batch, tables_embeddings_list_batch):
                    if table_info.table_name:
                        try:
                            batch.add_object(
                                properties={
                                    "table_id": table_name_to_id[table_info.table_name],
                                    "table_name": table_info.table_name,
                                    "table_comment_cut": table_info.table_comment_cut,
                                    "table_comment": table_info.table_comment,
                                    "column_info_list": [{
                                        "column_name": item.column_name,
                                        "column_comment": item.column_comment} for item in table_info.column_info_list
                                    ],
                                },
                                vector=table_embeddings
                            )
                        except Exception as e:
                            logger.error(e)
                            failed_id_list.add(table_name_to_id[table_info.table_name])
                start = end
                if end < table_length:
                    logger.info("[{}] tables have been completed, currently processing table `{}`".format(end, table_info_list[
                        min(end, table_length - 1)].table_name))
                else:
                    logger.info("Total ({} tables) have been completed".format(table_length))

        # Column Information Vectorization
        logger.info(f"Start vectorization for {len(column_info_list)} columns")
        with self.column_collection.batch.dynamic() as batch:
            column_length = len(column_info_list)
            start = 0
            while start < column_length:
                end = min(column_length, (start + CEmbedding.batch_size))
                column_info_list_batch = column_info_list[start:end]
                columns_embeddings_list_batch = CEmbedding.text2vec(
                    [item.column_comment for item in column_info_list_batch])
                for column_info, column_embeddings in zip(column_info_list_batch, columns_embeddings_list_batch):
                    if column_info.table_name and column_info.column_name:
                        try:
                            batch.add_object(
                                properties={
                                    "column_name": column_info.column_name,
                                    "column_comment_cut": column_info.column_comment_cut,
                                    "table_name": column_info.table_name,
                                    "table_id": table_name_to_id[column_info.table_name],
                                },
                                vector=column_embeddings
                            )
                        except Exception as e:
                            logger.error(e)
                            failed_id_list.add(table_name_to_id[column_info.table_name])
                start = end
                if end < column_length:
                    logger.info("[{}] column has been completed, currently processing table `{}`-column`{}`".format(
                        end,
                        column_info_list[min(end, column_length - 1)].table_name,
                        column_info_list[min(end, column_length - 1)].column_name))
                else:
                    logger.info("Total ({}columns) have been completed".format(column_length))
        if len(failed_id_list) > 0:
            return RetObj.build_partial(failed_id_list)
        else:
            return RetObj.build_success(None)

    def add_schema(self, ddl: str, table_id: int):
        if ddl is None or len(ddl) <= 0:
            raise RagArgumentsError(f"ddl is None or empty string")

        table_info = SchemaParse.ddl_parse(ddl)
        if self.exists_table(table_info.table_name):
            raise RagTableAlreadyExistError(
                f"table[{table_info.table_name}] already exists in region[{self.region_name}]")
        logger.info(f"Start [{table_info.table_name}]Vectorization")
        
        if table_info.embeddings is None:
            table_info.embeddings = CEmbedding.text2vec(table_info.table_comment)
        uuid = self.table_collection.data.insert(
            properties={
                "table_id": table_id,
                "table_name": table_info.table_name,
                "table_comment_cut": table_info.table_comment_cut,
                "table_comment": table_info.table_comment,
                "column_info_list": [{
                    "column_name": item.column_name,
                    "column_comment": item.column_comment} for item in table_info.column_info_list
                ],
            },
            vector=table_info.embeddings
        )
        logger.info(f"Finish Vectorization{uuid}")
        
        column_info_list = table_info.column_info_list
        length = len(column_info_list)
        start = 0
        while start < length:
            end = min(length, (start + CEmbedding.batch_size))
            column_info_list_batch = column_info_list[start:end]
            columns_embeddings_list_batch = CEmbedding.text2vec(
                [item.column_comment for item in column_info_list_batch])
            for column_info, column_embeddings in zip(column_info_list_batch, columns_embeddings_list_batch):
                if column_info.embeddings is None:
                    column_info.embeddings = column_embeddings
                self.column_collection.data.insert(
                    properties={
                        "column_name": column_info.column_name,
                        "column_comment_cut": column_info.column_comment_cut,
                        "table_name": column_info.table_name,
                        "table_id": table_id,
                    },
                    vector=column_info.embeddings
                )
            start = end
        logger.info(f"Finish [{table_info.table_name}] Vectorization!")
        return True

    def get_column_cluster_size(self, column_comment_cut, column_embedding, candidate_table_list):
        response = self.column_collection.query.hybrid(
            return_metadata=MetadataQuery(score=True, explain_score=True),
            vector=column_embedding,
            query=column_comment_cut,
            query_properties=['column_comment_cut'],
            fusion_type=HybridFusion.RELATIVE_SCORE,
            limit=3000,
            filters=Filter.by_property(
                "table_name").contains_any(candidate_table_list),

        )
        if response.objects:
            return sum(
                [1 if o.metadata.score >= CSchemaLinking.cluster_similarity_threshold else 0 for o in response.objects])
        else:
            return 1

    def get_similar_columns_object(self, query_vec, query_sep, similar_table_object, use_threshold=True):
        response = self.column_collection.query.hybrid(
            return_metadata=MetadataQuery(score=True, explain_score=True),
            vector=query_vec,
            query=query_sep,
            query_properties=['column_comment_cut'],
            fusion_type=HybridFusion.RELATIVE_SCORE,
            return_properties=["column_name", "column_comment_cut", "table_name", "table_id"],
            limit=3000,
            include_vector=True,
        )
        similar_column_object = {}
        cluster_size_dict = {}

        candidate_table_list = [] if len(similar_table_object) <= 0 else list(similar_table_object.keys())
        for o in response.objects:
            if use_threshold:
                if o.metadata.score >= CSchemaLinking.search_similarity_threshold:
                    candidate_table_list.append(o.properties["table_name"])
            else:
                candidate_table_list.append(o.properties["table_name"])

        # get cluster_size
        for o in response.objects:
            if use_threshold:
                if o.metadata.score >= CSchemaLinking.search_similarity_threshold:
                    if "table_name" in o.properties.keys():
                        table_name = o.properties["table_name"]
                        if table_name in similar_column_object.keys():
                            similar_column_object[table_name].append(o)
                        else:
                            similar_column_object[table_name] = [o]

                        column_name = o.properties['column_name']
                        if column_name not in cluster_size_dict.keys():
                            column_comment_cut = o.properties['column_comment_cut']
                            column_name_embedding = o.vector['default']
                            cluster_size = self.get_column_cluster_size(column_comment_cut, column_name_embedding, candidate_table_list)
                            cluster_size_dict[column_name] = cluster_size
            else:
                if "table_name" in o.properties.keys():
                    table_name = o.properties["table_name"]
                    if table_name in similar_column_object.keys():
                        similar_column_object[table_name].append(o)
                    else:
                        similar_column_object[table_name] = [o]
                    column_name = o.properties['column_name']
                    if column_name not in cluster_size_dict.keys():
                        column_comment_cut = o.properties['column_comment_cut']
                        column_name_embedding = o.vector['default']
                        cluster_size = self.get_column_cluster_size(column_comment_cut, column_name_embedding, candidate_table_list)
                        cluster_size_dict[column_name] = cluster_size

        return similar_column_object, cluster_size_dict

    def get_similar_table_object(self, query_vec, query_sep, use_threshold=True):
        response = self.table_collection.query.hybrid(
            return_metadata=MetadataQuery(score=True, explain_score=True),
            vector=query_vec,
            query=query_sep,
            query_properties=['table_comment_cut'],
            fusion_type=HybridFusion.RELATIVE_SCORE,
            return_properties=["table_name", "table_comment_cut", "table_id"],
            limit=3000,
        )
        similar_table_object = {}
        for o in response.objects:
            if use_threshold:
                if o.metadata.score >= CSchemaLinking.search_similarity_threshold:
                    if "table_name" in o.properties.keys():
                        table_name = o.properties["table_name"]
                        similar_table_object[table_name] = o
                        
            else:
                if "table_name" in o.properties.keys():
                    table_name = o.properties["table_name"]
                    similar_table_object[table_name] = o
                    
        return similar_table_object

    def normaliztion_score(self, all_table_score: dict):
        # min-max normalization
        if not isinstance(all_table_score, dict):
            return {}
        if len(all_table_score) <= 0:
            return {}
        min_value, max_value = min(all_table_score.values()), max(all_table_score.values())
        if min_value < max_value:
            return {key: (value - min_value) / (max_value - min_value) for key, value in all_table_score.items()}
        else:
            return {key: 1 for key in all_table_score.keys()}

    def search_schema(self, question: str, tok_k: int = 3):
        if question is None or len(question.strip()) <= 0:
            raise RagArgumentsError("question is None or empty")
        if self.table_collection is None or self.column_collection is None:
            raise RagKeyReferenceMissingError(
                f"can't find table_collection/column_collection referenced to this region[{self.region_name}]")
        if tok_k <= 0:
            tok_k = 3
        query_vec = CEmbedding.text2vec(question).tolist()[0]
        query_sep = CEmbedding.jieba_tokenize(question)
        similar_table_object = self.get_similar_table_object(query_vec, query_sep)
        similar_column_object, cluster_info_dict = self.get_similar_columns_object(query_vec, query_sep, similar_table_object)
        if len(similar_column_object) == 0 and len(similar_table_object) == 0:
            similar_table_object = self.get_similar_table_object(query_vec, query_sep, False)
            similar_column_object, cluster_info_dict = self.get_similar_columns_object(query_vec, query_sep, similar_table_object, False)

        # calculate score
        all_table_name = set(similar_column_object.keys()) | set(similar_table_object.keys())
        all_table_score = {key: 0.0 for key in all_table_name}
        for table_name in all_table_name:
            # column score
            if table_name in similar_column_object.keys():
                column_obj_list = similar_column_object[table_name]
                for column_obj in column_obj_list:
                    column_name = column_obj.properties["column_name"]
                    if column_name in cluster_info_dict.keys():
                        size = cluster_info_dict[column_name]
                        if size >= 1:
                            all_table_score[table_name] += column_obj.metadata.score * (1.0 / size)
            # table score
            if table_name in similar_table_object.keys():
                table_obj = similar_table_object[table_name]
                all_table_score[table_name] += table_obj.metadata.score
        all_table_score = self.normaliztion_score(all_table_score)
        top_k_items = heapq.nlargest(tok_k, all_table_score.items(), key=lambda x: x[1])
        ret_list = []
        for item_score in top_k_items:
            table_name = item_score[0]
            table_score = item_score[1]
            if table_name in similar_table_object.keys():
                table_id = similar_table_object[table_name].properties["table_id"]
            else:
                table_id = similar_column_object[table_name][0].properties["table_id"]
            ret_list.append(
                {"table_name": table_name, "id": table_id, "table_score": table_score, "region": self.region_name})
        return ret_list

    def delete_schema(self, table_id: int):
        if table_id <= 0:
            raise RagArgumentsError(f"table id must greater than 0, input is [{table_id}]")
        if self.table_collection is None or self.column_collection is None:
            raise RagKeyReferenceMissingError(
                f"can't find table_collection/column_collection referenced to this region[{self.region_name}]")
        response = self.table_collection.query.fetch_objects(
            filters=Filter.by_property("table_id").equal(table_id),
            limit=1
        )
        table_object = response.objects[0] if len(response.objects) > 0 else None
        if table_object:
            table_uuid = table_object.uuid
            table_name = table_object.properties["table_name"]
            if table_name:
                response = self.column_collection.query.fetch_objects(
                    filters=Filter.by_property("table_name").equal(table_name),
                    limit=1000
                )
                column_uuid_list = []
                for column_object in response.objects:
                    column_uuid_list.append(column_object.uuid)
                if len(column_uuid_list) > 0:
                    self.column_collection.data.delete_many(
                        where=Filter.by_id().contains_any(column_uuid_list)
                    )
            self.table_collection.data.delete_by_id(table_uuid)

    def delete_schema_by_table_id_list(self, table_id_list: list):
        if table_id_list is None or len(table_id_list) <= 0:
            raise RagArgumentsError(f"table_id_list is None or Empty")
        try:
            self.column_collection.data.delete_many(
                where=Filter.by_property("table_id").contains_any(table_id_list)
            )
            self.table_collection.data.delete_many(
                where=Filter.by_property("table_id").contains_any(table_id_list)
            )
        except Exception as e:
            logger.error(e)
            raise RagDeleteSchemaError(f"delete schemas failed, table_id_list={table_id_list} ")
        return True

    def get_table_object_by_page(self, offset, limit):
        if offset < 0 or limit <= 0:
            return RetObj.build_failed("offset and limit must greater than 0")

        response = self.table_collection.query.fetch_objects(
            limit=limit,
            offset=offset
        )
        markdown_table_list = []
        for o in response.objects:
            markdown_table = CSchemaLinking.format_table_2_markdown(o)
            if markdown_table:
                markdown_table_list.append(markdown_table)
        return RetObj.build_success(markdown_table_list)

    def get_table_desc(self, table_name: str):
        if table_name is None or len(table_name.strip()) <= 0:
            return None
        response = self.table_collection.query.fetch_objects(
            filters=Filter.by_property("table_name").equal(table_name),
            limit=1
        )
        if len(response.objects) > 0:
            table_object = response.objects[0]
            return CSchemaLinking.format_table_2_markdown(table_object)
        return None
