import re
from typing import List

from embedding.cembedding import CEmbedding


class ColumnInfo:
    def __init__(self, t_name, c_name, c_comment, c_comment_cut):
        self.column_name: str = c_name
        self.column_comment: str = c_comment
        self.column_comment_cut: str = c_comment_cut
        self.table_name: str = t_name
        self.embeddings = None
        self.cluster_name = None


class TableInfo:
    def __init__(self, t_name, t_comment, t_comment_cut):
        self.table_name: str = t_name
        self.table_comment: str = t_comment
        self.table_comment_cut: str = t_comment_cut
        self.column_info_list: List[ColumnInfo] = []
        self.embeddings = None

    def add_column_info(self, column_info: ColumnInfo):
        self.column_info_list.append(column_info)


class SchemaParse:

    @staticmethod
    def extract_table_info(sql):
        """
        Extract the names and comments of tables in the schema.
        """

        pattern1 = r"CREATE\s+TABLE\s+`?([^`\s]+)`?"  # table name
        pattern2 = r"CREATE\s+TABLE\s+`?[^`]+`?\s+\([^;]+COMMENT\s*=\s*'([^']+)'"  
        match1 = re.search(pattern1, sql, re.DOTALL)
        match2 = re.search(pattern2, sql, re.DOTALL)
        table_name = match1.group(1) if match1 else ''
        table_comment = match2.group(1) if match2 else ''

        if (not table_comment) or (len(table_comment) <= 0):
            pattern2 = r"CREATE\s+TABLE\s+\S+\s*\([\s\S]+?\)\s*ENGINE\s*=\s*\w+\s*ROW_FORMAT\s*=\s*\w+\s*COMMENT\s*=\s*'([^']+)'"  
            match2 = re.search(pattern2, sql, re.DOTALL)
            table_comment = match2.group(1) if match2 else ''
        return table_name, table_comment

    @staticmethod
    def extract_field_name_comment(sql):
        """Extract the names and comments of fields in the schema."""
        pattern = r"`([^`]+)`\s+[a-zA-Z]+(?:\([^\)]+\))?(?:[^,]+COMMENT\s+'([^']+)')?"
        matches = re.findall(pattern, sql)
        field_names = []
        field_comments = []
        for match in matches:
            field_name = match[0]
            field_comment = match[1]
            field_names.append(field_name)
            field_comments.append(field_comment)
        return field_names, field_comments

    @staticmethod
    def ddl_parse(sql):
        table_name, table_comment = SchemaParse.extract_table_info(sql)
        if table_comment is None or len(table_comment.strip()) <= 0:
            table_comment = table_name
        table_comment_cut = CEmbedding.jieba_tokenize(table_comment)
        table_info = TableInfo(table_name, table_comment, table_comment_cut)

        column_name_list, column_comment_list = SchemaParse.extract_field_name_comment(sql)
        for c in zip(column_name_list, column_comment_list):
            column_name = c[0]
            column_comment = c[1]
            if column_comment is None or len(column_comment.strip()) <= 0:
                column_comment = column_name
            column_comment_cut = CEmbedding.jieba_tokenize(column_comment)
            column_info = ColumnInfo(table_name, column_name, column_comment, column_comment_cut)
            table_info.add_column_info(column_info)
        return table_info


if __name__ == '__main__':
    ddl = """CREATE TABLE `b_stand_file`  (
  `id` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT 'Unique Identifier'
)"""
    SchemaParse.ddl_parse(ddl)
