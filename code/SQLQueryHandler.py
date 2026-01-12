class SQLQueryHandler:

    @staticmethod
    def read(path, encoding="utf8"):
        with open(path, 'r', encoding=encoding) as query_file:
            try:
                sql_query = query_file.read()
            except Exception as e:
                print(e)
            print(f'✅ Read {path}')
        return sql_query
    
    @staticmethod
    def modify(query, modifications):
        for key, value in modifications.items():
            query = query.replace(key, value)
        return query