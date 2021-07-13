import pymysql


class DBConnectionManager:
    def __init__(self, host, user, password, database, port):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = 'utf8'
        self.connection = None
        self.exe_result = None


    def __enter__(self):
        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database,
                                          port=self.port, charset=self.charset)
        self.cursor = self.connection.cursor(cursor=pymysql.cursors.DictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f'exc_type: {exc_type}')
            print(f'exc_value: {exc_val}')
            print(f'exc_traceback: {exc_tb}')
            print('exception handled')
        self.connection.close()
        return True

    def exe(self, sql_list):
        try:
            if isinstance(sql_list, str):
                sql_list = [sql_list]
            for sql in sql_list:
                self.cursor.execute(sql)
            self.connection.commit()
            self.exe_result = self.cursor.fetchall() or True
        except Exception as e:
            print('Error for exe read sql', e)
            self.connection.rollback()
            self.exe_result =None
            return False
        if self.exe_result:
            return self.exe_result


if __name__ == '__main__':
    db_info = dict()
    with DBConnectionManager(**db_info) as db_client:
        result = db_client.cursor.execute('show tables')
        if not result:
            print('没有返回值或者执行报错')
