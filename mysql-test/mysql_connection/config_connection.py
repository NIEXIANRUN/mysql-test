import configparser
import os
import pymysql


class Parser(configparser.RawConfigParser):
    def __init__(self, **kwargs):
        kwargs["allow_no_value"] = True
        configparser.RawConfigParser.__init__(self, **kwargs)

    # def __remove_quotes(self, value):
    #     quotes = ["'", '"']
    #     for quote in quotes:
    #         if len(value) >= 2 and value[0] == value[-1] == quote:
    #             return value[1:-1]
    #     return value

    def get(self, section, option):
        value = configparser.RawConfigParser.get(self, section, option)
        # return self.__remove_quotes(value)
        return value


def read_cnf(cnf_path):
    assert cnf_path is not None and os.path.exists(cnf_path)
    cnf_dict = {}
    cur_section = None
    with open(cnf_path) as cnf_reader:
        for line in cnf_reader.readlines():
            line = ''.join(line.split())
            if len(line) <= 0 or '#' == line[0]:
                continue
            if '[' == line[0] and ']' == line[-1]:
                cur_section = line[len('['):len(line) - 1]
                if cur_section not in cnf_dict:
                    cnf_dict[cur_section] = {}
            elif '=' in line and line.count('=') == 1:
                if cur_section is None:
                    print('cur_section is None')
                    continue
                tokens = line.split('=')
                key = tokens[0].replace('"', '').replace("'", '')
                value = tokens[1].replace('"', '').replace("'", "")
                cnf_dict[cur_section][key] = value
    return cnf_dict


class Connection:
    def __init__(self,
                 user=None,
                 password="",
                 host=None,
                 database=None,
                 # unix_socket=None,
                 port=0,
                 charset="",
                 read_default_group=None,
                 local_config_file=None,
                 ):

        if local_config_file:
            if not read_default_group:
                read_default_group = "mysqld"

        cfg = Parser()
        cfg.read(os.path.expanduser(local_config_file))

        def _config(key, arg):
            if arg:
                return arg
            try:
                return cfg.get(read_default_group, key)
            except Exception:
                return arg

        # user = _config("user", user)
        # password = _config("password", password)
        # host = _config("host", host)
        # database = _config("database", database)
        # unix_socket = _config("socket", unix_socket)
        port = int(_config("port", port))
        # charset = _config("default-character-set", charset)
        self.host = host or "localhost"
        self.port = port or 20001
        if type(self.port) is not int:
            raise ValueError("port should be of type int")
        self.user = user or 'root'
        self.password = password or b"p%5IGsvb*0Nh"
        if isinstance(self.password, str):
            self.password = self.password.encode("latin1")
        self.db = database or 'mysql'
        # self.unix_socket = unix_socket
        self.charset = charset or 'utf8'
        self._result = None
        self.connection = None
        # self._affected_rows = 0
        # self.host_info = "Not connected"

    def __enter__(self):
        self.connection = pymysql.connect(host=self.host, user=self.user, password=self.password,
                                          port=self.port, charset=self.charset, cursorclass=pymysql.cursors.DictCursor,
                                          )
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f'exc_type: {exc_type}')
            print(f'exc_value: {exc_val}')
            print(f'exc_traceback: {exc_tb}')
            print('exception handled')
        self.connection.close()
        return True

    def exe(self, sql):
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            self._result = self.cursor.fetchall()
        except Exception as e:
            print('Error for exe sql', e)
            self.connection.rollback()
            self._result = None
            return False
        if self._result:
            return self._result
