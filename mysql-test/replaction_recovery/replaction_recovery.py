import os
import time
import re
import pymysql
import paramiko
import subprocess


class DBConnectionManager:
    def __init__(self, host, user, password, database, port, host_type):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = 'utf8'
        self.connection = None
        self.exe_result = None
        self.host_type = host_type


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


def exec_shell(db, shell):
    host = db['host']
    host_type = db['host_type']
    if host_type == 'docker':
        username = 'root'
    else:
        username = 'mysql'
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key_file = paramiko.RSAKey.from_private_key_file('/home/mysql/.ssh/id_rsa')
    ssh.connect(host, 22, username=username, pkey=key_file, timeout=20)
    _, stdout, stderr = ssh.exec_command(shell)
    out = stdout.readlines()
    err = stderr.readlines()
    if len(err) > 0:
        if err[0].find('No such file or directory'):
            return -2, out
        sprint(0, 'shell命令执行有误')
        ssh.close()
        return -1, out
    else:
        ssh.close()
        return 1, out


def sprint(flag, text):
    date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if flag == 0:
        print('\033[1;31m {date} - {text}\033[0m'.format(date=date_time, text=text))
    elif flag == 1:
        print('\033[1;34m {date} - {text}\033[0m'.format(date=date_time, text=text))


def exec_cmd(cmd):
    try:
        obj_res = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        str_response = str(obj_res.stdout.read(), encoding='utf-8')
        obj_res.terminate()
        return str_response
    except Exception as e:
        return str(e)


def get_tmpdir(db):
    tmpdir_sql = "show variables like 'tmpdir'"
    tmp_dir = db.exe(tmpdir_sql)
    tmp_dir = tmp_dir[0].get('Value')
    return tmp_dir


def get_relay_log(db):
    relay_log_dir_sql = "show variables like 'relay_log'"
    status_sql = "show slave status"

    relay_log_dir = db.exe(relay_log_dir_sql)
    relay_log_dir = relay_log_dir[0].get('Value')
    relay_log_dir = os.path.split(relay_log_dir)[0]

    current_log_file = db.exe(status_sql)
    current_log_file = current_log_file[0].get('Relay_Log_File')

    relay_log_file = os.path.join(relay_log_dir, current_log_file)
    return relay_log_file


def get_slave_error_info(db):
    sql = "show slave status"
    slave_status = db.exe(sql)
    error_type = slave_status[0].get('Last_Errno')
    err_info = slave_status[0].get('Last_Error')

    pattern = re.search(r"executing transaction '(.*?)'", err_info)
    err_tran = pattern.group(1) if pattern else None

    return error_type, err_tran


def check_repl_status(db):
    sql = "show slave status"
    slave_status_info = db.exe(sql)
    io_thread_status = slave_status_info[0].get('Slave_IO_Running')
    sql_thread_status = slave_status_info[0].get('Slave_SQL_Running')
    if io_thread_status == 'Yes' and sql_thread_status == 'Yes':
        return True
    else:
        return False


def repl_recovery(db, err_info):
    err_type, err_tran = err_info[0], err_info[1]
    if err_type == 1062 or err_type == 1032:
        sql = f"stop slave;set session gtid_next='{err_tran}';begin;commit;" \
            f"set session gtid_next=automatic;start slave;"
        list_sql = sql.split(';')[:-1]
        res = db.exe(list_sql)
        return res
    return False


def record_log(db_info, db, err_info):
    relay_log_file = get_relay_log(db)
    tmp_dir = os.getcwd()
    log_dir = db_info.get('host') + ':' + str(db_info.get('port'))
    tmp_dir = os.path.join(tmp_dir, log_dir)
    cmd = 'mkdir -p {path}'.format(path=tmp_dir)
    exec_cmd(cmd)
    log_file = os.path.join(tmp_dir, f'{err_info[1]}.log')
    str_binlog = parse_binlog(db_info, err_info[1], relay_log_file)
    with open(log_file, 'w') as f:
        record = f"relay_log: {relay_log_file}\nerr_type: {err_info[0]}\nerr_tran: {err_info[1]}\nbinlog: {str_binlog}"
        f.write(record)
    return True


def parse_binlog(db_info, err_gtid, current_realy_log):
    cmd = 'mysqlbinlog -vv --base64-output=decode-rows --include-gtids={0} {1}'.format(err_gtid, current_realy_log)
    exec_flag, out = exec_shell(db_info, cmd)
    if exec_flag == -1:
        print('解析binlog失败')
    res = ''
    for line in out:
        if 'at' in out:
            pass
        else:
            res += line

    return res


if __name__ == '__main__':
    with open('config.txt') as f:
        for line in f.readlines():
            if not line.strip() or '#' in line:
                continue
            host_type, host, port = line.split(',')
            break

    db_info = dict(host=host.strip(), user='test', password='test', database='mysql', port=int(port.strip()), host_type=host_type.strip())
    with DBConnectionManager(**db_info) as db_client:
        skip_gtid_count = 0
        while True:
            slave_status = check_repl_status(db_client)
            if not slave_status:
                tup_err_type_tran = get_slave_error_info(db_client)
                skip_one_err = repl_recovery(db_client, tup_err_type_tran)
                if skip_one_err:
                    skip_gtid_count += 1
                    record_log(db_info, db_client, tup_err_type_tran)
                    print(f'已跳过{skip_gtid_count}个报错事务，当前报错事务：{tup_err_type_tran[1]}')
