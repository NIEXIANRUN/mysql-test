import subprocess
from mysql_connection import config_connection
import time


def give_db_conf_path(cmd):
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    conf_path = p.stdout.read().strip()
    return conf_path


def give_empty_tran_processlist_id(db_conn, sql):
    list_id = db_conn.exe(sql)
    if list_id is None:
        return
    # print(list_id)
    for item in list_id:
        processlist_id = item.get('kill_id')
        yield processlist_id


if __name__ == '__main__':
    db_conf_path_cmd = r"ps -ef|grep mysql|grep '\-\-defaults\-file'|awk '{print $9}'|awk -F'=' '{print $NF}'"
    db_conf_path = give_db_conf_path(db_conf_path_cmd)

    db_info = dict(host='127.0.0.1', user='root', password='p%5IGsvb*0Nh', database='mysql',
                   read_default_group='mysqld', local_config_file=db_conf_path)
    # db_info = dict(host='127.0.0.1', user='root', password='p%5IGsvb*0Nh', database='mysql', port=20001)

    detect_empty_tran_sql = """
        select  
        concat('kill ',l.id,';') kill_id,
        concat('user:',l.user,';host:',l.host, ';trx_id:',trx_id,';thread_id:',trx_mysql_thread_id,';state:',l.state,';command:',l.command,';kill ',l.id,';') info   
        from  information_schema.innodb_trx trx, information_schema.processlist l 
        where trx.trx_mysql_thread_id=l.id and  TIMESTAMPDIFF(SECOND,trx_started,now()) >10   
        and  l.command='sleep' and l.user not in ('system user','root','myrobot','inception','operator','replic','repl','tvi_backup','tube');
        """
    with config_connection.Connection(**db_info) as db_client:
        while True:
            thread_id_list = give_empty_tran_processlist_id(db_client, detect_empty_tran_sql)
            if not thread_id_list:
                print('没有查到空事务')
                continue
            for thread_id in thread_id_list:
                db_client.exe(thread_id)
                # print(thread_id)
            time.sleep(5)
