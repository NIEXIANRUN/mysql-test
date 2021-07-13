import paramiko
import os
import time
import logging
import urllib.request
import pymysql

# 备份用户及密码
backup_user = 'bkpuser'
backup_passwd = '123456'
# oss 信息
oss_access_key = '123456'
oss_access_secret = '1qaz'
oss_host = "http://127.0.0.1"
oss_endpoint = '127.0.0.1'
oss_bucket = 'prd-mysql-data'
oss_path = ''
clusterId_dict = {'ly': 'luoyang--ly-app-hpc-prd-1',
                  'ha': 'huaian--ha-app-hpc-prd-1',
                  'ha-arm': 'huaian--ha-app-hpc-arm-prd-3rd'}
center_id_tupe = ("ly", "ha")


def log(logfile, logIns, flag, msg):
    # 创建一个logger
    logger = logging.getLogger(logIns)
    logger.setLevel('INFO')
    # 创建一个handler，用于写入日志文件
    log_file = "/home/mysql/docker/qianyi/" + logfile + ".log"
    fhandler = logging.FileHandler(log_file)
    # 定义handler的输出格式
    # 2021-07-07 16:30:57 - {logIns} - INFO - this is a info
    basic_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    data_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(basic_format, data_format)
    fhandler.setFormatter(formatter)
    # 将logger添加到handler里面
    logger.addHandler(fhandler)

    if flag == 0:
        logger.error(msg)
    elif flag == 1:
        logger.info(msg)
        sprint(1, msg)
    else:
        pass


def sprint(flag, text):
    date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    if flag == 0:
        print('\033[1;31m {date} - {text}\033[0m'.format(date=date_time, text=text))
    elif flag == 1:
        print('\033[1;34m {date} - {text}\033[0m'.format(date=date_time, text=text))


def exec_shell(host, shell):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key_file = paramiko.RSAKey.from_private_key_file('/home/mysql/.ssh/id_rsa')
    ssh.connect(host, 22, username='mysql', pkey=key_file, timeout=20)
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


def exec_sql(host, port: int, sql):
    port = int(port)
    conn = pymysql.connect(host=host, port=port, user=backup_user, password=backup_passwd)
    with conn.cursor() as cursor:
        try:
            rows = cursor.execute(sql)
            req = cursor.fetchall()
        except pymysql.err.IntegrityError:
            sprint(0, '执行SQL错误')
            return -1, ''
        conn.close()
        return rows, req


def upload_myoss(host, port:int):
    log_put = host + ':' + str(port)
    local_file = r'/home/mysql/zhanggr/soft/myOSS'
    remote_file = os.path.join("/home/mysql/soft/upload/", "myOSS")
    _, res = exec_shell(
        host, "ls -l {ossfile} | wc -l".format(ossfile=remote_file)
    )
    if res[0].strip() == '1':
        log(log_put, "myOSS", 1, '当前主机已存在myoss, 无需再次配置')
    else:
        sprint(0, '目标主机没有myOSS，正在上传')
        ssh = paramiko.Transport((host, 22))
        key_file = paramiko.RSAKey.from_private_key_file('/home/mysql/.ssh/id_rsa')
        ssh.connect(username='mysql', pkey=key_file, timeout=20)
        sftp = paramiko.SFTPClient.from_transport(ssh)
        log(log_put, 'myOSS', 1, '开始上传myOSS')
        try:
            sftp.put(local_file, remote_file)
        except:
            log(log_put, "myOSS", 0, "myOSS目录不存在")
            exec_shell(host, 'mkdir -p {path}'.format(path=os.path.split(remote_file)[0]))
            sftp.put(local_file, remote_file)
            log(log_put, 'myOSS', 1, 'myOSS上传成功')
            exec_shell(host, 'chmod +x /home/mysql/soft/upload/myOSS')
            ssh.close()


def get_ins_info(host, port: int):
    log_put = host + ':' + str(port)
    get_cnf_file = r"ps -ef|grep mysqld|grep '\-\-port=%d|' awk '{print $9, $(NF-1)}'" % port
    exec_flag, out = exec_shell(host, get_cnf_file)
    if exec_flag == -1:
        log(log_put, 'get_ins_info', 0, '实例信息获取失败')
        return '', '', ''
    ins_info = out[0].strip()
    cnf_file = ins_info.split()[0]
    socket_file = ins_info.split()[1]
    ins_name = cnf_file.split('/')[3].replace('db_', "")
    return cnf_file, socket_file, ins_name


def docker_post_recovery(mysqlClusterName, center_id, backupfile, host, port:int):
    # 调用观云台接口，恢复备份
    # date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log_handle = host + ":" + str(port)
    get_masterHost_sql = "select host, port from mysql.slave_master_info;"
    row , masterInfo = exec_sql(host, port, get_masterHost_sql)
    if row == 1:
        masterHost = masterInfo[0][0]
        masterPort = masterInfo[0][1]
    else:
        log(log_handle, 'docker-recovery', 0, '当前备份数据库master获取失败')
        log(log_handle, 'docker-recovery', 0, '当前数据库为单节点，使用当前节点搭建主从')
        masterHost = host
        masterPort = port
    post_data = {
        'mysqlClusterName': mysqlClusterName,
        'namespace': 'mysql',
        'clusterId': clusterId_dict[center_id],
        'osskey': mysqlClusterName + '-' + center_id.upper() + '/' + backupfile,
        'masterHost': masterHost,
        'masterPort': masterPort
    }
    ossurl = 'http://127.0.0.1:20001/mysql/api/openapi/mysql/entireRestore'
    post_data_json = urllib.parse.urlencode(post_data).encode(encoding='UTF8')
    header_dict = {'Content-type': 'application/json'}
    req = urllib.request.Request(method='POST',
                                 url=ossurl,
                                 data=post_data_json,
                                 headers=header_dict
                                 )
    r = urllib.request.urlopen(ossurl, post_data_json)
    callResult = r.read()
    callResult = str(callResult, encoding='utf8')
    s_call = callResult.strip('{}')
    if 'true' in s_call.split(":"):
        log(log_handle, 'docker-recovery', 1, '调用恢复接口成功')
    else:
        log(log_handle, 'docker-recovery', 0, "调用恢复接口失败")


def backup_new(host, port:int, center_id, ClusterName):
    log_put = host + ':' + str(port)
    cnf_file, socket_file, ins_name = get_ins_info(host, port)
    backup_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    if cnf_file == '':
        return
    backupdir = '/data/DBbackup/{ins_name}/'.format(ins_name=ins_name)
    exec_shell(host, 'mkdir -p %s' % backupdir)
    backup_filename = f"{ins_name}_{backup_time}.xbstream"
    backuplog = f"{backupdir}/{ins_name}_{backup_time}.log"
    bk_cmd = f"""
    innobackupex {cnf_file} {socket_file} --user={backup_user} --password='{backup_passwd}' \
    --parallel=6 \
    --compress --compress-threads=4 --compress-chunk-size=1024k \
    --kill-log-queries-timeout=15 --kill-log-query-type=select \
    --stream=xbstream {backupdir} 2> {backuplog} \
    | /home/mysql/soft/upload/myOSS -E {oss_host} -K {oss_access_key} -S {oss_access_secret} -B {oss_bucket} -f {oss_path}/{backup_filename}
    """
    print(bk_cmd)
    log(log_put, "backup", 1, "{ins_name}备份开始".format(ins_name=ins_name))
    exec_shell(host, bk_cmd)
    _, result = exec_shell(host, 'tail -1 {backuplog}'.format(backuplog=backuplog))
    result = result[0].strip('\n').split()[-1]
    if result == "OK!":
        log(log_put, 'backup', 1, "{ins_name}备份成功".format(ins_name=ins_name))
        docker_post_recovery(ClusterName, center_id, backup_filename, host, port)
    else:
        log(log_put, "backup", 1, "{ins_name}备份成功".format(ins_name=ins_name))
        return


if __name__ == '__main__':
    pass