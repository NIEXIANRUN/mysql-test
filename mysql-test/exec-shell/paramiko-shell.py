import paramiko
import time


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