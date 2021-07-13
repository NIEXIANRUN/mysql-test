import subprocess
import traceback


class CmdUtils(object):

    def __init__(self, user, passwd, ip, port, cmd='', cmd_file=''):
        self.user = user
        self.passwd = passwd
        self.ip = ip
        self.port = port
        self.cmd = cmd
        self.cmd_file = cmd_file

    def exe(self):
        if self.cmd != '':
            self.cmd = self.cmd.replace("'", '"')
            cmd = "mysql -u{0} -p{1} -h{2} -P{3} -e '{4}'".format(self.user, self.passwd, self.ip, self.port, self.cmd)
        if self.cmd_file != '':
            cmd = "mysql -u{0} -p{1} -h{2} -P{3} < '{4}'".format(self.user, self.passwd, self.ip, self.port, self.cmd_file)
        print(cmd)
        try:
            obj_res = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            str_response = str(obj_res.stdout.read(), encoding='utf-8')
            obj_res.terminate()
            return str_response
        except Exception as e:
            traceback.print_exc()
            return str(e)


if __name__ == '__main__':
    shell_client = CmdUtils('nxr', '123', '127.0.0.1', '20001', cmd='df -h')
    shell_client.exe()