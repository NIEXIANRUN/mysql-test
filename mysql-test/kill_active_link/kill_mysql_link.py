from mysql_connection import DBConnection


def record_kill_active_link_log(db, sql):
    logs = db.exe(sql)
    if not logs:
        return
    for log in logs:
        begin_time = log.get('begin_time')
        sql_info = log.get('info')
        format_log = f'{begin_time} {sql_info}'
        with open('kill_link.log', 'a+') as f:
            f.write(format_log + '\n')


def kill_active_link(db, sql):
    list_thread_id = db.exe(sql)
    if not list_thread_id:
        return
    for item in list_thread_id:
        kill_thread_id_sql = item.get('kill_id')
        db.exe(kill_thread_id_sql)


if __name__ == '__main__':
    with open('config.txt') as f:
        for line in f.readlines():
            if '#' in line:
                continue
            host, port, kill_user, exec_time = line.split(',')
            break
    kill_user = kill_user.strip()
    exec_time = int(exec_time.strip())
    db_info = dict(host=host.strip(), user='root', password='p%5IGsvb*0Nh', database='mysql', port=int(port.strip()))

    sum_active_link_sql = "select count(*) as counts from information_schema.processlist " \
                          "where command='Query' and user = '{0}' and time >= {1}".format(kill_user, exec_time)
    give_kill_log_sql = "select now() begin_time as counts from information_schema.processlist " \
                          "where command='Query' and user = '{0}' and time >= {1}".format(kill_user, exec_time)
    give_kill_link_sql = "select concat('kill', id, ';') as kill_id from information_schema.processlist " \
                          "where command='Query' and user = '{0}' and time >= {1}".format(kill_user, exec_time)
    with DBConnection.DBConnectionManager(**db_info) as db_client:
        while True:
            sum_active_link = db_client.exe(sum_active_link_sql)[0]
            print(f'当前用户{kill_user} 查询时长为{exec_time}SQL数量为{sum_active_link}')
            if sum_active_link:
                record_kill_active_link_log(db_client, give_kill_log_sql)
                kill_active_link(db_client, give_kill_link_sql)

