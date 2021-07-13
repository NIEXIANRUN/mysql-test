with open('config.txt') as f:
    for line in f.readlines():
        if '#' in line:
            continue
        host_type, ip, port, = line.split(',')
        break
    print(host_type, ip, port)