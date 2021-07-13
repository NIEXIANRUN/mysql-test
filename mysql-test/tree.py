from threading import Thread
from multiprocessing import JoinableQueue
import time
import os


q = JoinableQueue()


def producer():
    for item in range(30):
        time.sleep(2)
        q.put(item)
        print(f'producer {item}')


def worker():
    while True:
        item = q.get()
        q.task_done()
        print(f'Consumer {item}')


for i in range(5):
    c = Thread(target=worker, daemon=True).start()

producers = []
for i in range(10):
    p = Thread(target=producer)
    producers.append(p)
    p.start()

for p in producers:
    p.join()

q.join()
print('All work completed')
