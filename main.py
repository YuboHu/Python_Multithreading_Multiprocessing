import threading
import random
import string

import sqlite3
from sqlite3 import Error
from multiprocessing import Pool, Queue, Lock
from collections import OrderedDict

queue = Queue()
cursor = None
lock = None
textFile = "/tmp/random_string.txt"
insertSQL = "INSERT into Strings (string) VALUES (?)"
dbFile = "/tmp/sort_string.sqlite"

def create_connection_on_disk(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def create_table(create_table_sql):
    cursor.execute(create_table_sql)

def generate_random_string(N = 1024):
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=N))+'\n'

def doWork(args):
    line = queue.get(True,5)
    readyString = operate(line)
    print("Ready string:" + readyString)
    #lock.acquire()
    #conn = create_connection_on_disk(dbFile)
    #cursor = conn.cursor()
    cursor.execute(insertSQL,(readyString,))
    #conn.close()
    print("inserted")
    #lock.release()

def operate(line):
    return "".join(sorted(OrderedDict.fromkeys(line)))

def init(l):
  global lock
  lock = l


class MyThread(threading.Thread):
    def run(self):
        f= open(textFile,"a+")
        for i in range(50):
            f.write(generate_random_string())
        f.close()

def main():
    global queue
    global cursor
    for x in range(20):                                     
        mythread = MyThread()
        mythread.start()
        mythread.join() 
    conn = create_connection_on_disk(dbFile)
    cursor = conn.cursor()
    createTableSQL = """CREATE TABLE IF NOT EXISTS Strings (
                                        id integer PRIMARY KEY,
                                        string text NOT NULL
                                    );"""
    create_table(createTableSQL)
    f= open(textFile,"r")
    for line in f: 
        print("Read Line:" + line)
        queue.put(line)

    lock = Lock()
    pool = Pool(processes=1, initializer=init, initargs=(lock,))
    pool.map(doWork, range(1))

    pool.close()
    pool.join()
    conn.close()

if __name__ == '__main__':main()
