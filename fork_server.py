
# -*-  encoding:utf-8 -*-

import meinheld
from multiprocessing import Process
import signal
import os
import time
import os
import bottle
import redis
import psycopg2
import gevent


from gevent_psycopg2 import gevent_wait_callback,GeventConnectionPool
import traceback

workers = []

def hello_world(environ, start_response):
    pass
    """
    """


def hello(environ, start_response):
    a = bottle. BaseRequest(environ)
    print(dir(a))
    status = '200 OK' 
    res = "Hello world!"
    response_headers = [('Content-type','text/plain'),('Content-Length',str(len(res))),('Server', "leapord")]
    start_response(status, response_headers)
    # print environ
    return [res]


def receive_web(environ, start_response):
    if environ['REQUEST_METHOD'] == 'GET' and environ['PATH_INFO'].startswith('/receive_web') :
        try:
            global conn
            conn = pool.getconn()
            status = '200 Ok'
            r = redis.Redis('127.0.0.1', db ='3')
            a = bottle. BaseRequest(environ)
            logger.debug('connect to server ok!')
            search = a.query
            #for i in search.items():
            #    print(i)
            data = search.data
            logger.info(data)
            logger.info(type(data))
            result_dict = eval(data)
            sample_name ,node_name , batch_id = result_dict['sample_name'],result_dict['node_name'], result_dict['batch_id']
            serial_number  =  result_dict["serial_number"] 
            sample_li = sample_name.split(".")
            li = r.sismember('batch_node_lists', batch_id)           
            sql_batch = """
                INSERT INTO t_batch_receive(batch_id) VALUES ('%s');
            """
            sql_normal = """
                INSERT INTO node_receive(seq_node, md5, hash_sha1, node_batch_id, sequence_id)
                VALUES ('%s', '%s', '%s', '%s', '%s')
            """
            sql_tree = """
                 INSERT INTO node_trees_info(file_name, batch_id)
                 VALUES ('%s', '%s')
            """
            if not li :
            #    #conn = pool.getconn()
                if conn.closed:
                    conn = pool.getconn()
                else:
                    pass
                logger.info('will insert INTO batch_receive')
                cur = conn.cursor()
                cur.execute(sql_batch  %str(batch_id))
                conn.commit()
                redis_result = r.sadd('batch_node_lists', batch_id)
                print(redis_result)
            if len(sample_li) == 2:
                md5 , hash_sha1 = sample_li[0], sample_li[1]
                #conn = pool.getconn()
                if conn.closed:
                    conn.pool.getconn()
                else:
                    pass
                cur = conn.cursor()
	            #sql_normal_str = sql_normal
                sql_normal_str =  sql_normal %(str(node_name), str(md5), str(hash_sha1), str(batch_id), str(serial_number))
                cur.execute(sql_normal_str)
                conn.commit()
            if len(sample_li) == 3:
                #conn = pool.getconn()
                if conn.closed:
                    conn.pool.getconn()             
                else:
                    pass
                cur = conn.cursor()
                cur.execute(sql_tree %(str(sample_name),  str(batch_id)))
                conn.commit()
            pool.putconn(conn)
            res = 'right'
            response_headers = [('Content-type','text/plain'),('Content-Length',str(len(res))),('Server', "awesome")]
            start_response(status, response_headers)
            # print environ
            return [res]
  
        except Exception as e:
            traceback.print_exc()
            print( ' exception ')
            response_headers = [("Server"),("awesome")]
            start_response(status, response_headers)
            return [str(e)]
    else:
        res = 'error'
        status = '404 '
        response_headers = [('Content-type','text/plain'),('Content-Length',str(len(res))),('Server', "wisedom")]
        start_response(status, response_headers)
        
        return ['error occur here!\n'] 


def run(app, i):
    meinheld.run(app)

def kill_all(sig, st):
    for w in workers:
        w.terminate()

def start(num=1):
    for i in range(num):
        p = Process(name="worker-%d" % i, target=run, args=(receive_web,i))
        workers.append(p)
        p.start()


if __name__ == '__main__':

    signal.signal(signal.SIGTERM, kill_all)
    meinheld.set_keepalive(60)
    meinheld.set_access_logger(None)
    meinheld.set_error_logger(None)
    meinheld.listen(("0.0.0.0", 6000))
    dsn = "host=127.0.1.1 dbname=data user=postgres password=**** "
    psycopg2.extensions.set_wait_callback(gevent_wait_callback)
    pool = GeventConnectionPool(5 ,10,dsn)
    conn = pool.getconn()
    start()
    
