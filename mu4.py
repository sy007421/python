# -*- coding: utf-8 -*-
import MySQLdb
import sys
import time
reload(sys)
sys.setdefaultencoding("utf-8")

def get_master_connect():
    myconn = MySQLdb.connect(host='192.168.137.2',user='sunyu',passwd='mysql',db='sunyu',charset='utf8',port=3306)
    return myconn

def get_okc_connect():
    myconn = MySQLdb.connect(host='192.168.137.2',user='sunyu',passwd='mysql',db='sss',charset='utf8',port=3306)
    return myconn

def pd_tab(table_name):
    try:
        conn = get_master_connect()
        cur = conn.cursor()
        sql = "select * from information_schema.tables where TABLE_NAME='"+str(table_name)+"'"
        cur.execute(sql)
        r = cur.fetchone()
        return len(r)
    except TypeError as e:
        print e
    finally:
        cur.close()
        conn.close()

def sql_command(connect,sql_txt):
    conn = ""+str(connect)+""
    cur = conn.cursor()
    sql = ""+str(sql_txt)+""
    cur.execute(sql)
    cur.close()
    conn.close()

def get_num(table_name):
    conn = get_master_connect()
    cur = conn.cursor()
    sql = "select * from "+str(table_name)+" limit 1"
    cur.execute(sql)
    r = cur.fetchone()
    return len(r)
    cur.close()
    conn.close()

def counts(n):
    s = '%s'
    if n > 1:
        for i in range(n-1):
            s += ',%s'
    return s

def insert_new_tab_del(table_name,sr_id,end_id):
    conn_master = get_master_connect()
    cur_master = conn_master.cursor()
    sql_master = "select * from "+str(table_name)+" where id > "+str(sr_id)+" and id <= "+str(end_id)+" order by id asc limit 100"
#    print sql_master
    cur_master.execute(sql_master)
    args = cur_master.fetchall()
    cur_master.close()
    conn_master.close()

    conn_slave = get_okc_connect()
    cur_slave = conn_slave.cursor()
    num_s = counts(get_num(table_name))
    sql_slave = "insert into "+str(table_name)+" values("+str(counts(get_num(table_name))) +")"
    cur_slave.executemany(sql_slave,args)
#    print sql_slave
    conn_slave.commit()
    cur_slave.close()
    conn_slave.close()

    conn_master = get_master_connect()
    cur_master = conn_master.cursor()
    sql_master_del = "delete from "+str(table_name)+" where id > "+str(sr_id)+" and id <= "+str(end_id)+" order by id asc limit 100"
#    print sql_master_del
    cur_master.execute(sql_master_del)
    conn_master.commit()
    cur_master.close()
    conn_master.close()

def get_max_id(table_name):
    conn = get_okc_connect()
    cur = conn.cursor()
    sql = "select max(id) from "+str(table_name)+""
    cur.execute(sql)
    r=cur.fetchone()[0]
    if r == None:
        r = 0
    else:
        pass
    return r
    cur.close()
    conn.close()

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

if __name__ == '__main__':
    print 'here we go! begin time is %s' %[get_time()]
    tablename = sys.argv[1] 
    endid = int(sys.argv[2])
    tab_re_info = pd_tab(tablename)
    if tab_re_info > 0:
        print 'table exists,we will take some time!'
        while True:
            srid = get_max_id(tablename)
            if srid < endid:
                insert_new_tab_del(tablename,srid,endid)
#                time.sleep(1)
            else:
                break
        print 'here we go! begin time is %s' %[get_time()]
    elif tab_re_info == None:
        print 'table not exists,you need to create table in this db!!'

