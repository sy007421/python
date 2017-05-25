# -*- coding: utf-8 -*-
import MySQLdb
import sys
import time
reload(sys)
sys.setdefaultencoding("utf-8")

def get_connect():
    myconn = MySQLdb.connect(host='192.168.137.2',user='sunyu',passwd='mysql',db='sunyu',charset='utf8',port=3306)
    return myconn

def sql_command(sql_txt):
    conn = get_connect()
    cur = conn.cursor()
    sql = ""+str(sql_txt)+""
    cur.execute(sql)
    cur.close()
    conn.close()

def rename_table(table_name):
    conn = get_connect()
    cur = conn.cursor()
    table_des = "show create table "+str(table_name)+""
    cur.execute(table_des)
    table_des = cur.fetchone()[1]
    _bak = '_bak'
    new_table_name = table_name + _bak
    sql_rename = "rename table "+str(table_name)+" to "+str(new_table_name)+""
    sql_command(sql_rename)
    sql_command(table_des)
    sql_alter = "alter table "+str(table_name)+" AUTO_INCREMENT = 1"
    sql_command(sql_alter)
    cur.close()
    conn.close()

def get_max_id(table_name):
    conn = get_connect()
    cur = conn.cursor()
    sql = "select max(id) from "+str(table_name)+""
    cur.execute(sql)
    return cur.fetchone()[0]
    cur.close()
    conn.close()

def insert_new_tab(rename_tab,new_tab,sr_id,end_id):
    conn = get_connect()
    cur = conn.cursor()
    sql = "select * from "+str(rename_tab)+" where id > "+str(sr_id)+" and id <= "+str(end_id)+" order by id asc limit 100"
#    print sql
    cur.execute(sql)
    args = cur.fetchall()
    sql_insert = "insert into "+str(new_tab)+" values("+str(counts(get_num(rename_tab))) +")"
    cur.executemany(sql_insert,args)
#    print sql_insert
    conn.commit()
    cur.close()
    conn.close()

def get_num(table_name):
    conn = get_connect()
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

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

if __name__ == '__main__':
    print 'here we go! begin time is %s' %[get_time()]
    tablename = sys.argv[1]
    rename_table(tablename)
    srid = int(sys.argv[2])
    _bak = '_bak'
    rename_table = tablename + _bak
    endid = int(get_max_id(rename_table))
    while True:
        if srid < endid:
            insert_new_tab(rename_table,tablename,srid,endid)
            srid = int(get_max_id(tablename))
 #           time.sleep(1)
        elif srid == endid:
            break
    sql = "drop table "+str(rename_table)+""
    sql_command(sql)
    print 'ok,we have done!! end time is %s' %[get_time()]
