import MySQLdb
import sys
import time

def get_connect():
    myconn = MySQLdb.connect(host='192.168.137.2',user='sunyu',passwd='mysql',db='sunyu',charset='utf8',port=3306)
    return myconn

def get_maxid(table_name):
    conn = get_connect()
    cur = conn.cursor()
    sql = "select max(id) from "+str(table_name)+""
    cur.execute(sql)
    return cur.fetchone()[0]
    cur.close()
    conn.close()

def delete_sql(table_name,num_id):
    conn = get_connect()
    cur = conn.cursor()
    sql = "delete from "+str(table_name)+" where id > "+str(num_id)+" order by id limit 100"
#    print sql
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def get_current_id(table_name,num_id):
    try:
        conn = get_connect()
        cur = conn.cursor()
        sql = "select id from "+str(table_name)+" where id > "+str(num_id)+" limit 1"
        cur.execute(sql)
        return cur.fetchone()[0]
    except TypeError as e:
        print 'type error'
    finally:
        cur.close()
        conn.close()

def get_time():
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'we need more canshu'
    else:
        print 'here we go! begin time is %s' %[get_time()]
        tablename = sys.argv[1]
        num = int(sys.argv[2])
        while True:
            max_id = int(get_maxid(tablename))
            current_id = get_current_id(tablename,num)
#            print 'regions delete id is %s' %current_id
#            print 'max_id,num is %d %d' %(max_id,num)
            if max_id <= num:
                break
            else:
                delete_sql(tablename,num)
#            time.sleep(1)
    print 'delete is complate'
    print 'ok,we have done!! end time is %s' %[get_time()]
