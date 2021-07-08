import mysql.connector
import datetime
import time

con = mysql.connector.connect(user='username', password='password', host ='hostname',
                                  database='databasename')
c = con.cursor()

var_first_date = datetime.datetime(2021,1,1)
var_last_date = datetime.datetime(2021,2,1)

print var_first_date
print var_last_date

#######--------------------db.table1 table delete process----------------------########
sql = 'select min(numberid), max(numberid), count(1) from db.table1 where numberid in ( select numberid from db.table2 where jobid in ( select jobid  from db.table3 where jobcreationtime between %s and %s))'


params = (var_first_date, var_last_date)
c.execute(sql,params)

result_set = c.fetchall()
print 'db.table1 table deletion stats'
print 'min numberid: ', result_set[0][0]
print 'max numberid: ', result_set[0][1]
print 'row count: ', result_set[0][2]

var_loop = result_set[0][2]//1000
print 'total loop count:', var_loop

sql =  'insert into purgeStatsDB.dataPurgeStats(tableName, minID, maxID, deleteStarttime) values(%s, %s, %s, %s)'
val = ('db.table1', result_set[0][0], result_set[0][1], datetime.datetime.now())
c.execute(sql,val)
con.commit()


sql = 'select max(id) from purgeStatsDB.dataPurgeStats'
c.execute(sql)
getID = c.fetchall()
print 'id value of the new entry on purgestat table', getID[0][0]


i = 0
#the line below needs to be deleted, it is used for testing
var_loop = 5
while i <= var_loop:
  sql = "DELETE FROM db.table1 where numberid >= %s and numberid <= %s limit 1000"
  val = (result_set[0][0], result_set[0][1])
  c.execute(sql,val)
#once this script approved, rollback should be commit, rollback is used for testing
  con.rollback()
  i = i +1
  print i*1000, ' records deleted'
  sql =  'update purgeStatsDB.dataPurgeStats set deletedRowsCount = %s where id = %s'
  val = (i*1000, getID[0][0])
  c.execute(sql,val)
  con.commit()
#the line below needs to be deleted, it is used for testing
  time.sleep(5)
sql =  'update purgeStatsDB.dataPurgeStats set deletedRowsCount = %s, deleteEndtime = %s where id = %s'
val = (result_set[0][2],datetime.datetime.now(), getID[0][0])
c.execute(sql,val)
con.commit()



#######--------------------db.table2 table delete process----------------------########


sql = 'select min(NumberID), max(NumberID), count(1) from db.table2 where jobid in (select jobid from db.table3 where jobcreationtime between %s and %s)'
params = (var_first_date, var_last_date)

c.execute(sql,params)


result_set = c.fetchall()
print 'db.table2 table deletion stats'
print 'min numberid: ', result_set[0][0]
print 'max numberid: ', result_set[0][1]
print 'row count: ', result_set[0][2]

var_loop = result_set[0][2]//1000
print 'total loop count:', var_loop



sql =  'insert into purgeStatsDB.dataPurgeStats(tableName, minID, maxID, deleteStarttime) values(%s, %s, %s, %s)'
val = ('db.table2', result_set[0][0], result_set[0][1], datetime.datetime.now())
c.execute(sql,val)
con.commit()


sql = 'select max(id) from purgeStatsDB.dataPurgeStats'
c.execute(sql)
getID = c.fetchall()
print 'id value of the new entry on purgestat table', getID[0][0]


i = 0
#the line below needs to be deleted, it is used for testing
var_loop = 5
while i <= var_loop:
  sql = "DELETE FROM db.table2 where numberid >= %s and numberid <= %s limit 1000"
  val = (result_set[0][0], result_set[0][1])
  c.execute(sql,val)
#once this script approved, rollback should be commit, rollback is used for testing
  con.rollback()
  i = i +1
  print i*1000, ' records deleted'
  sql =  'update purgeStatsDB.dataPurgeStats set deletedRowsCount = %s where id = %s'
  val = (i*1000, getID[0][0])
  c.execute(sql,val)
  con.commit()
#the line below needs to be deleted, it is used for testing
  time.sleep(5)
sql =  'update purgeStatsDB.dataPurgeStats set deletedRowsCount = %s, deleteEndtime = %s where id = %s'
val = (result_set[0][2],datetime.datetime.now(), getID[0][0])
c.execute(sql,val)
con.commit()




#######--------------------db.table3 table delete process----------------------########


sql = 'select min(jobid), max(jobid), count(1) from db.table3 where jobcreationtime between %s and %s'
params = (var_first_date, var_last_date)

c.execute(sql,params)

    
result_set = c.fetchall()
print 'db.table3 table deletion stats'
print 'min jobid: ', result_set[0][0]
print 'max jobid: ', result_set[0][1]
print 'row count: ', result_set[0][2]

var_loop = result_set[0][2]//1000
print 'total loop count:', var_loop


sql =  'insert into purgeStatsDB.dataPurgeStats(tableName, minID, maxID, deleteStarttime) values(%s, %s, %s, %s)'
val = ('db.table3', result_set[0][0], result_set[0][1], datetime.datetime.now())
c.execute(sql,val)
con.commit()


sql = 'select max(id) from purgeStatsDB.dataPurgeStats'
c.execute(sql)
getID = c.fetchall()
print 'id value of the new entry on purgestat table', getID[0][0]


i = 0
#the line below needs to be deleted, it is used for testing
var_loop = 5
while i <= var_loop:
  sql = "DELETE FROM db.table3 where jobid >= %s and jobid <= %s limit 1000"
  val = (result_set[0][0], result_set[0][1])
  c.execute(sql,val)
#once this script approved, rollback should be commit, rollback is used for testing
  con.rollback()
  i = i +1
  print i*1000, ' records deleted'
  sql =  'update purgeStatsDB.dataPurgeStats set deletedRowsCount = %s where id = %s'
  val = (i*1000, getID[0][0])
  c.execute(sql,val)
  con.commit()
#the line below needs to be deleted, it is used for testing
  time.sleep(5)
sql =  'update purgeStatsDB.dataPurgeStats set deletedRowsCount = %s, deleteEndtime = %s where id = %s'
val = (result_set[0][2],datetime.datetime.now(), getID[0][0])
c.execute(sql,val)
con.commit()

    
c.close()
