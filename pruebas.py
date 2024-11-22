import firebird.driver as fdb

con = fdb.connect(
    database= '192.168.0.9/3050:/var/lib/firebird/3.0/data/prueba.fdb',
    user='sysdba',
    password='masterkey',
    
)

sql = input()
print(sql)
cur = con.cursor()
cur.description
cur.fetchall()
con.begin()
cur.execute(sql)
con.commit()
print(cur.affected_rows)

cur.close()
con.close()