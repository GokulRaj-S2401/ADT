from cassandra.cluster import Cluster
import pandas as pd
clustr = Cluster()
session = clustr.connect('container')
tableName = 'student'
while True:
    try:
        mdes = input("\t1.table operations\t2.crud operations\n")
        if mdes == 1:
            subDes = input("\t1.alter table \t 2.Truncate table \n")
            if subDes ==1:
                des = input("\t1.add \t2.drop \n")
                if des==1:
                    colname = raw_input("Enter column name \n")
                    coltype = raw_input("column type \n")
                    session.execute("alter table "+tableName+" add "+colname+" "+coltype)
                elif des==2:
                    colname = raw_input("Enter column name\n")
                    session.execute("alter table "+tableName+" drop "+colname)
                else:
                    print("try again later\n")
            elif subDes ==2:
                session.execute("truncate "+tableName)
                print("deleted all records")
        elif mdes==2:
            des = input("\t1.create\t2.read\t3.update\t4.delete\n")
            if des ==1:
                result = session.execute('select * from '+tableName)
                df = pd.DataFrame(result)
                col = df.columns.values.tolist()
                data = []
                for x in col:
                    temp = raw_input("Enter "+x)
                    data.append(temp)
                per_s = ["%s" for x in col ]
                per_str = ''
                for x in per_s:
                    per_str =per_str+x+','
                colList_str = ''
                for x in col:
                    colList_str = colList_str+x+','
                dataList_str = ''
                for x in data:
                    dataList_str = dataList_str+"'"+x+"'"+','
                print(dataList_str)
                qry = 'insert into '+tableName+'('+colList_str[:-1]+') values('+dataList_str[:-1]+');'
                print(qry)
                session.execute(qry)
            elif des==2:
                result = session.execute('select * from '+tableName)
                df = pd.DataFrame(result)
                print(df)
            elif des==3:
                s_id = raw_input("Enter student id \n")
                name = raw_input("Enter new name \n")
                session.execute("update "+tableName+" set name='"+name+"' where id="+"'"+s_id+"'") 
                print("updated !\n")
            elif des==4:
                s_id = raw_input("Enter student id \n")
                session.execute("delete from "+tableName+" where id="+"'"+s_id+"'")
                print(s_id+" deleted successfully!")  
    except:
        print('wrong input')
        exit()
# session.execute("create keyspace conts with replication={'class':'SimpleStrategy','replication_factor':1};")
