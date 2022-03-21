import pandas as pd
import mysql.connector
import psycopg2
import numpy as np
def cursor_to_list(cursor):
    heading = [i[0] for i in cursor.description]
    list_data = []
    for row in cursor:
        each_data = {}
        for index, key in enumerate(heading):
            if key == 'course_json' and type(row[index]) == str:
                each_data[key] = json.loads(row[index])
            else:
                each_data[key] = row[index]
        list_data.append(each_data)
    return(list_data)
def findHireidclosed(col):

    if col[0]>col[1] and col[0]<col[2]:
        
        return col[-1]
    else:
        return "Not found"
def findHireidopened(col):

    if col[0]>col[1] :
        
        return col[-1]
    else:
        return "Not found"
connection = psycopg2.connect(user = "postgres",password = "dinesh",host = "127.0.0.1",port = "5432",database = "sampledb")
psqlcursor = connection.cursor()
q = "SELECT id,vehicle_number,event_date_time FROM vehiclealert WHERE hire_id IS NULL"
psqlcursor.execute(q)
def maphireid(psqlcursor):

    psqldata = pd.DataFrame(cursor_to_list(psqlcursor))
    psqldata["vehicle_number"] = psqldata["vehicle_number"].apply(lambda x: x.replace("-", ""))
    psqldata["event_date_time"] = pd.to_datetime(psqldata['event_date_time'])
    psqldata['event_date_time'] = psqldata['event_date_time'].apply(lambda x: x.replace(tzinfo=None))
    #psqldata["event_date_time"]=psqldata["event_date_time"].apply(lambda x:x+(pd.Timedelta(minutes=330)))
    a = input("enter the status of the hire you have to update : ")
    connection2 = mysql.connector.connect(host = "localhost",user ="root",passwd = "dinesh",database = "transport_app")
    sqlcursor = connection2.cursor()
    

    sqlcursor.execute("select from_date+from_time as fromdate,to_date+to_time as todate,testdata.vehicle_id as verify1,vehicle.id as verify2,vehicle.vehicle_number,testdata.id as hireid ,testdata.status from testdata join vehicle on testdata.vehicle_id=vehicle.id where testdata.status=0")
    closedHires = pd.DataFrame(cursor_to_list(sqlcursor))
    # operations on closed hires
    closedHires["vehicle_number"] = closedHires["vehicle_number"].apply(lambda x: x.replace(" ", ""))
    closedHires["fromdate"] = pd.to_datetime(closedHires['fromdate'],unit='ms')
    #closedHires["fromdate"]=closedHires["fromdate"].apply(lambda x:x+(pd.Timedelta(minutes=330)))
    closedHires["todate"] = pd.to_datetime(closedHires['todate'],unit='ms')
    #closedHires["todate"]=closedHires["todate"].apply(lambda x:x+(pd.Timedelta(minutes=330)))

        #print(closedHires)
    #connect to the psql 
    closedHireMerged = psqldata.merge(closedHires,on = 'vehicle_number')
    if(len(closedHireMerged)!=0):
        closedHireMerged['hire_id_insert']=closedHireMerged[['event_date_time','fromdate','todate','hireid']].apply(findHireidclosed,axis = 1)
    for i in range(len(closedHireMerged)):
    #print(closedHireMerged.iat[i,9])
        if(closedHireMerged.iat[i,9]!='Not found'):
        #print(closedHireMerged.iat[i,9])
            update_query = "update vehiclealert set hire_id = "+str(closedHireMerged.iat[i,9])+" where id = "+str(closedHireMerged.iat[i,0])
            psqlcursor.execute(update_query)
            connection.commit()
   
  
    sqlcursor.execute("select from_date+from_time as fromdate,testdata.vehicle_id as verify1,vehicle.id as verify2,vehicle.vehicle_number,testdata.id as hireid,testdata.status from testdata join vehicle on testdata.vehicle_id=vehicle.id where testdata.status=1")
    openHires = pd.DataFrame(cursor_to_list(sqlcursor))
    openHires["vehicle_number"] = openHires["vehicle_number"].apply(lambda x: x.replace(" ", ""))
    openHires["fromdate"] = pd.to_datetime(openHires['fromdate'],unit='ms')
        #openHires["fromdate"]=openHires["fromdate"].apply(lambda x:x+(pd.Timedelta(minutes=330)))
    openedHireMerged = psqldata.merge(openHires,on = 'vehicle_number')
    if(len(openedHireMerged)!=0):
        openedHireMerged['hire_id_insert']=openedHireMerged[['event_date_time','fromdate','hireid']].apply(findHireidopened,axis = 1)
    for i in range(len(openedHireMerged)):
    #print(openedHireMerged.iat[i,8])
        if(openedHireMerged.iat[i,8]!='Not found'):
       #print(openedHireMerged.iat[i,8])
            update_query = "update vehiclealert set hire_id = "+str(openedHireMerged.iat[i,8])+" where id = "+str(openedHireMerged.iat[i,0])
            psqlcursor.execute(update_query)
            connection.commit()
maphireid(psqlcursor)



