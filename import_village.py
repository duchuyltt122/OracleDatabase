import pandas as pd
import cx_Oracle

x=pd.ExcelFile('village_data.xlsx')

df = pd.read_excel(x, 0, header=0)
max_rows = int(len(df.iloc[:, 0]))

try:
    con = cx_Oracle.connect('cv_user/cv_user@localhost:1521/orcl')
    cursor = con.cursor()
    for d in range(0,max_rows):
        village_id=str(df['STT'][d])
        village_name=str(df['village_name'][d])
        village_name = village_name.replace("'", "''")
        coordinate=str(df['coordinate'][d])
        ward_id=str(df['ward_id'][d])
        address=str(df['address'][d])+", "
        ward=str(df['ward'][d])+", "
        district=str(df['district'][d])+", "
        province=str(df['province'][d])
        note=village_name+" tại "+address+ward+district+province
        note=note.replace("'", "''")
        hasadded='0'
        insert_str="insert into VILLAGE values("+village_id+", '"+village_name+"', '"+coordinate+"', "+ward_id+", '"+note+"', "+hasadded +")"
        print(insert_str)
        cursor.execute(insert_str)
        con.commit()
    print("value inserted successful")

except cx_Oracle.DatabaseError as e:
    print("There is a problem with Oracle", e)

finally:
    if cursor:
        cursor.close()
    if con:
        con.close()

