import json
import cx_Oracle
import requests

def add_province():
    response = requests.get("https://provinces.open-api.vn/api/p/")
    # print(response.json())
    json_list_provinces=response.json() #pass_times = response.json()['district']

    try:

        con = cx_Oracle.connect('cv_user/cv_user@localhost:1521/orcl')
        # Now execute the sqlquery
        cursor = con.cursor()
        for d in json_list_provinces:
            province_id = d['code']
            province_name = d['name']
            country_id = 234
            insert_str="insert into PROVINCE values("+str(province_id)+", NULL, '"+str(province_name)+"', "+str(country_id)+")"
            print(insert_str)
            cursor.execute(insert_str)

            # commit that insert the provided data
            con.commit()

        print("value inserted successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)

        # by writing finally if any error occurs
    # then also we can close the all database operation
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

def add_district():
    response = requests.get("https://provinces.open-api.vn/api/d/")
    # print(response.json())
    json_list_districts=response.json()

    try:

        con = cx_Oracle.connect('cv_user/cv_user@localhost:1521/orcl')
        # Now execute the sqlquery
        cursor = con.cursor()
        for d in json_list_districts:
            district_id = d['code']
            district_name = str(d['name'])
            district_name=district_name.replace("'", "''")
            province_id = d['province_code']

            insert_str="insert into DISTRICT values("+str(district_id)+", NULL, '"+district_name+"', "+str(province_id)+")"
            print(insert_str)
            cursor.execute(insert_str)

            # commit that insert the provided data
            con.commit()

        print("value inserted successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)

        # by writing finally if any error occurs
    # then also we can close the all database operation
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()


def add_ward():
    response = requests.get("https://provinces.open-api.vn/api/w/")
    # print(response.json())
    json_list_wards=response.json()

    try:

        con = cx_Oracle.connect('cv_user/cv_user@localhost:1521/orcl')
        # Now execute the sqlquery
        cursor = con.cursor()
        for d in json_list_wards:
            ward_id = d['code']
            ward_name = str(d['name'])
            ward_name = ward_name.replace("'", "''")
            district_id = d['district_code']
            insert_str="insert into WARD values("+str(ward_id)+", NULL, '"+str(ward_name)+"', "+str(district_id)+")"
            print(insert_str)
            cursor.execute(insert_str)

            # commit that insert the provided data
            con.commit()

        print("value inserted successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)

        # by writing finally if any error occurs
    # then also we can close the all database operation
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

def create_table_province():
    # Create a table in Oracle database
    try:

        con = cx_Oracle.connect('cv_user/cv_user@localhost:1521/orcl')

        # Now execute the sqlquery
        cursor = con.cursor()

        # Creating a table srollno heading which is number
        cursor.execute("create table PROVINCE(PROVINCE_ID NUMBER(38,0), PROVINCE_CODE NVARCHAR2(20), PROVINCE_NAME NVARCHAR2(50), COUNTRY_ID NUMBER(38,0))")

        print("Table Created successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)

    # by writing finally if any error occurs
    # then also we can close the all database operation
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()


def create_table_district():
    # Create a table in Oracle database
    try:

        con = cx_Oracle.connect('cv_user/cv_user@localhost:1521/orcl')

        # Now execute the sqlquery
        cursor = con.cursor()

        # Creating a table srollno heading which is number
        cursor.execute("create table DISTRICT(DISTRICT_ID NUMBER(38,0), DISTRICT_CODE NVARCHAR2(20), DISTRICT_NAME NVARCHAR2(50), PROVINCE_ID NUMBER(38,0))")

        print("Table Created successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)

    # by writing finally if any error occurs
    # then also we can close the all database operation
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()


def create_table_ward():
    # Create a table in Oracle database
    try:

        con = cx_Oracle.connect('cv_user/cv_user@localhost:1521/orcl')

        # Now execute the sqlquery
        cursor = con.cursor()

        # Creating a table srollno heading which is number
        cursor.execute("create table WARD(WARD_ID NUMBER(38,0), WARD_CODE NVARCHAR2(20), WARD_NAME NVARCHAR2(50), DISTRICT_ID NUMBER(38,0))")

        print("Table Created successful")

    except cx_Oracle.DatabaseError as e:
        print("There is a problem with Oracle", e)

    # by writing finally if any error occurs
    # then also we can close the all database operation
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()

create_table_province()
add_province()
create_table_district()
add_district()
create_table_ward()
add_ward()

