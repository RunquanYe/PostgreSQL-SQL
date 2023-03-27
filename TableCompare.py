#!/usr/bin/env python
from openpyxl import Workbook
import psycopg2
import random 
import functools

'''
Programmer: Runquan Ye
'''

tables1 = ['temp_1990', 'temp_1991', 'temp_1992', 'temp_1993', 'temp_1994', 'temp_1995', 'temp_1996', 'temp_1997', 'temp_1998', 'temp_1999', 'temp_2000', 'temp_2001', 'temp_2002', 'temp_2003', 'temp_2004', 'temp_2005', 'temp_2006', 'temp_2007', 'temp_2008', 'temp_2009', 'temp_2010', 'temp_2011', 'temp_2012', 'temp_2013', 'temp_2014', 'temp_2015', 'temp_2016', 'temp_2017']

try:
    connection = psycopg2.connect(
        host = "localhost",
        database = "cri",
        port = "5432")

    columnNames = []
    schemaName = "ews"
    testSize = 1000
    with connection:
        with connection.cursor() as cursor:
            resultDisplay = {}
            for t in tables1:
                print(f"Start to check target table {tables1.index(t) + 1}, {t}" )
                cursor.execute('select count(*) from {0}."{1}"'.format(schemaName, t))
                result1 = cursor.fetchall()
                tableSize =  int(str(result1[0][0]))
                if testSize > tableSize:
                    testSize = tableSize
                targetList = random.sample(range(tableSize), testSize)

                cursor.execute('select "employer_id_number" from {0}."{1}"'.format(schemaName, t))
                
                einList = [ein[0] for ein in cursor] 
                testEINList = [einList[i] for i in targetList]

                trueList = []
                falseEIN = []
                tableInfo = {
                    "# of True": 0,
                    "# of False": 0,
                    "False EINs": [],
                }
                for k in testEINList:
                    cursor.execute('select * from {0}."{1}" where "employer_id_number" = '.format(schemaName, t) + f"'{k}';" )
                    tempOutput = [str(k2) for k1 in list(cursor.fetchall()) for k2 in k1]
                    
                    cursor.execute('select * from {0}."master_nccs" where "file_year" = {1} and "filer_ein" = '.format(schemaName, int(str(t)[-4:])) + f"'{k}';" )
                    masterOutput = [str(k2) for k1 in list(cursor.fetchall()) for k2 in k1]

                    if not functools.reduce(lambda x,y: x and y, map(lambda p,q: p == q, tempOutput, masterOutput), True):
                        falseEIN.append(str(k))
            
                tableInfo['# of True'] = testSize - len(trueList)
                tableInfo['# of False'] = len(falseEIN)
                tableInfo['False EINs'] = falseEIN
                resultDisplay[str(t)[-4:]] = tableInfo
                print(f"Finish Checking target table {tables1.index(t) + 1}, {t}" )
            print("\nDisplay Table Analysis:")
            for key, value in resultDisplay.items():
                print("\t", key, " : ", value)

except (Exception, psycopg2.Error) as error :
    print ("Error: ", error)

finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
