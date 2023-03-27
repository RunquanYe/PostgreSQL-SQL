import psycopg2

'''
Programmer: Runquan Ye
'''

# columns = ['compensation_for_officers', 'total_revenue', 'total_expenses', 'total_assets_eoy', 'total_liabilities_eoy', 'net_investment_income', 'contributions_paid', 'contributions_received', 'net_income', 'charity_expenses', 'inv_gov_oblig', 'inv_corp_bonds', 'inv_corp_stocks', 'securities']

columns = ['cash', 'rental_expenses']

schemaName = 'ews'
tableName = 'master_nccs_pf'
toNoneVarType = 'text'

try:
    connection = psycopg2.connect(
        host = "localhost",
        database = "cri",
        port = "5432")
    # create cuursor
    cursor = connection.cursor()

    with connection:
        with connection.cursor() as cursor:
            print(f"Start to alter table {tableName} column type to '{toNoneVarType}' with total columns: {len(columns)}")
            for cName in columns:
                print(f"Start Altering target column {columns.index(cName) + 1}, {cName}" )

                target = 'ALTER TABLE {0}.{1} ALTER COLUMN "{2}" TYPE {3} USING "{2}"::{3};'.format(schemaName, tableName, cName, toNoneVarType) 
                
                cursor.execute(target)
                print(f"Finish Altering target column  {columns.index(cName) + 1}, {cName}" )
            print(f"\nFinish altering table {tableName} column type to '{toNoneVarType}'.")
            
except (Exception, psycopg2.Error) as error :
    print ("Error: ", error)

finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")