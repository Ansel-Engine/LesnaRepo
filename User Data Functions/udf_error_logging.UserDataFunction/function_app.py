import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB",alias="ASQLDatabase")
@udf.function()
def insert_error2(sqlDB: fn.FabricSqlConnection, pipelinename: str, rundate: str, runid: str, pipelinestatus: str, pipelineid: str, errormessage: str, errorcode: str)-> list:

    # Establish a connection to the SQL database
    connection = sqlDB.connect()
    cursor = connection.cursor()

    #run the stored proc to insert the data
    insert_description_query = "exec dbo.InsertJobStatus ?, ?, ?, ?, ?, ?, ?;"
    cursor.execute(insert_description_query, (pipelinename, rundate, runid, pipelinestatus, pipelineid, errormessage, errorcode))

    #Commit the transaction
    connection.commit()
    cursor.close()
    connection.close()

    return "error logged"