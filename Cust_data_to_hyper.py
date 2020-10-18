from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException

def run_create_hyper_file():
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        #create new Hyper File "CustomerData.hyper".
        with Connection(endpoint=hyper.endpoint,database="CustomerData.hyper",
                        create_mode=CreateMode.CREATE_AND_REPLACE
                        ) as connection:
            extract_table = TableDefinition(table_name= TableName("Extract","Extract"),columns=[
                    TableDefinition.Column("Customer ID", SqlType.text(), NOT_NULLABLE),
                    TableDefinition.Column("Customer Name", SqlType.text(), NOT_NULLABLE),
                    TableDefinition.Column("Item Price", SqlType.big_int(), NOT_NULLABLE),
                    TableDefinition.Column("Item Name", SqlType.text(), NOT_NULLABLE),
                    TableDefinition.Column("Payment Mode", SqlType.text(), NOT_NULLABLE)
                    ])
            connection.catalog.create_schema("Extract")
            connection.catalog.create_table(extract_table)
            print("Table Created")

            #Add Data to Table
            data = [["1","Abhishek Pal",1000,"Power Bank","Debit Card"],
                    ["2","Rahul",500,"Wallet","Credit_card"],
                    ["3","Toni",600,"Sunglasses","Cash"],
                    ["4","Bob",200,"Book","Phone Pay"],
                    ]
            with Inserter(connection,extract_table) as inserter:
                inserter.add_rows(data)
                inserter.execute()
                print("Data Inserted in Table")
                
def read_data_from_hyper_file():
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Read data from hyper file "CustomerData.hyper".
        with Connection(endpoint=hyper.endpoint,database="CustomerData.hyper",
                        create_mode=CreateMode.NONE
                        ) as connection:
            print("Getting Data from Table")
            print(connection.execute_list_query(f"select * from {TableName('Extract','Extract')}"))

def add_new_column_in_hyper_file():
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Add new column to hyper file "CustomerData.hyper".
        with Connection(endpoint=hyper.endpoint,database="CustomerData.hyper",
                        create_mode=CreateMode.NONE
                        ) as connection:
            connection.execute_command(f"ALTER TABLE {TableName('Extract','Extract')} ADD COLUMN Age int")
            print("Column Added")
            connection.execute_command(f"UPDATE {TableName('Extract','Extract')} SET Age = 27")
            print("Values added to Column")
            print(connection.execute_list_query(f"select * from {TableName('Extract','Extract')}"))

def delete_rows_hyper_file():
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Delete data from hyper file "CustomerData.hyper".
        with Connection(endpoint=hyper.endpoint,database="CustomerData.hyper",
                        create_mode=CreateMode.NONE
                        ) as connection:
            row_count= connection.execute_command(f"DELETE FROM {TableName('Extract','Extract')} WHERE {escape_name('Customer ID')} = {escape_string_literal('4')}")
            print("Column Deleted")
            print(f"The number of deleted rows from table is {row_count}. ")
            print("Current data we have is")
            print(connection.execute_list_query(f"select * from {TableName('Extract','Extract')}"))


if __name__ == '__main__':
    try:
        run_create_hyper_file()
        read_data_from_hyper_file()
        add_new_column_in_hyper_file()
        delete_rows_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)            


    
            
