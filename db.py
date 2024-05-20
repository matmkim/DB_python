from berkeleydb import db

prompt_header = 'DB_2023-16728> '

# open db
def open_db(db_name):
    myDB = db.DB()
    myDB.open(db_name,dbtype = db.DB_HASH, flags = db.DB_CREATE)
    global_i = "__global__i" # for data insertion (to make each key different)
    x = myDB.get(global_i.encode())
    if not x:
        myDB.put(global_i.encode(),'0'.encode()) # start from 0; ++ after each insertion;
    return myDB

# fetch global_i
def load_global_i(myDB): 
    global_i = myDB.get("__global__i".encode())
    return int(global_i.decode())

# save global_i
def save_global_i(myDB,global_i): # save global_i
    myDB.put("__global__i".encode(), str(global_i).encode())
    
# fetch metadata
def load_metadata(myDB, table_name): 
    metadata_key = f"__meta__{table_name}"
    metadata_value = myDB.get(metadata_key.encode())
    if metadata_value:
        return metadata_value.decode().split("|")
    else:
        return None

# metadata stores information of table (column details)
def save_metadata(myDB, table_name, columns): #
    meta_key = f"__meta__{table_name}"
    
    columns = list(map(lambda x: "@".join(x), columns))
    column_info = "|".join(columns)
    myDB.put(meta_key.encode(), column_info.encode())

# check whether table_name(column) exists also whether it is primary key 
# (this is for checking validity of foreign key constraints - CREATE TABLE)
# if column exists, return the type of column
def check_column_detail(myDB, table_name, column): 
    metadata_key = f"__meta__{table_name}"
    
    return_value=''
    primary_cnt=0

    metadata_value = myDB.get(metadata_key.encode())
    if metadata_value:
        columns_detail = metadata_value.decode().split("|")
        for column_detail in columns_detail:
            x = column_detail.split("@")
            if x[0]==column:
                return_value = x[1]  
                if not x[3].startswith("PRI"):
                    return "non primary"
            if x[3].startswith("PRI"):
                primary_cnt+=1
            
                        
    else: 
        return "non existing table"
    if primary_cnt==1 and return_value:
        return return_value
    if return_value:
        return "non primary"
    return "non existing column"

# check for each CREATE TABLE Error
# if no errors, save the metadata of the table
def create_table(myDB, table_name, columns):
    if table_name == "DuplicatePrimary":
        print(prompt_header+"Create table has failed: primary key definition is duplicated")
        return 
    if table_name.startswith("!"):
        print(prompt_header+f"Create table has failed: '{table_name[1:]}' does not exist in column definition")
        return
    if load_metadata(myDB, table_name) == None:
        column_name = [row[0] for row in columns]
        if len(column_name) != len(set(column_name)):
            print(prompt_header + "Create table has failed: column definition is duplicated")
            return 
        
        for column in columns:
            # check for foreign key cosntraints
            # put metadata of foreign key to the table
            if column[4]:
                r_table, r_column = column[4].split("=")
                check = check_column_detail(myDB, r_table, r_column)
                if check == "non existing table":
                    print(prompt_header+"Create table has failed: foreign key references non existing table")
                    return 
                if check == "non existing column":
                    print(prompt_header+"Create table has failed: foreign key references non existing column")
                    return
                if check == "non primary":
                    print(prompt_header+"Create table has failed: foreign key references non primary key column")
                    return 
                if check != column[1]:
                    print(prompt_header+"Create table has failed: foreign key references wrong type")
                    return
                myDB.put(f"__foreign__{table_name}__{r_table}".encode(), r_table.encode())
            
            if column[1].startswith("char"):
                if int(column[1][5:-1])<1:
                    print(prompt_header + "Char length should be over 0")
                    return


        save_metadata(myDB, table_name, columns)
        print(prompt_header + f"'{table_name}' table is created")
    else: 
        print(prompt_header + f"Create table has failed: table with the same name already exists")


    
# first check whether the table is referenced by other table
# then erase every data related with the table (metadata, foreign key data, inserted data)
def drop_table(myDB, table_name):
    metadata = load_metadata(myDB, table_name)
    if metadata is None:
        print(prompt_header+"No such table")
        return
    
    cursor = myDB.cursor()
    while x:=cursor.next():
        key,value = x
        if key.decode().startswith("__foreign__") and value.decode()==table_name:
            print(prompt_header+f"Drop table has failed: '{table_name}' is referenced by other table")
            return 

    metadata_key = f"__meta__{table_name}"
    myDB.delete(metadata_key.encode())

    f_cursor = myDB.cursor()
    while x:= f_cursor.next():
        key, value = x
        if key.decode().startswith("__foreign__"+table_name) or key.decode().startswith(table_name):
            myDB.delete(key)
    print(prompt_header + f"'{table_name}' table is dropped")

# print metadata of the table in table format
def explain(myDB, table_name):
    metadata = load_metadata(myDB, table_name)
    if metadata is None:
        print(prompt_header+"No such table")
        return
    print('-------------------------------------------------------')
    print(f"table_name [{table_name}]")
    print(f"{'column_name'.ljust(25)}{'type'.ljust(13)}{'null'.ljust(8)}{'key'.ljust(10)}")
    for x in metadata:
        column_name, c_type, null, key, reference= x.split("@")
        print(f"{column_name.ljust(25)}{c_type.ljust(13)}{null.ljust(8)}{key.ljust(10)}")
    print('-------------------------------------------------------')

# list every tables
def show_tables(myDB):
    print('------------------------')
    cursor = myDB.cursor()
    while x := cursor.next():
        key, value = x
        if key.decode().startswith("__meta__"):
            print(key.decode()[8:])
    print('------------------------')

# insert the data to the table
def insert(myDB, table_name, column_name_list, values):
    metadata = load_metadata(myDB, table_name)
    if metadata is None:
        print(prompt_header+"No such table")
        return
    
    table_column_name = [row.split("@")[0] for row in metadata]
    table_column_type = [row.split("@")[1] for row in metadata]
    column_null = [row.split("@")[2] for row in metadata]

    # sort the value in the order of table_column_name
    if column_name_list:
        if len(column_name_list)!=len(values):
            #InsertTypeMismatchError
            print(prompt_header+"Insertion has failed: Types are not matched")
            return

        for i in range(len(column_name_list)):
            if column_name_list[i] not in table_column_name:
                #InsertColumnExistenceError
                print(prompt_header+f"Insertion has failed: '{column_name_list[i]}' does not exist")
                return
        order_map = {column: values[i] for i, column in enumerate(column_name_list)} 
        values=[None]*len(table_column_name)
        for i in range(len(table_column_name)):
            if table_column_name[i] in order_map.keys():
                values[i] = order_map[table_column_name[i]]
            else:
                values[i] = "null"
            if values[i] == "null" and column_null[i] == "N": 
                #InsertColumnNonNullableError
                print(prompt_header+f"Insertion has failed: '{table_column_name[i]}' is not nullable")
                return
    else:
        if len(table_column_name)!=len(values):
            #InsertTypeMismatchError
            print(prompt_header+"Insertion has failed: Types are not matched")
            return 
    

    # for char type; if it is longer than the defined length, slice it
    for i in range(len(table_column_type)):
        if table_column_type[i].startswith("char"):
            if '"' not in values[i] and "'" not in values[i]: # not char
                #InsertTypeMismatchError
                print(prompt_header+"Insertion has failed: Types are not matched")
                return
            if values[i]!="null":
                values[i] = values[i][:int(table_column_type[i][5:-1])+1]
        elif table_column_type[i]=="int":
            if '"' in values[i] or "'" in values[i] or "-" in values[i]: # not int
                #InsertTypeMismatchError
                print(prompt_header+"Insertion has failed: Types are not matched")
                return
        elif table_column_type[i]=="date":
            if "-" not in values[i] or "'" in values[i] or '"' in values[i]: # not date
                #InsertTypeMismatchError
                print(prompt_header+"Insertion has failed: Types are not matched")
                return

    global_i = load_global_i(myDB)
    key = table_name+"@"+str(global_i)
    global_i+=1
    save_global_i(myDB, global_i)
    compressed_values = "@".join(values)
    myDB.put(key.encode(), compressed_values.encode())
    print(prompt_header+"1 row inserted")

def delete(myDB, table_name, where):
    cnt=0
    metadata = load_metadata(myDB, table_name)
    table_column_name = [row.split("@")[0] for row in metadata]

    if metadata is None:
        print(prompt_header+"No such table")
        return
    
    
    f_cursor = myDB.cursor()
    while x:= f_cursor.next():
        key,value = x
        value_decode = value.decode().split("@")
        if key.decode().startswith(table_name):
            # Record of the tablex
            if evaluate_expression(value_decode, table_column_name, where):
                cnt+=1
                myDB.delete(key)
    
    print(prompt_header+f"{cnt} row(s) deleted")

def evaluate_boolean(record, column_names, test):
    if len(test)==4:
        column_name, operator1, operator2, value = test
        operator = operator1+" "+operator2
    else: column_name, operator, value = test
    column_i = column_names.index(column_name)
    record_value = record[column_i]

    if operator == '=':
        return record_value == value
    elif operator == '!=':
        return record_value != value
    elif operator == '>':
        return record_value > value
    elif operator == '<':
        return record_value < value
    elif operator == '>=':
        return record_value >= value
    elif operator == '<=':
        return record_value <= value
    elif operator == "is":
        return record_value == value
    elif operator =="is not":
        return record_value != value
    else:
        raise ValueError(f"Unsupported operator: {operator}")

def evaluate_expression(record, column_names, where):
    print(record)
    if not where:
        return True
    if len(where)==1:
        return evaluate_boolean(record, column_names, where[0])
    if where[1]=="and":
        return evaluate_boolean(record, column_names, where[0]) and evaluate_boolean(record, column_names, where[2])
    if where[1]=="or":
        return evaluate_boolean(record, column_names, where[0]) or evaluate_boolean(record, column_names, where[2])

    
# print every inserted data in table format
def select(myDB, table_name, select_column, where):
    metadata=[]
    for table in table_name:
        load = load_metadata(myDB, table)
        if load is None:
            print(prompt_header+f"Selection has failed: '{table}' does not exist")
            return
        metadata.extend(load)
    print("|"+"-------------------------|"*len(metadata))
    print("|",end='')
    for x in metadata:
        column_name = x.split("@")[0]
        print(f"{column_name.ljust(25)}",end='|')
    print()
    print("|"+"-------------------------|"*len(metadata))
    cursor = myDB.cursor()
    cnt=0
    while x:=cursor.next():
        cnt+=1
        key, value = x
        for name in table_name:
            if key.decode().startswith(name):
                print("|",end='')
                for x in value.decode().split("@"):
                    y=x
                    if y[0] == "'" or y[0] == "\"": y = y[1:]
                    if y[-1] == "'" or y[-1] == "\"": y = y[:-1]
                    print(f"{y.ljust(25)}",end ='|')
                print()

    if cnt==0: print()
    print("|"+"-------------------------|"*len(metadata))

# made for debugging
def print_db(myDB):
    cursor = myDB.cursor()
    while x:= cursor.next():
        print(x)
    cursor.close()
		
def main():
    myDB = open_db("Main_DB")
    print_db(myDB)
    
    myDB.close()

if __name__ == "__main__":
    main()
