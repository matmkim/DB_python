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
    
    # extract each info from metadata
    table_column_name = [row.split("@")[0] for row in metadata]
    table_column_type = [row.split("@")[1] for row in metadata]
    column_null = [row.split("@")[2] for row in metadata]
    key_constraint = [row.split("@")[3] for row in metadata]
    referentials = [row.split("@")[4] for row in metadata]

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
        # sort value order
        order_map = {column: values[i] for i, column in enumerate(column_name_list)} 
        values=[None]*len(table_column_name)
        for i in range(len(table_column_name)):
            if table_column_name[i] in order_map.keys():
                values[i] = order_map[table_column_name[i]]
            else:
                values[i] = "null"
    else:
        if len(table_column_name)!=len(values):
            #InsertTypeMismatchError
            print(prompt_header+"Insertion has failed: Types are not matched")
            return 
    
    for i in range(len(table_column_name)):
        if referentials[i]:
            ref_table, ref_attribute = referentials[i].split("=")
            metadata = load_metadata(myDB, ref_table)
            ref_table_column_name = [row.split("@")[0] for row in metadata]
            ref_i = ref_table_column_name.index(ref_attribute)

        if values[i] == "null" and column_null[i] == "N": 
            #InsertColumnNonNullableError
            print(prompt_header+f"Insertion has failed: '{table_column_name[i]}' is not nullable")
            return
        if key_constraint[i].startswith("PRI"):
            # check duplicate values
            cursor = myDB.cursor()
            while x:= cursor.next():
                key,value = x
                if key.decode().split('@')[0]!= table_name:
                    continue
                value_decode = value.decode().split("@")
                v_i, v_f = value_decode[i], values[i]
                if v_i[0]=="'" or v_i[0]=="\"":
                    v_i, v_f = v_i[1:-1], v_f[1:-1]
                if v_i == v_f:
                    # InsertDuplicatePrimaryKeyError
                    print(prompt_header+"Insertion has failed: Primary key duplication")
                    return
        if key_constraint[i].endswith("FOR"):
            # check if there is the reference
            exist = False
            cursor = myDB.cursor()
            while x:= cursor.next():
                key,value = x
                if key.decode().split('@')[0]!= ref_table:
                    continue
                value_decode = value.decode().split("@")
                v_i, v_f = value_decode[ref_i], values[i]
                if v_i[0]=="'" or v_i[0]=="\"":
                    v_i, v_f = v_i[1:-1], v_f[1:-1]
                if v_i == v_f:
                    exist = True
            if not exist:      
                # InsertReferentialIntegrityError
                print(prompt_header+"Insertion has failed: Referential integrity violation")
                return
        

    # for char type; if it is longer than the defined length, slice it
    for i in range(len(table_column_type)):
        if values[i] == "null": continue
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

# function to check for foreign key constraint during delete
# find if there is a record referencing to primary_values
def check_referential(myDB, primary_values, primary_column, table_name):
    cursor = myDB.cursor()
    ref_table=[]
    while x:= cursor.next():
        key,value = x
        if key.decode().startswith("__foreign__") and key.decode().endswith(table_name):
            ref_table.append(key.decode().split("__")[2])
    for table in ref_table:
        metadata = load_metadata(myDB, table)
        referentials = [row.split("@")[4] for row in metadata]
        index = []
        for attribute in primary_column:
            index.append(referentials.index(table_name+"="+attribute))

        cursor = myDB.cursor()
        # if there is a record referencing to primary_values -> Error
        while x:= cursor.next():
            key,value = x
            if key.decode().split('@')[0]!= table:
                continue
            value_decode = value.decode().split("@")
            equal = True
            for i in range(len(primary_values)):
                v_i, v_f = value_decode[index[i]], primary_values[i]
                if v_i[0]=="'" or v_i[0]=="\"":
                    v_i, v_f = v_i[1:-1], v_f[1:-1]
                if v_i != v_f:
                    equal = False
            if equal: 
                return "ReferentialIntegrity"
    

def delete(myDB, table_name, where):
    cnt=0
    metadata = load_metadata(myDB, table_name)
    if metadata is None:
        print(prompt_header+"No such table")
        return
    table_column_name = [row.split("@")[0] for row in metadata]
    key_constraint = [row.split("@")[3] for row in metadata]
    
    # extract primary keys
    primary_keys = []
    primary_column = []
    for i in range(len(table_column_name)):
        if key_constraint[i].startswith("PRI"):
            primary_keys.append(i)
            primary_column.append(table_column_name[i])

    target=[]
    f_cursor = myDB.cursor()
    error = False
    try:
        while x:= f_cursor.next():
            key,value = x
            value_decode = value.decode().split("@")

            if key.decode().startswith(table_name):
                # Record of the table
                if evaluate_expression(value_decode, table_column_name, where):
                    cnt+=1
                    primary_values=[value_decode[i] for i in primary_keys]
                    if check_referential(myDB, primary_values, primary_column, table_name) == "ReferentialIntegrity":
                        error = True
                    target.append(key)
        if error:
            print(prompt_header+f"{cnt} row(s) are not deleted due to referential integrity")
            return
        for key in target:
            myDB.delete(key)
    except Exception as e:
        e = str(e)
        if e == "IncomparableError":
            print(prompt_header+"Where clause trying to compare incomparable values")
        elif e == "ColumnNotExist":
            print(prompt_header+"Where clause trying to reference non existing column")
        return 
    
    print(prompt_header+f"{cnt} row(s) deleted")

# function to calculate the result of the test for the record 
def evaluate_boolean(record, column_names, test):
    if len(test)==4:
        # 'is not' case
        column_name, operator1, operator2, value = test
        operator = operator1+" "+operator2
    else: column_name, operator, value = test

    if column_name not in column_names:
        raise Exception("ColumnNotExist")

    column_i = column_names.index(column_name)
    record_value = record[column_i]

    # type valid tests
    if value[0].isalpha():
        # value is the column
        column_i = column_names.index(value)
        value = record[column_i]
    if "'" in value or "\"" in value:
        if "'" in record_value or "\"" in record_value:
            record_value = record_value[1:-1]
            value = value[1:-1]
        else:
            raise Exception("IncomparableError")
    elif "-" in value:
        if "-" not in record_value or "'" in record_value or "\"" in record_value:
            raise Exception("IncomparableError")
    elif value == "null":
        pass
    else:
        if "-" in record_value or "'" in record_value or "\"" in record_value:
            raise Exception("IncomparableError")

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

# function to evaluate the expression for the record
def evaluate_expression(record, column_names, where):
    if not where:
        return True
    if len(where)==1:
        return evaluate_boolean(record, column_names, where[0])
    if where[1]=="and":
        return evaluate_boolean(record, column_names, where[0]) and evaluate_boolean(record, column_names, where[2])
    if where[1]=="or":
        return evaluate_boolean(record, column_names, where[0]) or evaluate_boolean(record, column_names, where[2])

# join function
def select_all(myDB, table_name):
    cursor = myDB.cursor()
    records=[[] for i in range(len(table_name))]
    while x:=cursor.next():
        key, value = x
        for i in range (len(table_name)):
            if key.decode().startswith(table_name[i]):
                records[i].append(value.decode().split("@"))
    while len(records)>1:
        joined_records=[]
        for record1 in records[-1]:
            if not record1: return False
            for record2 in records[-2]:
                if not record2: return False
                joined_records.append(record2+record1)
        records[-2]=joined_records
        records.pop()
    return records[0]

# convert the column name into {table_name}.{attribute} form
# test errors
def column_valid_test(table_name_list, metadata, columns):
    converted_columns=[]
    for column in columns:
        if '.' in column:
            if column in metadata:
                converted_columns.append(column)
            else:
                if column.split(".")[0] not in table_name_list:
                    raise Exception(f"TableNotSpecified@{column}")
                raise Exception(f"ColumnNotExist@{column}")
        else:
            cnt=0
            for x in metadata:
                if x.split(".")[1] == column:
                    cnt+=1
                    converted_columns.append(x)
            if cnt>1: 
                raise Exception(f"AmbiguosReference@{column}")
            if cnt==0:
                raise Exception(f"ColumnNotExist@{column}")
    return converted_columns

# filter the joined_record with where clause and projection
def filter_record(joined_records, metadata, where, converted_columns):
    filter_records=[]
    for record in joined_records:
        if evaluate_expression(record, metadata, where):
            split_record=[]
            for column in converted_columns:
                split_record.append(record[metadata.index(column)])
            filter_records.append(split_record)
    
    return filter_records

# print every inserted data in table format
def select(myDB, table_name_list, select_column, where):
    metadata=[]
    for table in table_name_list:
        load = load_metadata(myDB, table)
        if load is None:
            print(prompt_header+f"Selection has failed: '{table}' does not exist")
            return
        for i in range(len(load)):
            load[i] = table+"."+load[i].split('@')[0]

        metadata.extend(load)    
    
    if not select_column: select_column=metadata

    # special case: {table_name}.{attribute}
    for i in range(len(where)):
        if type(where[i])==list:
            for j in range(len(where[i])-1, 0, -1):
                if where[i][j][0].isalpha() and where[i][j-1][0].isalpha():
                    where[i][j-1]=where[i][j-1]+"."+where[i][j]
                    where[i].pop(j)

    ## test for errors & convert into formal form
    try:
        for x in where:
            if type(x)==list:
                converted_column_name =  column_valid_test(table_name_list, metadata, [x[0]])
                x[0] = converted_column_name[0]
                if len(x)==3 and x[2].isalpha():
                    x[2] = column_valid_test(table_name_list, metadata, [x[2]])[0]
    except Exception as e:
        e = str(e).split("@")[0]
        if e == "TableNotSpecified":
            print(prompt_header+"Where clause trying to reference tables which are not specified")
        elif e == "ColumnNotExist":
            print(prompt_header+"Where clause trying to reference non existing column")
        elif e == "AmbiguosReference":
            print(prompt_header+"Where clause contains ambiguous reference")
        return

    try:
        converted_columns = column_valid_test(table_name_list, metadata, select_column)
    except Exception as e:
        column = str(e).split("@")[1]
        print(prompt_header+f"Selection has failed: fail to resolve '{column}'")
    
    joined_records = select_all(myDB, table_name_list)
    try:
        filter_records = filter_record(joined_records, metadata, where, converted_columns)
    except Exception as e:
        e = str(e)
        if e == "IncomparableError":
            print(prompt_header+"Where clause trying to compare incomparable values")
        return 

    print("|"+"------------------------|"*len(converted_columns))
    print("|",end='')
    for x in select_column:
        print(f"{x.ljust(24)}",end='|')
    print()
    print("|"+"------------------------|"*len(converted_columns))
    
    for record in filter_records:
        print("|",end='')
        for x in record:
            y=x
            if y[0] == "'" or y[0] == "\"": y = y[1:]
            if y[-1] == "'" or y[-1] == "\"": y = y[:-1]
            print(f"{y.ljust(24)}",end='|')
        print()
    print("|"+"------------------------|"*len(converted_columns))

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
