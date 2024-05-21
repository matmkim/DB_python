from lark import Lark, Transformer, Token
from db import *

# Customized Transformer 
# Parsing result as input
class SqlTransformer(Transformer):
    # return the content of the items (unpacking)
    def command(self,items): 
        (items,)=items
        return items
    
    # return the content of the items (unpacking)
    def query_list(self,items): 
        (items,)=items
        return items
    
    # return the content of the items (unpacking)
    def query(self,items): 
        (items,)=items
        return items
    
    # for *_query token, return the appropriate string
    def select_query(self,items):
        
        column_name = list(map(lambda x: ".".join(x.scan_values(lambda v: isinstance(v,Token))),  list(items[1].find_data('selected_column'))))
        table_name =  list(set(map(lambda x: "".join(x.scan_values(lambda v: isinstance(v,Token))),  list(items[2].find_data('table_name')))))
        
        where=[]
        if items[2].find_data('where_clause'):
            tests = list(items[2].find_data("boolean_test"))
            and_or = list(map(lambda x: x.value, list(items[2].scan_values(lambda v: isinstance(v,Token) and (v.type == "OR" or v.type == "AND")))))
            for test in tests:
                where.append(list(map(lambda x: x.value, test.scan_values(lambda v: isinstance(v,Token)))))
            if and_or:
                where.insert(1, and_or[0])
        
        return "SELECT", table_name, column_name, where
    
    def create_table_query(self,items): 
        table_name = items[2].children[0].lower()
        table_constraints = items[3].find_data("table_constraint_definition")
        # dictionary for primary constraints
        primary = dict() 
        primary_def_cnt =0 
        # dictionary for referential constraints
        referential = dict()
        for x in table_constraints:
            table_constraint = list(map(lambda x: x.value, x.scan_values(lambda v: isinstance(v,Token))))
            if table_constraint[0] == 'primary': 
                primary_def_cnt+=1
                for i in range(3,len(table_constraint)-1):
                    primary[table_constraint[i].lower()] = True
            if table_constraint[0] == 'foreign':
                referential[table_constraint[3].lower()] = table_constraint[6].lower()+"="+table_constraint[8].lower()
        # more than one primary constraint definition
        if primary_def_cnt>1: table_name = "DuplicatePrimary"

        column_definition = items[3].find_data("column_definition")
        column_details=[]
        for x in column_definition:
            # column_name, type, null, key, foreign key info
            column_detail = ['']*5
            column_detail[0] = "".join(list(x.find_data("column_name"))[0].scan_values(lambda v: isinstance(v, Token))).lower()
            column_detail[1] = "".join(list(x.find_data("data_type"))[0].scan_values(lambda v: isinstance(v, Token)))
            # if not null or primary key 
            column_detail[2] = 'N' if list(x.scan_values(lambda v: isinstance(v, Token) and v.value=="null")) or primary.get(column_detail[0]) else 'Y' 
            column_detail[3] += "PRI" if primary.get(column_detail[0]) else ""
            if referential.get(column_detail[0]):
                if column_detail[3] != "": column_detail[3]+="/"
                column_detail[3] += "FOR"
                column_detail[4] = referential[column_detail[0]]
            column_details.append(column_detail)
        
        # check non existing column
        non_key=[]
        column_names = set([row[0] for row in column_details])
        non_key += [key for key in primary.keys() if key not in column_names]
        non_key += [key for key in referential.keys() if key not in column_names]
        if non_key:
            table_name = "!"+non_key[0]
        
        return "CREATE TABLE", table_name, column_details
    
    def drop_table_query(self,items): 
        table_name = items[2].children[0].lower()
        return "DROP TABLE", table_name
    
    def explain_query(self,items): 
        table_name = items[1].children[0].lower()
        return "EXPLAIN", table_name
    
    def describe_query(self,items): 
        table_name = items[1].children[0].lower()
        return "EXPLAIN", table_name
    
    def desc_query(self,items):
        table_name = items[1].children[0].lower()
        return "EXPLAIN", table_name
    
    def show_tables_query(self,items): 
        return "SHOW TABLES"
    
    def insert_query(self,items): 
        table_name = items[2].children[0].lower()
        column_name_list = []
        if items[3] != None:
            # extract column_name_list
            column_name_list=list(map(lambda x: x.value.lower(),list(list(items[3].find_data('column_name_list'))[0].scan_values(lambda v: isinstance(v, Token) and v.type=="IDENTIFIER"))))
        values=[]
        for x in items[5].scan_values(lambda v: isinstance(v, Token)):
            if x=="(" or x == ")":
                continue
            values.append(x.value)
        return "INSERT", table_name, column_name_list, values
    
    def delete_query(self,items):
        where=[]
        table_name= items[2].children[0].lower()
        if items[3]:
            tests = list(items[3].find_data("boolean_test"))
            and_or = list(map(lambda x: x.value, list(items[3].scan_values(lambda v: isinstance(v,Token) and (v.type == "OR" or v.type == "AND")))))
            for test in tests:
                where.append(list(map(lambda x: x.value, test.scan_values(lambda v: isinstance(v,Token)))))
            if and_or:
                where.insert(1, and_or[0])
        return "DELETE", table_name, where
    
    def update_query(self,items): 
        return "'UPDATE' requested"

# create sql_parser based on "grammar.lark"
with open("grammar.lark") as file:
    sql_parser = Lark(file.read(),start="command", lexer="basic")

myDB = open_db("Main_DB")

prompt_header = 'DB_2023-16728> '

# initialize
sql_input=''
flag=True

while flag:
    sql_input += input(prompt_header).strip()

    # if not ended with semicolon, continue input
    if not sql_input or ';' != sql_input[-1]:
        sql_input+=" " # seperate each line with whitespace
        continue
        
    input_lines = sql_input.split(";") # split by semicolon

    # Parse line-by-line
    # input_lines[-1] is blank, beacuase of the right-most semicolon
    for input_line in input_lines[:-1]: 
        #try:
        output = sql_parser.parse(input_line.strip()+";") # parse
        result = SqlTransformer().transform(output) # transform
        '''except:
            # It means that syntax error occured
            print(prompt_header+'Syntax error') 

            break # stop parsing next query'''

        if result=="exit": flag=False; break # end the program

        if type(result) is str:     # SHOW TABLES, DELETE, UPDATE
            if result == "SHOW TABLES": 
                show_tables(myDB)
            else: print(prompt_header+result) 
        
        else:   # send the transform result to the db_function
            if result[0] == "CREATE TABLE":
                create_table(myDB, result[1], result[2])
            elif result[0] == "DROP TABLE":
                drop_table(myDB, result[1])
            elif result[0] == "EXPLAIN":
                explain(myDB, result[1])
            elif result[0] == "INSERT":
                insert(myDB, result[1], result[2], result[3])
            elif result[0] == "SELECT":
                select(myDB, result[1],result[2], result[3])
            elif result[0] == "DELETE":
                delete(myDB, result[1], result[2])

    sql_input = '' # initialize