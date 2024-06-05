import pickle
import re
from flask import Flask, Response, jsonify, make_response, redirect,render_template, request, session


master_session={}


with open('JSONs/params.json','r') as f:
    params = json.load(f)

with open('JSONs/EXAMPLE_sample_table_desciption.json','r') as f:
    description = json.load(f)

with open('JSONs/EXAMPLE_sample_base_prompt.json','r') as f:
    template_info = json.load(f)

with open("JSONs/prompt.txt", 'r') as file:
    prompt_txt = file.read()


model_id1 = ModelTypes.MIXTRAL_8X7B_INSTRUCT_V01_Q #STARCODER#MIXTRAL_8X7B_INSTRUCT_V01_Q
parameters = {
    GenParams.DECODING_METHOD: "sample",
    GenParams.MAX_NEW_TOKENS: 1500,
    GenParams.REPETITION_PENALTY: 1,
    GenParams.TEMPERATURE: 0.1,
    GenParams.STOP_SEQUENCES: ["<end_of_description>"]
}
model = Model(
    model_id=model_id1, 
    params=parameters, 
    credentials=params['WMLCred'],
    project_id=params['WatsonXPID'])

parameters_query_model = {
    GenParams.DECODING_METHOD: "sample",
    GenParams.MAX_NEW_TOKENS: 1500,
    GenParams.REPETITION_PENALTY: 1,
    GenParams.TEMPERATURE: 0.1,
    GenParams.STOP_SEQUENCES: ["<end_of_code>"]
}
query_model = Model(
    model_id=model_id1, 
    params=parameters_query_model, 
    credentials=params['WMLCred'],
    project_id=params['WatsonXPID'])

app = Flask(__name__)
app.secret_key = '123456'


@app.route('/')
@app.route('/login')
def login():
    return render_template('login/login.html')


@app.route('/verifylogin', methods=['POST'])
def verifylogin():
    if request.method == 'POST':
        username=request.form['username']
        password=request.form['password']
    with open('users.json', 'r') as f:
        users_data = json.load(f)
        if username in users_data and users_data[username] == password:
            data={'msg':'success','user':'true','password':'false'}  
            session['user']=username
            master_session[session['user']]={}
            print(master_session)
            return data
        else:
            if username in users_data:
                data={'msg':'error','user':'right','password':'wrong'}   
                return data
            else :
                data={'msg':'error','user':'wrong','password':'wrong'}
                return data

@app.route('/logout')
def logout():
    del master_session[session['user']]
    del session['user']
    print(master_session)
    print(session)
    return redirect('/')

@app.route('/disconnect',methods=['GET'])
def disconnect():
    try:
        master_session[session['user']]['conn'][0].close()
        del master_session[session['user']]['conn']
        del master_session[session['user']]['metadata']
        print(master_session)
        return 'success'
    except Exception as e :
        print(e)
        return 'error'


@app.route('/main')
def main():
    print('vvvv',session)
    return render_template('index.html')


def test_sql_query1(query, conn, parse_type):
    print('called test_sql_query',40*'-')
    try:
        result = conn.execute(text(query['Answer']))
        print('Example_answ',20*'=')
        rows = result.fetchall()
        print(rows)
        return rows
    except Exception as e:
        pass

@app.route('/getquery',methods=['POST'])
def getquery():
    print('called getquery',40*'-')
    try:
        if request.method=='POST':
            query=request.form['qry']
            print(query)
            dbdata=master_session[session['user']]['conn']
            print(dbdata)
            hashed_conn_string = ret_hash(dbdata[1])
            db_desc_filepath = os.path.join(os.path.join(params['DB_Details'],hashed_conn_string),params['DB_DESC_FILENAME'])
            db_ex_filepath = os.path.join(os.path.join(params['DB_Details'],hashed_conn_string),params['DB_EXAMPLE_FILENAME'])
            with open(db_ex_filepath,'r') as f:
                examples = json.load(f)

            with open(db_desc_filepath,'r') as f:
                description = json.load(f)

            if not os.path.exists(params['Cache_DB_folder']):
                os.makedirs(params['Cache_DB_folder'])
            print('running 1')  
            cache_path = os.path.join(params['Cache_DB_folder'],params['Cache_DB_filename'])
            db_cache = get_cacheDB(hashed_conn_string)
            print("*************1.6")
            sql_database = SQLDatabase(dbdata[2], sample_rows_in_table_info=2)
            print("object==========",sql_database)
            context = generate_context(sql_database,description,template_info)
            print('running 2')
            prompt = generate_prompt(template_info,examples,context,query)
            query_start='INPUT: Write a mssql query by correctly using column names, table name, JOIN operations , GROUP BY , ORDER BY and mathematical function LIKE SUM,AVERAGE etc. operations to get '
            query1 = query_start+query
            print('prompt start ====================')
            print(prompt)
            print('prompt end ====================')
            result=exec_query(prompt,query_model,dbdata[1],query1,dbdata[0],query)

            #print('myresult',result)
            if result == None:
                print('none')
                return 'tryagain'
            else:
                print('result',result)
                # result='hh'
                return result
    except Exception as e:
        print(e)
        return 'error'



@app.route('/connectdb' ,methods=['POST'])
def conectdb():
    print('called conectdb',40*'-')
    try:
        if (request.method == 'POST'):
            db_host=request.form['hostname']
            db_user=request.form['user']
            db_password=request.form['password']
            db_port=request.form['portno']
            db_name=request.form['database']   
            conn=connectmysqldb(db_user,db_password,db_host,db_port,db_name)
            print("12345",session)
            master_session[session['user']]['conn']=conn
            master_session[session['user']]['metadata']={ 
                                                        "db_host":request.form['hostname'], 
                                                        "db_user":request.form['user'],
                                                        "db_password":request.form['password'],
                                                        "db_port":request.form['portno'],
                                                        "db_name":request.form['database']   
                                                    }
            print(master_session)
            inspector = inspect(conn[2])
            print("inspector", inspector)
            table_names = inspector.get_table_names()
            print("table names", table_names)
            mastertbl={}
            for table_name in table_names:
                  querysql_query = f'SELECT * FROM {table_name};'  
                  DataFramedf = pd.read_sql(text(querysql_query), conn[0])
                  print("This is a frame",DataFramedf)
                  mastertbl[table_name]=DataFramedf.head(3).to_json(orient='records')
                  print("This is a master",mastertbl[table_name])
            return  jsonify(mastertbl)
    except Exception as e:
        print(e)
        return 'error'

@app.route('/getmetadata' ,methods=['GET'])
def getmetadata():
    print('called getmetadata',40*'-')
    try:
        if (request.method == 'GET'):
            value = master_session[session['user']].get('metadata') #master_session[session['user']]['metadata']
            print('getdata',master_session)
            if value is not None:
                return master_session[session['user']]['metadata']
            else :
                return 'nothing'

    except Exception as e:
        print(e)
        return 'error'

@app.route('/sendtable' ,methods=['POST'])
def sendtable():
    print('called sendtable',40*'-')
    try :
        if request.method=='POST':
            tlist=request.form['tables']
            print(type(tlist))
            print(tlist)
        return 'hurrey'
    except Exception as e:
        print(e)
        return 'error'

# def exec_query(prompt,model,connection_string,query,conn,user_query):
#     print('called exec_query',40*'-')
#     # hashed_string=ret_hash(connection_string)
#     i=0
#     while i<=1:
#         print(i)
#         i+=1
#         result = model.generate_text(prompt)
#         print("exec",result)
#         result = parse_query(result,conn,user_query,parse_type="Query")

#         print(result)
#         if result != None:
#             return result
#     return None
    


def connectmysqldb(db_user,db_password,db_host,db_port,db_name):
    print('called connectmysqldb',40*'-')
    encoded_password = quote_plus(db_password)
    # connection_string = f"mysql+pymysql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"
    connection_string = f'mssql+pymssql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)
    conn = engine.connect()
    print("This is conn",conn)
    return conn,connection_string,engine


# def ret_hash(con_str):
#     print('called ret_hash',40*'-')
#     sha256 = hashlib.sha256()
#     sha256.update(con_str.encode('utf-8'))
#     hashed_string = sha256.hexdigest()
#     return hashed_string

# def get_cacheDB(hashed_string):
#     print('called get_cacheDB',40*'-')
#     path=os.path.join(params['Cache_DB_folder'],params['Cache_DB_filename'])
#     if os.path.exists(path):
#         with open(path, 'r') as file:
#             cache = json.load(file)
#     else:
#         cache = {}
#         with open(path, 'w') as file:
#             json.dump(cache, file, indent=4)

#     if hashed_string in cache.keys():
#         return cache[hashed_string]
#     else:
#         cache[hashed_string]={}
#         with open(path, 'w') as file:
#             json.dump(cache, file, indent=4)
#     return cache[hashed_string]


@app.route('/savequery',methods=['POST'])
def save_cache(): 
    print('called save_cache',40*'-')
    if request.method=='POST':
        connection_string=request.form['connection_string']
        query=request.form['query']
        llm_output=request.form['llm_output']
        hashed_string=ret_hash(connection_string)
        path=os.path.join(params['Cache_DB_folder'],params['Cache_DB_filename'])
        try:
            with open(path, 'r') as file:
                cache = json.load(file)
            cache[hashed_string][query]=llm_output
            with open(path, 'w') as file:
                json.dump(cache, file, indent=4)
            return 'success'
        except:
            print('No cache file found.')
            return 'error'


@app.route('/generatedescription' ,methods=['POST'])
def gendescription():
    print('called gendescription',40*'-')
    try:
        if (request.method == 'POST'):
            btn_action=request.form['generate_btype']
            dbdata=master_session[session['user']]['conn']
            hashed_conn_string = ret_hash(dbdata[1])
            db_desc_filepath = os.path.join(params['DB_Details'],hashed_conn_string, params['DB_DESC_FILENAME'])
            os.makedirs(os.path.dirname(db_desc_filepath), exist_ok=True)
            if os.path.exists(db_desc_filepath):
                if btn_action == "analyze":
                    with open(db_desc_filepath,'r') as f:
                        json_description = json.load(f)
                    return json_description
            if dbdata[0]!=None:
                sql_database = SQLDatabase(dbdata[2], sample_rows_in_table_info=2)
                desc = generate_description(model,sql_database)
                print('descript list')
                print(desc)
                json_description = process_desc(desc)
                with open(db_desc_filepath, 'w') as f:
                    json.dump(json_description,f, indent=3)    
                return json_description
            else:
                print('Connection Unsuccesfull')
                return 'error'
    except Exception as e :
        print(e)
        return 'error'
    

# def gendescription():
#     print('called gendescription', 40 * '-')
#     try:
#         if request.method == 'POST':
#             btn_action = request.form['generate_btype']
#             dbdata = master_session.get(session['user'], {}).get('conn', None)
            
#             if not dbdata:
#                 print("No database data found in session.")
#                 return 'error'

#             print(f"Database data: {dbdata}")

#             hashed_conn_string = ret_hash(dbdata[1])
#             db_desc_filepath = os.path.join(params['DB_Details'], hashed_conn_string, params['DB_DESC_FILENAME'])
#             os.makedirs(os.path.dirname(db_desc_filepath), exist_ok=True)

#             print(f"DB Description File Path: {db_desc_filepath}")

#             if os.path.exists(db_desc_filepath):
#                 if btn_action == "analyze":
#                     with open(db_desc_filepath, 'r') as f:
#                         json_description = json.load(f)
#                     print(f"Returning cached description from {db_desc_filepath}")
#                     return json_description

#             if dbdata[0] is not None:
#                 sql_database = SQLDatabase(dbdata[2], sample_rows_in_table_info=2)
#                 print(f"Connected to SQL Database: {dbdata[2]}")

#                 desc = generate_description(model, sql_database)
#                 print('Generated description list:', desc)

#                 if not desc:
#                     print('Description list is empty')
#                     return 'error'

#                 json_description = process_desc(desc)
#                 with open(db_desc_filepath, 'w') as f:
#                     json.dump(json_description, f, indent=3)

#                 return json_description
#             else:
#                 print('Connection Unsuccessful')
#                 return 'error'
#     except Exception as e:
#         print('Exception occurred:', e)
#         return 'error'



@app.route('/generateexample' ,methods=['POST'])
def generateexample():
    print('called generateexample',40*'-')
    try:
        if (request.method == 'POST'):
            btn_action=request.form['generate_btype']
            dbdata=master_session[session['user']]['conn']
            hashed_conn_string = ret_hash(dbdata[1])
            db_desc_filepath = os.path.join(os.path.join(params['DB_Details'],hashed_conn_string),params['DB_DESC_FILENAME'])
            db_ex_filepath = os.path.join(os.path.join(params['DB_Details'],hashed_conn_string),params['DB_EXAMPLE_FILENAME'])

            if os.path.exists(db_desc_filepath):
                if os.path.exists(db_ex_filepath):
                    if btn_action == "analyze":
                        with open(db_ex_filepath,"r") as f:
                            examples = json.load(f)
                        return examples
                
                if dbdata[0]!=None:
                    with open(db_desc_filepath,"r") as f:
                        db_description = json.load(f)
                    sql_database = SQLDatabase(dbdata[2], sample_rows_in_table_info=2)
                    print("this is sql",sql_database)
                    context = generate_example_base_context(description,template_info)
                    prompt = generate_example_final_prompt(sql_database,db_description,context,template_info)
                    examples = process_generate_examples(model,db_ex_filepath,prompt,dbdata[0])
                    with open(db_ex_filepath,'w') as f:
                        json.dump(examples,f,indent = 3)
                    return examples

            else:
                print("Generate DB Description File First.")
                return 'error' 
    except Exception as e :
        print(e)
        return 'error'

@app.route('/savedescription' ,methods=['POST'])
def savedescription():
    print('called savedescription',40*'-')
    try:
        if (request.method == 'POST'):
            desc=request.form['desc']
            print('desc',type(desc),desc)
            dbdata=master_session[session['user']]['conn']
            if dbdata[0]!=None:
                hashed_conn_string = ret_hash(dbdata[1])
                db_desc_filepath = os.path.join(os.path.join(params['DB_Details'],hashed_conn_string),params['DB_DESC_FILENAME'])
                if os.path.exists(db_desc_filepath):
                    with open(db_desc_filepath,'w') as f:
                        json.dump(json.loads(desc),f,indent = 3)
                    return 'success'
    except Exception as e:
        print(e)
        return 'error'

@app.route('/savedexample' ,methods=['POST'])
def savedexample():
    print('called savedexample',40*'-')
    try:
        if (request.method == 'POST'):
            exmpl=request.form['exmpl']
            dbdata=master_session[session['user']]['conn']
            if dbdata[0]!=None:
                hashed_conn_string = ret_hash(dbdata[1])
                db_ex_filepath = os.path.join(os.path.join(params['DB_Details'],hashed_conn_string),params['DB_EXAMPLE_FILENAME'])
                if os.path.exists(db_ex_filepath):
                    with open(db_ex_filepath,'w') as f:
                        json.dump(json.loads(exmpl),f,indent = 3)
                    return 'success'
    except Exception as e:
        print(e)
        return 'error'


@app.route('/downloadcsv',methods=['POST'])
def download_csv():
    print('called download_csv',40*'-')
    try:
        if request.method =='POST':
            sql_query = request.form['query']
            print(sql_query)
            print('dd ',master_session)
            dbdata=master_session[session['user']]['conn']
            print(type(dbdata),dbdata[0])
            engine = dbdata[2]
            data = pd.read_sql(text(sql_query), dbdata[0]) 
            print(data.to_json(orient='records')) 
            jdata=data.to_json(orient='records')
            return jsonify(jdata)
    except Exception as e:
        print(e)
        return 'error'



# def are_sentences_similar(sentence1, sentence2):
#     doc1 = nlp(sentence1)
#     doc2 = nlp(sentence2)
#     # Compute the similarity between the two sentences
#     similarity_score = doc1.similarity(doc2)
#     return similarity_score

# def generate_create_table_statement(table_schema):
#     print('called generate_create_table_statement',40*'-')
#     # Split the input string into table name, columns, and foreign keys
#     parts = table_schema.split(" has columns: ")
#     table_name = parts[0].strip()
    
#     columns_and_keys = parts[1].split(" and foreign keys: ")
#     columns_str = columns_and_keys[0].strip()
#     foreign_keys_str = columns_and_keys[1].strip("['']").split(" -> ")
    
#     # Split the columns string into a list of column definitions
#     column_definitions = columns_str.split(", ")
    
#     # Create a dictionary to store column names and data types
#     columns = {}
    
#     # Create a list to store foreign keys
#     foreign_keys = []

#     for column_definition in column_definitions:
#         column_parts = column_definition.split(" ")
#         column_name = column_parts[0]
#         data_type = " ".join(column_parts[1:])
#         columns[column_name] = data_type
    
#     # Extract primary key from columns
#     primary_key = columns_str.split(" ")[0]

#     # Extract foreign keys
#     if len(foreign_keys_str) > 1:
#         foreign_keys = [f.strip() for f in foreign_keys_str[1].split(",")]

#     # Create the initial part of the create table statement
#     create_table_query = f"CREATE {table_name} (\n"
    
#     # Add columns
#     for column_name, data_type in columns.items():
#         create_table_query += f"    {column_name} {data_type},\n"

#     # Add primary key constraint
#     create_table_query += f"    PRIMARY KEY ({primary_key}),\n"

#     # Add foreign key constraints
#     # for foreign_key in foreign_keys:
#     #     create_table_query += f"    FOREIGN KEY ({foreign_key}) REFERENCES {foreign_key.split('.')[0]} ({foreign_key.split('.')[1]}),\n"

#     for foreign_key in foreign_keys:
#         fk_parts = foreign_key.split('.')
#         try:
#             if len(fk_parts) == 2:
#                 referenced_table, referenced_column = fk_parts[0], fk_parts[1]
#                 create_table_query += f"FOREIGN KEY ({foreign_key}) REFERENCES {referenced_table} ({referenced_column}),\n"
#         except Exception as e:
#             pass
#         # else:
#         #     # Handle the case where the foreign key format is unexpected
#         #     print(f"Warning: Unexpected format for foreign key '{foreign_key}'. Skipping.")
    
#     # Remove the trailing comma and newline
#     create_table_query = create_table_query.rstrip(",\n") + "\n"

#     create_table_query += ");"
#     # print(type(create_table_query))
#     # print(create_table_query)
#     return create_table_query

# def descr(model,table_string,table):
#     print('called descr',40*'-')
#     prompt = f"""
# Given the table schema for "{table}". Generate a concise and precise description of columns and any constraints such as 'PRIMARY KEY' and 'FOREIGN KEY'.
# Strictly use the given data for the table "{table}". Do not use any other data or add information from other tables. Strictly Do not generate 'CREATE TABLE' querie
# Some examples are given below:

# INPUT: 
# CREATE TABLE 'orders' (
#     Order_ID (VARCHAR(10)),
#     ClientID (VARCHAR(7)),
#     OrderDate (DATE),
#     Actual_Delivery_Date (DATE),
#     Expected_Delivery_Date (DATE),
#     Product_ID (BIGINT),
#     Quantity (INTEGER),
#     OrderType (TEXT),
#     PRIMARY KEY (Order_ID),
#     FOREIGN KEY (ClientID) REFERENCES clients(ClientID),
#     FOREIGN KEY (Product_ID) REFERENCES products(Product_ID)
# );

# OUTPUT: 
# TABLE NAME: orders
# Order_ID : Unique ID assigned to each order from the clients, used to get unique orders
# OrderDate : Date on which the order was placed by the client.
# Actual_Delivery_Date : Actual Date on which the order was successfully delivered to the client.
# Expected_Delivery_Date : Date on which the order was delivered.
# ClientID : ID of client who ordered some product , taken from clients table as foreign key
# Product_ID : ID of the product ordered by the client, taken from products tables as foreign key
# Quantity : Quantity Sold for the product in this order
# OrderType : Denotes whether the order was a 'Bulk' order or a 'Sample' order

# <end_of_description>

# input: {table_string}
# output:
# """
#     print('start------+++++')
#     print(prompt)
#     print('end------+++++')
#     # print()
#     # print()
#     # print("++++++++++++++++++++++++++++++++++++")
#     result = model.generate_text(prompt)
#     # print(result)
#     return {table:result}

# def generate_description(model,sql_database):
#     # print('called generate_description',40*'-')
#     table_list = []
#     try:
#         # print(10*'1',sql_database._all_tables)
#         for table in tqdm(list(sql_database._all_tables)):
#             print()
#             # print(sql_database.get_single_table_info(table))
#             create_command = generate_create_table_statement(sql_database.get_single_table_info(table))
#             # print(10*'*','create_command')
#             print("This is create command",create_command)
#             table_string = "\n" + create_command + '\n'
#             desc = descr(model,table_string,table)
#             # print(10*'*','desc')
#             # print(desc)
#             table_list.append(desc)
#         return table_list
#     except Exception as e:
#         print(f"Error: {e}")
#         return None

# def parse_table_description(input_dict):
#     print('called parse_table_description',40*'-')
#     input_string = input_dict[list(input_dict.keys())[0]]

#     try:
#         if '<end_of_description>' in input_string:
#             # Define the regular expression pattern
#             pattern = r"TABLE NAME: (\w+)\n(.*?)\n<end_of_description>"
#         else:
#             # Define the regular expression pattern
#             pattern = r"TABLE NAME: (\w+)\n(.*?)\n"

#         # Extract table name and content using regular expressions
#         match = re.match(pattern, input_string, re.DOTALL)

#         if match:
#             table_name, content = match.groups()

#             # Split content into lines and create a dictionary
#             table_dict = {line.split(' : ')[0]: line.split(' : ')[1] for line in content.split('\n') if line}

#             # Create the final dictionary
#             output_dict = {table_name: table_dict}

#             return output_dict
#         else:
#             return {list(input_dict.keys())[0]:{}}
#     except:
#         return {list(input_dict.keys())[0]:{}}

# def parse_table_description(input_dict):
#     print('called parse_table_description', 40 * '-')
#     input_string = input_dict[list(input_dict.keys())[0]]
#     print(input_string)

#     try:
#         # Define the regular expression pattern to extract the table name and all subsequent content
#         pattern = r"TABLE NAME: (\w+)\n(.*)"

#         # Extract table name and content using regular expressions
#         match = re.search(pattern, input_string, re.DOTALL)
#         if match:
#             table_name, content = match.groups()

#             # Split content into lines and create a dictionary
#             # Each line is expected to be in the format "ColumnName : Description"
#             table_dict = {}
#             for line in content.split('\n'):
#                 if line:  # Check if the line is not empty
#                     parts = line.split(' : ')
#                     if len(parts) == 2:  # Ensure that the line can be split into exactly two parts
#                         key, value = parts
#                         table_dict[key.strip()] = value.strip()

#             # Create the final dictionary with the table name as the key and column descriptions as values
#             output_dict = {table_name: table_dict}
#             return output_dict
#         else:
#             return {list(input_dict.keys())[0]: {}}
#     except Exception as e:
#         print(f"Error: {e}")
#         return {list(input_dict.keys())[0]: {}}


# def process_desc(desc_ls):
#     print('called process_desc',40*'-')
#     json_result = {}
#     for des in desc_ls:
#         results = parse_table_description(des)
#         print(des,20*'+')
#         print(results)
#         json_result = {**json_result,**results}
#     return json_result    

# def generate_context(sql_database,description,template_info):
#     print('called generate_context',40*'-')
#     table_string = template_info['Instruction_NL2SQL'] + "\n\n"
#     table_string += "TABLE SCHEMAS:"
#     for table in list(sql_database._all_tables):
#         create_command = generate_create_table_statement(sql_database.get_single_table_info(table))
#         table_string = table_string + ' \n' + create_command + '\n'
    
#     table_string += "\n\nTABLE DESCRIPTIONS:\n"
#     for key,value in description.items():
#         table_string += "TABLE NAME: " + str(key) + "\n"
#         for col,desc in value.items():
#             table_string += str(col) + " : " + str(desc) + "\n"
#         table_string+="\n"
#     table_string+="\n"

#     table_string+="STEPS TO GENERATE THE SQL QUERY:\n"
#     for step in template_info["Pointers"]["Steps"]:
#         table_string+=step+"\n"
#     table_string+="\n"

#     table_string+="INSTRUCTIONS:\n"
#     for step in template_info["Pointers"]["Instruction"]:
#         table_string+=step+"\n"
#     table_string+="\n"

#     return table_string

# def generate_prompt(template_info,examples,context,query):
#     print('called generate_prompt',40*'-')
#     template = context + "\n\n"
#     for example in examples:
#         template += f"INPUT: {example['Question']}\n"
#         template += f"OUTPUT: {example['Answer']}\n"
#         template += f"Description: {example['Description']}\n"
#         template += f"Tables Used: {example['Tables_used']}\n"
#         template += f"{template_info['Stop_Sequence'][0]}\n"
#         template += "\n" 
#     template+= f"INPUT: {query}\n"
#     return template

# def generate_example_base_context(description,template_info):
#     print('called generate_example_base_context',40*'-')
#     table_string = template_info['Instruction_Example'] + "\n\n"
    
#     # Adding Instructions to the Prompt:
#     table_string += "STRICTLY FOLLOW THESE INSTRUCTION - \n"
#     for step in template_info["Pointers"]["Instruction"]:
#         table_string+=step+"\n"
#     table_string+="\n"
#     table_string+=template_info["Seperator"] + "\n\n"

#     # Adding Sample Schema to Prompt:
#     table_string += "DB SCHEMA - \n"
#     for step in template_info["Sample_Schema"]:
#         table_string+=step+"\n"

#     # Adding Description of Sample Table:
#     table_string += "\n\nTABLE DESCRIPTIONS:\n"
#     for key,value in description.items():
#         table_string += "TABLE NAME: " + str(key) + "\n"
#         for col,desc in value.items():
#             table_string += str(col) + " : " + str(desc) + "\n"
#         table_string+="\n"
#     table_string+="\n"
    
#     # Adding Sample Examples:
#     table_string+=str(len(template_info["Sample_Examples"])) + " DATA ANALYTIC QUESTIONS AND mssql QUERIES- \n"
#     for step in template_info["Sample_Examples"]:
#         table_string+="INPUT: " + step["Question"] + "\n"
#         table_string+="OUTPUT: " + step["Answer"] + "\n"
#         table_string+="Description: " + step["Description"] + "\n"
#         table_string+="Tables Used: " + ', '.join(step["Tables_used"]) + "\n\n"
#     table_string+=template_info["Stop_Sequence"][0] + "\n"
#     table_string+=template_info["Seperator"] + "\n\n"
    
#     return table_string

# def generate_example_final_prompt(sql_database,db_description,context,sample_template_info):
#     print('called generate_example_final_prompt',40*'-')
#     # Get Table Schema - 
#     table_string = context + "DB SCHEMA - \n"
#     for table in list(sql_database._all_tables):
#         create_command = generate_create_table_statement(sql_database.get_single_table_info(table))
#         table_string += create_command + '\n'
    
#     # Get Table Description - 
#     table_string += "\n\nTABLE DESCRIPTIONS:\n"
#     for key,value in db_description.items():
#         table_string += "TABLE NAME: " + str(key) + "\n"
#         for col,desc in value.items():
#             table_string += str(col) + " : " + str(desc) + "\n"
#         table_string+="\n"
#     table_string+="6 DATA ANALYTIC QUESTIONS AND mssql QUERIES - "
#     print("Table ST",table_string)
#     return table_string

# def test_sql_query(query, conn, parse_type):
#     print('called test_sql_query',40*'-')
#     if parse_type == "Example":
#         try:
#             result = conn.execute(text(query['Answer']))
#             print('Example',20*'=')
#             rows = result.fetchall()  # fetches all rows
#             row_count = len(rows)
#             if (row_count>0):    
#                 return True, row_count
#             else:
#                 return False, 0
#         except Exception as e:
#             return False,0
#     if parse_type == "Query":
#         try:
#             result = conn.execute(text(query['Answer'])) 
#             print('Example',20*'=')
#             rows = result.fetchall()  # fetches all rows
#             row_count = len(rows)
#             return True, 0
#         except Exception as e:
#             return False,0
        
# def parse_query(input_string,conn,user_query,parse_type):
#     print('called parse_query',40*'-')
#     if parse_type == "Example":
#         pattern = re.compile(r'INPUT:\s*(.+?)\nOUTPUT:\s*(.+?)\nDescription:\s*(.+?)\nTables Used:\s*(.+?)(?=\n\n|$)', re.DOTALL | re.IGNORECASE)
#         matches = pattern.findall(input_string)

#         result_list = []
#         for match in matches:
#             question, answer, description, tables_used = match
#             print(answer)
#             tables_used_list = [table.strip().replace("'","").replace("[","").replace("]","").replace("\n","").replace("<end_of_code>","") for table in tables_used.split(',')]
#             res = {
#                 'Question': question.strip(),
#                 'Answer': re.sub(' +', ' ', answer.strip().replace('\n',' ')),
#                 'Description': description.strip(),
#                 'Tables_used': tables_used_list
#             }
#             print('answer')
#             print(res)
#             print('end ans')
#             res_flag,row_count = test_sql_query(res,conn,parse_type)
#             print('test returned',res_flag)
#             if res_flag:
#                 result_list.append(res)

#         return result_list
#     elif parse_type == "Query":
#         print('query input ')
#         print(input_string)
#         print('input end')
#         pattern = re.compile(r'OUTPUT:\s*(.+?)\nDescription:\s*(.+?)\nTables Used:\s*(.+?)(?=\n\n|$)', re.DOTALL | re.IGNORECASE)
#         matches = pattern.findall(input_string)
#         result_list = []
#         for match in matches:
#             question = user_query 
#             answer, description, tables_used = match
#             tables_used_list = [table.strip().replace("'","").replace("[","").replace("]","").replace("\n","").replace("<end_of_code>","") for table in tables_used.split(',')]
            
#             try:
#                 dbdata = master_session[session['user']]['conn']
#                 ans = re.sub(' +', ' ', answer.strip().replace('\n',' '))
#                 df = pd.read_sql(text(ans), dbdata[0])
#                 prompt = f"""Write a Python function with logic to generate a graph Using pandas,numpy,matplotlib and seaborn library. Follow the Below given instructions. 
#                 "INSTRUCTIONS":
#                 1. Identify the column names from provided the sql query {answer}\
#                 2. The data related to the column names is present in {df}. Which is a dataframe.\
#                 3. Use the identified column names and data from {df} to generate a python code which creates a function 'analyze_graph'.
#                 4. The 'analyze_graph' function takes {df} as an argument which is a dataframe.
#                 "Input": {description + "and also plot the graph"}
                
#                 """
                
#             except Exception as e:
#                 pass
#             res = {
#                 'Question': question.strip(),
#                 'Answer': re.sub(' +', ' ', answer.strip().replace('\n',' ')),
#                 'Description': description.strip(),
#                 'Tables_used': tables_used_list,
#                 'Output': df.to_html()
#             }
#             res_flag,row_count = test_sql_query(res,conn,parse_type)
#             if res_flag:
#                 return res
#             else:
#                 return None
        

# def perform_llm_call(data, option, data_desc, query):
#     global model, instruction_sr, prompt_sr, instruction_tp, prompt_tp
#     if option == "Spot Rate":
#         new_prompt = generate_prompt(option, data_desc, query, instruction_sr, prompt_sr)
#     elif option == "Term Premia":
#         new_prompt = generate_prompt(option, data_desc, query, instruction_tp, prompt_tp)
    
#     print("Calling Model")
#     generated_response = model.generate_text(prompt=new_prompt, guardrails=False)
#     print("Calling Model Done")
#     generated_response = generated_response.replace('<end of code>','').strip()
#     standard_lib = """
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
# import scipy
# import sklearn
# import statsmodels.api as sm
# """
#     generated_response = f"{standard_lib}\n\n{generated_response}"
#     generated_response += "\n\nresult_msg, result_data, graph_img = analyze_df(data)"
#     compile={'data':data}
#     # print(generated_response)

#     try:
#         compile_exec = compile.copy()  # Create a copy to avoid modifying the original dict
#         exec(generated_response, compile_exec)
#         result_msg, result_data, graph_img = compile_exec['result_msg'], compile_exec['result_data'], compile_exec['graph_img']
#         return result_msg, result_data, graph_img, generated_response
#     except Exception as e:
#         # print(e)
#         msg = f"I'm sorry, I encountered an issue while trying to process your question. Could you please try rephrasing it or provide more context?"
#         result_msg, result_data, graph_img = msg, None, None
#         return result_msg, result_data, graph_img, generated_response



# def parse_query(input_string, conn, user_query, parse_type):
#     if parse_type == "Example":
#         pattern = re.compile(r'SELECT\s+(.+?)\s+FROM\s+(.+?)\s+JOIN\s+(.+?)\s+ON\s+(.+?)\s+JOIN\s+(.+?)\s+ON\s+(.+?)\s+GROUP BY\s+(.+?);[\n\r]+\s*Description:\s*(.+?)[\n\r]+\s*Tables Used:\s*\[(.*?)\]', re.DOTALL | re.IGNORECASE)
#         matches = pattern.findall(input_string)

#         result_list = []
#         for match in matches:
#             select_clause, from_clause, join1_table, join1_condition, join2_table, join2_condition, group_by_clause, description, tables_used = match
#             tables_used_list = [table.strip().replace("'", "").replace("[", "").replace("]", "").replace("\n", "").replace("<end_of_code>", "") for table in tables_used.split(',')]
#             res = {
#                 'Question': user_query.strip(),
#                 'Answer': re.sub(' +', ' ', select_clause.strip().replace('\n', ' ')),
#                 'Description': description.strip(),
#                 'Tables_used': tables_used_list
#             }
#             res_flag, row_count = test_sql_query(res, conn, parse_type)
#             if res_flag:
#                 result_list.append(res)

#         return result_list

#     elif parse_type == "Query":
#         pattern = re.compile(r'SELECT\s+(.+?)\s+FROM\s+(.+?)\s+JOIN\s+(.+?)\s+ON\s+(.+?)\s+JOIN\s+(.+?)\s+ON\s+(.+?)\s+GROUP BY\s+(.+?);[\n\r]+\s*Description:\s*(.+?)[\n\r]+\s*Tables Used:\s*\[(.*?)\]', re.DOTALL | re.IGNORECASE)
#         matches = pattern.findall(input_string)
#         result_list = []

#         for match in matches:
#             select_clause, from_clause, join1_table, join1_condition, join2_table, join2_condition, group_by_clause, description, tables_used = match
#             tables_used_list = [table.strip().replace("'", "").replace("[", "").replace("]", "").replace("\n", "").replace("<end_of_code>", "") for table in tables_used.split(',')]
#             res = {
#                 'Question': user_query.strip(),
#                 'Answer': re.sub(' +', ' ', select_clause.strip().replace('\n', ' ')),
#                 'Description': description.strip(),
#                 'Tables_used': tables_used_list
#             }
#             res_flag, row_count = test_sql_query(res, conn, parse_type)
#             if res_flag:
#                 return res
#             else:
#                 return None



# def process_generate_examples(model,db_ex_filepath,prompt,conn):
#     print('called process_generate_examples',40*'-')
#     examples = []
#     while True:
#         result = model.generate_text(prompt)
#         temp_examples = parse_query(result,conn,"",parse_type="Example")
#         examples.extend(temp_examples)
#         if len(examples)>=5 :
#             examples = examples[:5]
#             break
#     return examples



if __name__ == '__main__':
    app.run(debug=True)
