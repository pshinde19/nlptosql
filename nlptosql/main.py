
from flask import Flask, Response, jsonify, make_response, redirect,render_template, request, session
import json
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy import create_engine,inspect,text
from urllib.parse import quote_plus
import json
import pandas as pd
# from langchain.chat_models import ChatOpenAI
from langchain_community.chat_models import ChatOpenAI
import os
import plotly.io as pio
import plotly.graph_objects as go


master_session={}
app = Flask(__name__)
app.secret_key = '123456'



os.environ["OPENAI_API_KEY"] = ""
llm_model = "gpt-3.5-turbo"
llm = ChatOpenAI(temperature=0.1, model=llm_model)

with open("table_structure.txt", 'r') as file:
    table_struct = file.read()

with open("example.txt", 'r') as file:
    example_st = file.read()

with open('sample_qr.txt','r') as f:
    sample = f.read()

with open("prompt.txt", 'r') as file:
    instructions = file.read()

with open("table_descriptions.txt", 'r') as file:
    desc = file.read()

with open("example_queries.txt", 'r') as file:
    Q_example = file.read()



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
        master_session[session['user']]={}
        print(master_session)
        return 'success'
    except Exception as e :
        print(e)
        return 'error'


@app.route('/main')
def main1():
    print('vvvv',session)
    return render_template('index.html')


@app.route('/getquery',methods=['POST'])
def getquery():
    print('called getquery',40*'-')
    try:
        if request.method=='POST':
            query=request.form['qry']
            print(query)
            html_table,graph_html,sqlquery= main(query)
            return jsonify({"table":html_table,"msg":"success","graph":graph_html,"query":sqlquery})
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
            conn, connection_string, engine,mastertbl=connectmysqldb(db_user,db_password,db_host,db_port,db_name)
            print("12345",session)
            # master_session[session['user']]['conn']=conn
            master_session[session['user']]['metadata']={ 
                                                        "db_host":request.form['hostname'], 
                                                        "db_user":request.form['user'],
                                                        "db_password":request.form['password'],
                                                        "db_port":request.form['portno'],
                                                        "db_name":request.form['database'],
                                                        'schema':'{}'
                                                    }
            print(master_session)
            return  jsonify({"msg":"success","schema":mastertbl})
    except Exception as e:
        print(e)
        return 'error'

@app.route('/getmetadata' ,methods=['GET'])
def getmetadata():
    try:
        if (request.method == 'GET'):
            value = master_session[session['user']].get('metadata') #master_session[session['user']]['metadata']
            print('getmetadata')
            print('getdata',master_session)
            if value is not None:
                db_user=master_session[session['user']]['metadata']['db_user']
                db_password=master_session[session['user']]['metadata']['db_password']
                db_host=master_session[session['user']]['metadata']['db_host']
                db_port=master_session[session['user']]['metadata']['db_port']
                db_name=master_session[session['user']]['metadata']['db_name']
                conn, connection_string, engine,schema=connectmysqldb(db_user,db_password,db_host,db_port,db_name)
                # master_session[session['user']]['conn']=conn
                return jsonify({"metadata":master_session[session['user']]['metadata'],"schema":schema})
            else :
                return 'nothing'
    except Exception as e:
        print(e)
        return 'nothing'



def get_databases(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sys.databases"))
        return [row[0] for row in result]
    
def connectmysqldb(db_user, db_password, db_host, db_port, db_name):
    database_structure = {}
    print('called connectmysqldb', 40 * '-')
    encoded_password = quote_plus(db_password)
    print('con url')
    connection_string = f'mssql+pymssql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
    print(connection_string)
    engine = create_engine(connection_string)
    conn = engine.connect()
    print("This is conn", conn)
    database_structure[db_name] = {}
    print(f"Database: {db_name}")
    inspector_db = inspect(engine)
    # Get list of schemas
    schemas = get_schemas(inspector_db)
    for schema in schemas:
            database_structure[db_name][schema] = {'tables': {}, 'views': {}}
            print(f"  Schema: {schema}")

            # Get tables and views for each schema
            tables, views = get_tables_and_views(inspector_db, schema)
            print(f"    Tables in {schema}:")
            for table in tables:
                print(f"      {table}")
                # Get columns for each table
                columns = get_columns(inspector_db,schema,table)
                if columns:
                    database_structure[db_name][schema]['tables'][table] = {column['name']: str(column['type']) for column in columns}

            print(f"    Views in {schema}:")
            for view in views:
                print(f"      {view}")
                # Get columns for each view
                columns = get_columns(inspector_db, schema, view)
                if columns:
                    database_structure[db_name][schema]['views'][view] = {column['name']: str(column['type']) for column in columns}
    # engine.dispose()
    return conn, connection_string, engine ,database_structure

def get_schemas(inspector):
    return inspector.get_schema_names()
    

 
def get_tables_and_views(inspector, schema):
    tables = inspector.get_table_names(schema=schema)
    views = inspector.get_view_names(schema=schema)
    return tables, views
 
 
def get_columns(inspector, schema, table_name):
    try:
        return inspector.get_columns(table_name, schema=schema)
    except NoSuchTableError:
        return []
    
def alldata(db_user, db_password, db_host, db_port, db_name,engine):
    database_structure = {}
    if db_name:
        
        # Switch to the database
        # connection_string_db = f'mssql+pymssql://{username}:{encoded_password}@{server_name}:{port}/{db_name}'
        # encoded_password = quote_plus(db_password)
        # connection_string_db = f'mssql+pymssql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
        database_structure[db_name] = {}
        print(f"Database: {db_name}")
        # engine_db = create_engine(connection_string)
        inspector_db = inspect(engine)
        # Get list of schemas
        schemas = get_schemas(inspector_db)
        for schema in schemas:
            database_structure[db_name][schema] = {'tables': {}, 'views': {}}
            print(f"  Schema: {schema}")

            # Get tables and views for each schema
            tables, views = get_tables_and_views(inspector_db, schema)
            print(f"    Tables in {schema}:")
            for table in tables:
                print(f"      {table}")
                # Get columns for each table
                columns = get_columns(inspector_db,schema,table)
                if columns:
                    database_structure[db_name][schema]['tables'][table] = {column['name']: str(column['type']) for column in columns}

            print(f"    Views in {schema}:")
            for view in views:
                print(f"      {view}")
                # Get columns for each view
                columns = get_columns(inspector_db, schema, view)
                if columns:
                    database_structure[db_name][schema]['views'][view] = {column['name']: str(column['type']) for column in columns}

        # Close the engine for the current database
        engine_db.dispose()

    # Close the main engine
    engine.dispose()

    return database_structure


def get_table_names(data):
    table_names = []
    for db_name, db_content in data.items():
        for schema_name, schema_content in db_content.items():
            for table_or_view, tables_and_views in schema_content.items():
                if table_or_view == 'tables':
                    for table_name in tables_and_views:
                        table_names.append(table_name)
    return table_names

def primary(conn, table_name):
    primary_key_query = text("""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
    AND TABLE_NAME = :table_name;
    """)
    primary1 = pd.read_sql(primary_key_query, conn, params={"table_name": table_name})
    return primary1['COLUMN_NAME'].tolist()

def foreign(conn, table_name):
    foreign_key_query = text("""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsForeignKey') = 1
    AND TABLE_NAME = :table_name;
    """)
    foreign1 = pd.read_sql(foreign_key_query, conn, params={"table_name": table_name})
    return foreign1['COLUMN_NAME'].tolist()

def generate_table_descriptions(conn, structure, table_names):
    descriptions = {}

    for database, schemas in structure.items():
        for schema, content in schemas.items():
            for table_name, columns in content["tables"].items():
                # Check if the table exists in the database
                if table_name not in table_names:
                    print(f"Table {table_name} does not exist in the database. Skipping.")
                    continue

                # Get primary keys for the table
                primary_keys = primary(conn, table_name)

                # Get Foreign keys for the table
                foreign_keys = foreign(conn, table_name)

                # Generate table description
                table_description = {}
                for column, dtype in columns.items():
                    description = f"{column.replace('_', ' ').capitalize()} of type {dtype}"
                    if column in primary_keys:
                        description += " (Primary Key)"
                    table_description[column] = description
                    if column in foreign_keys:
                        description += " (Foreign Key)"
                    table_description[column] = description
                descriptions[table_name] = table_description

    return descriptions


def test_query(conn, query):
    try:
        result = conn.execute(text(query))
        result.fetchall()
        return True
    except Exception as e:
        print(f"Query failed: {query}\nError: {e}")
        return False
    
def extract_tables(structure):
    # Function to extract table columns dynamically
    for database, schemas in structure.items():
        for schema, content in schemas.items():
            return content["tables"]




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
            structure=request.form['schema']
            print(type(structure))
            print(structure)
            conn=get_connection()
            json_dict =json.loads(structure)
            print(type(json_dict))
            print(json_dict)
            table_names =get_table_names(json_dict)
            
            descriptions =generate_table_descriptions(conn, json_dict, table_names)
            print("Structure of tables:")
            print(json.dumps(descriptions, indent=2))
            with open("table_structure.txt", "w") as file:
                file.write(json.dumps(descriptions, indent=2))
            master_session[session['user']]['metadata']['schema']=structure    
            return 'success'
    except Exception as e :
        print(e)
        return 'error'


def get_connection():
    db_user=master_session[session['user']]['metadata']['db_user']
    db_password=master_session[session['user']]['metadata']['db_password']
    db_host=master_session[session['user']]['metadata']['db_host']
    db_port=master_session[session['user']]['metadata']['db_port']
    db_name=master_session[session['user']]['metadata']['db_name']
    encoded_password = quote_plus(db_password)
    connection_string = f'mssql+pymssql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}'
    print(connection_string)
    engine = create_engine(connection_string)
    conn = engine.connect()
    return conn

def main(nlquestion):
    conn=get_connection()
    descript = f"""You have been provided with table structure. Generate the table description for the provided table structure.\
Example:
{example_st}

Input:
{table_struct}

Output:

"""
    print(20*'+')
    print(descript)
    description_table = llm.invoke(descript)
    try:
        description_dict = json.loads(description_table.content)
        with open("table_descriptions.txt", "w") as file:
            file.write(json.dumps(description_dict, indent=2))
        print("JSON successfully written to table_descriptions.txt")
    except json.JSONDecodeError as e:
        print("JSONDecodeError:", e)
        # Handle the case where the content is not valid JSON
        with open("table_descriptions.txt", "w") as file:
            file.write(description_table.content)
        print("Raw output written to table_descriptions.txt for further analysis")

    sample_query_prompt = f"""Consider you are a expert in mssql and you can give correct SQL to fetch data everytime.\
Database Schema Description is given below as "INPUT" and Provide the "OUTPUT"\
{sample}\

INPUT: 
{desc}

OUTPUT:

"""
    
    Sql_demo_query = llm.invoke(sample_query_prompt)
    sample_queries = Sql_demo_query.content
    with open("example_queries.txt","w") as f:
        f.write(sample_queries)

    # nlquestion = input("Enter your question?: ")

    prompt_sql = f"""Consider you are a expert in mssql and you can give correct SQL to fetch data everytime. 
    Now, using the Table description and Instructions given below, convert the user's natural language query to mssql Query -

    TABLE DESCRIPTION:
    {desc}

    INSTRUCTIONS:
    {instructions}

    EXAMPLE:
    {Q_example}

    INPUT:
    {nlquestion}
    ANSWER:


"""
    Sql_ans = llm.invoke(prompt_sql)
    ans = Sql_ans.content.replace("\n"," ").replace("Answer","").replace(":"," ").replace('"','')
    print(ans)
    df = pd.read_sql(text(ans), conn)
    print(df)
    with open("graph_prompt.txt", 'r') as file:
        g_prompt = file.read()

    graph_prompt = f"""{g_prompt}\

    INPUT:
    {df}

    OUTPUT:

    """

    Sql_graph = llm.invoke(graph_prompt)
    img_code = Sql_graph.content.replace("python","").replace("`","")
    graph = {}
    print(img_code)
    exec(img_code, graph)
    g1 = graph
    print(g1)
    print(g1)
    conn.close()
    newdf= df.to_html(index=False)
    new_graph = pio.to_html(g1['graph_object'], full_html=False,include_plotlyjs=False,config={'responsive': True},default_height="338px",default_width="100%",div_id="mygraph")
    return(newdf,new_graph,ans)






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









if __name__ == '__main__':
    app.run(debug=False)
