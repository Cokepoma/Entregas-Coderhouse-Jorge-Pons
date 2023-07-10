import pandas as pd 
import requests
from sqlalchemy import create_engine, DateTime,Float,String
# from claves import USUARIO, CONTRASEÑA,ENDPOINT,PUERTO,BASE_DE_DATOS,API_KEY
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator


"""USUARIO = 'jorgepons90_coderhouse'
CONTRASEÑA = 'aJSb9Nf6G6'
ENDPOINT = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
PUERTO = 5439
BASE_DE_DATOS = 'data-engineer-database'
connection_string = "postgresql://<usuario>:<contraseña>@<endpoint>:<puerto>/<nombre_base_de_datos>"

API_KEY='ldck7dJTzob1DJaYv28PAM45nyFYojrB'"""

USUARIO = Variable.get("USUARIO")
CONTRASEÑA = Variable.get("CONTRASEÑA")
ENDPOINT = Variable.get("ENDPOINT")
PUERTO = Variable.get("PUERTO")
BASE_DE_DATOS = Variable.get("BASE_DE_DATOS")
API_KEY = Variable.get("API_KEY")


def extract_data():
    latylon_ciudades={"Madrid":[40.4165000,-3.7025600],"Barcelona":[41.3887900,2.1589900],
                      "Valencia":[39.4697500,-0.3773900],"Sevilla":[37.3828300,-5.9731700],
                      "Zaragoza":[41.6560600,-0.8773400],"Málaga":[36.7201600,-4.4203400]}
    ciudades_data=[]
    for i in range(len(list(latylon_ciudades.keys()))):
        BASE_URL= f"https://api.tomorrow.io/v4/timelines?location={latylon_ciudades[list(latylon_ciudades.keys())[i]][0]},{latylon_ciudades[list(latylon_ciudades.keys())[i]][1]}&fields=temperature&timesteps=1h&units=metric&apikey={API_KEY}"
        print(BASE_URL)
        resp = requests.get(BASE_URL)
        data = resp.json()
        print(data)
        data_prov = pd.DataFrame(data["data"]["timelines"][0]["intervals"][:25])
        df2 = pd.DataFrame(data_prov)
        df2['temp'] = df2['values'].apply(lambda x: x['temperature'])
        df2['ciudad'] = list(latylon_ciudades.keys())[i] 
        df2 = df2.drop("values",axis=1)
        df2 = df2.rename(columns = {"startTime" : "fecha"})
        df2['fecha'] = pd.to_datetime(df2['fecha']).dt.strftime('%Y-%m-%d %H:%M:%S')
        ciudades_data.append(df2)
    df = pd.concat(ciudades_data)
    return df

def data_to_db(df):
    # df = extract_data()
    connection_string = f"postgresql://{USUARIO}:{CONTRASEÑA}@{ENDPOINT}:{PUERTO}/{BASE_DE_DATOS}"
    engine = create_engine(connection_string)
    dtype = {'Fecha': DateTime(), 'Temp': Float(), 'Ciudad': String()}
    df.to_sql('weather', con=engine, if_exists='append',index=False,dtype=dtype)
    


def read_from_db(table):
    connection_string = f"postgresql://{USUARIO}:{CONTRASEÑA}@{ENDPOINT}:{PUERTO}/{BASE_DE_DATOS}"
    engine = create_engine(connection_string)
    query = f'SELECT * FROM {table}'
    lectura = pd.read_sql(query,engine)
    return lectura 

def transform_data(data):
    data["Date"] = pd.to_datetime(data["fecha"]).dt.date
    data["Month"] = pd.to_datetime(data["fecha"]).dt.month
    data["Day"] = pd.to_datetime(data["fecha"]).dt.day
    data["year"] = pd.to_datetime(data["fecha"]).dt.year
    fecha_ciudad = data.groupby(["Date","ciudad"])["temp"].mean().reset_index()
    fecha_ciudad = fecha_ciudad.round(2)
    return fecha_ciudad

def load_by_city(fecha_ciudad):
    connection_string = f"postgresql://{USUARIO}:{CONTRASEÑA}@{ENDPOINT}:{PUERTO}/{BASE_DE_DATOS}"
    engine = create_engine(connection_string)
    for i in fecha_ciudad["ciudad"].unique():
        nombre_tabla = f'datos_climaticos_{i}'
        filtrado = fecha_ciudad[fecha_ciudad["ciudad"]==i].reset_index(drop=True)
        
        create_table_query = f'''CREATE TABLE IF NOT EXISTS {nombre_tabla}(
                Date  DATE,
                ciudad VARCHAR(100),
                temp FLOAT
                )
                DISTKEY (ciudad)
                SORTKEY (date)  ;'''
        engine.execute(create_table_query)
        
        truncate = f"TRUNCATE TABLE {nombre_tabla}"    
        engine.execute(truncate)
        
        insert_query = f'''INSERT INTO {nombre_tabla} (Date, ciudad, temp)
                              VALUES (%s, %s, %s)'''
            
        values = [(row['Date'], row['ciudad'], row['temp']) for _, row in filtrado.iterrows()]
        engine.execute(insert_query, values)
    

default_args ={
    'owner' : 'Jorge',
    'retries': 1,
    'retry_delay' : timedelta(minutes=1)
}

with DAG(
    default_args = default_args,
    dag_id = "Dag_tercera_entrega",
    description = "Dag de la tercera entrega de coderhouse",
    start_date = datetime(2023,1,1),
    schedule_interval = "0 0 * * *",
    catchup=False
) as dag:
    
    extract = PythonOperator(
        task_id = "extract",
        python_callable=extract_data,

    )

    data_db = PythonOperator(
        task_id = "data_db",
        python_callable=data_to_db,
        op_kwargs={"df": extract.output},

    )

    read_db = PythonOperator(
        task_id = "read_db",
        python_callable=read_from_db,
        op_kwargs={'table': 'weather'},
        
    )
    transform = PythonOperator(
        task_id = "transform",
        python_callable=transform_data,
        op_kwargs={"data": read_db.output},
        
    )

    load_city = PythonOperator(
        task_id = "load_city",
        python_callable=load_by_city,
        op_kwargs={"fecha_ciudad": transform.output},
        
    )


    extract >> data_db >> read_db >> transform >>  load_city  
