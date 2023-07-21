import pandas as pd 
import requests
from sqlalchemy import create_engine, DateTime,Float,String
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable



USER = Variable.get("USER")
PASSWORD = Variable.get("PASSWORD")
ENDPOINT = Variable.get("ENDPOINT")
PORT = Variable.get("PORT")
DATABASE = Variable.get("DATABASE")
API_KEY = Variable.get("API_KEY")
connection_string = f"postgresql://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"

def extract_data_to_db():
    latylon_ciudades={"Madrid":[40.4165000,-3.7025600],"Barcelona":[41.3887900,2.1589900],
                      "Valencia":[39.4697500,-0.3773900],"Sevilla":[37.3828300,-5.9731700],
                      "Zaragoza":[41.6560600,-0.8773400],"Málaga":[36.7201600,-4.4203400]}
    ciudades_data=[]
    for i in range(len(list(latylon_ciudades.keys()))):
        BASE_URL= f"https://api.tomorrow.io/v4/timelines?location={latylon_ciudades[list(latylon_ciudades.keys())[i]][0]},{latylon_ciudades[list(latylon_ciudades.keys())[i]][1]}&fields=temperature&timesteps=1h&units=metric&apikey={API_KEY}"
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
    print("correct data extraction")
    df = pd.concat(ciudades_data)
    engine = create_engine(connection_string)
    dtype = {'Fecha': DateTime(), 'Temp': Float(), 'Ciudad': String()}
    df.to_sql('weather', con=engine, if_exists='append',index=False,dtype=dtype)

    
def extract_manipulate_insert_data(table):
    engine = create_engine(connection_string)
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(query,engine)
    data["Date"] = pd.to_datetime(data["fecha"]).dt.date
    data["Month"] = pd.to_datetime(data["fecha"]).dt.month
    data["Day"] = pd.to_datetime(data["fecha"]).dt.day
    data["year"] = pd.to_datetime(data["fecha"]).dt.year
    fecha_ciudad = data.groupby(["Date","ciudad"])["temp"].mean().reset_index()
    fecha_ciudad = fecha_ciudad.round(2)
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
        python_callable=extract_data_to_db,

    )

    read_transform_insert = PythonOperator(
        task_id = "read_transform_insert",
        python_callable=extract_manipulate_insert_data,
        op_kwargs={'table': 'weather'},
    )

    extract  >> read_transform_insert 
