from sqlalchemy import create_engine,DateTime,Float,String
import requests
from datetime import datetime, timedelta
import smtplib
import pandas as pd 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator


# Variables
user = Variable.get("USER")
password = Variable.get("PASSWORD")
endpoint = Variable.get("ENDPOINT")
port = Variable.get("PORT")
database = Variable.get("DATABASE")
api_key = Variable.get("API_KEY")
connection_string = f"postgresql://{user}:{password}@{endpoint}:{port}/{database}"
start_time = datetime.now()
email_from = Variable.get("SMTP_EMAIL_FROM")
email_pass = Variable.get("SMTP_PASSWORD")
email_to = Variable.get("SMTP_EMAIL_TO")
engine = create_engine(connection_string)

# Funciones
def check_and_send_email(subject):
    current_date = datetime.now().date()
    target_date = datetime(2023, 9, 1).date()

    if current_date == target_date:
        enviar(subject)
def enviar(subject):
    try:
        # Creacion SMTP
        x = smtplib.SMTP("smtp.gmail.com", 587)
        x.starttls()
        x.login(email_from, email_pass)
        subject = f"{subject}"

        # Variable calculo de tiempo de ejecución
        execution_time = datetime.now() - start_time
        execution_time_seconds = execution_time.total_seconds()
        hours, remainder = divmod(execution_time_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Textos del mail
        execution_time_str = f"{int(hours)} horas, {int(minutes)} minutos, {int(seconds)} segundos"
        body_text = f"{subject}  \n Execution Time:  {start_time} for {execution_time_str}"
        message = f"Subject: {subject}\n\n{body_text}"
        x.sendmail(email_from, email_to, message)
        x.quit()

    except Exception as exception:
        print(exception)
        

def extract_data_to_db():
    try:
        # Coordenadas Ciudades
        latylon_ciudades = {"Madrid":[40.4165000, -3.7025600], "Barcelona":[41.3887900, 2.1589900],
                        "Valencia":[39.4697500, -0.3773900], "Sevilla":[37.3828300, -5.9731700],
                        "Zaragoza":[41.6560600, -0.8773400], "Málaga":[36.7201600, -4.4203400]}
        ciudades_data = []

        # bucle por ciudad,coordenadas:
        for i in range(len(list(latylon_ciudades.keys()))):
            # Consulta
            base_url = f"https://api.tomorrow.io/v4/timelines?location={latylon_ciudades[list(latylon_ciudades.keys())[i]][0]},{latylon_ciudades[list(latylon_ciudades.keys())[i]][1]}&fields=temperature&timesteps=1h&units=metric&apikey={api_key}"
            resp = requests.get(base_url)
            data = resp.json()

            # Creación dataframe
            df2 = pd.DataFrame(data["data"]["timelines"][0]["intervals"][:25])
            
            # Transformación variables
            df2["temp"] = df2["values"].apply(lambda x: x["temperature"])
            df2["ciudad"] = list(latylon_ciudades.keys())[i] 
            df2["date_extract"] = start_time
            df2["temperature_scale"] = "Cº"
            df2 = df2.drop("values", axis=1)
            df2 = df2.rename(columns = {"startTime" : "fecha"})
            df2["fecha"] = pd.to_datetime(df2["fecha"]).dt.strftime("%Y-%m-%d %H:%M:%S")
            ciudades_data.append(df2)
        
        # Creación final de dataframe y envio a DB
        print("correct data extraction")
        df = pd.concat(ciudades_data)
        dtype = {"Fecha" : DateTime(), "Temp" : Float(), "Ciudad" : String(), "date_extract" : DateTime(), "temperature_scale" : String()}
        df.to_sql("weather", con = engine, if_exists = "append", index = False, dtype = dtype)

    except Exception as exception:
        # Tratamiento de Exceciones
        error = str(exception)
        if error == "'data'":
            error = "Error while APIs was downloading"
        else:
            pass

        # Envio correo con tipo de error
        enviar(f"The program execution was incorrect while the 'extract' task was working, the error was:   {error}")

        # Generación error
        raise exception

def extract_manipulate_insert_data(table):
    try:
        # Consulta Base de datos
        query = f"SELECT * FROM {table}"

        #Creación dataframe y manipulación de variables
        data = pd.read_sql(query, engine)
        data["Date"] = pd.to_datetime(data["fecha"]).dt.date
        data["Month"] = pd.to_datetime(data["fecha"]).dt.month
        data["Day"] = pd.to_datetime(data["fecha"]).dt.day
        data["year"] = pd.to_datetime(data["fecha"]).dt.year
        fecha_ciudad = data.groupby(["Date", "ciudad"])["temp"].mean().reset_index()
        fecha_ciudad = fecha_ciudad.round(2)
        for i in fecha_ciudad["ciudad"].unique():
            nombre_tabla = f"datos_climaticos_{i}"
            filtrado = fecha_ciudad[fecha_ciudad["ciudad"]==i].reset_index(drop=True)
            
            # Creación tabla si no existe
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {nombre_tabla}(
                    Date  DATE,
                    ciudad VARCHAR(100),
                    temp FLOAT
                    )
                    DISTKEY (ciudad)
                    SORTKEY (date)  ;"""
            engine.execute(create_table_query)
            truncate = f"TRUNCATE TABLE {nombre_tabla}"   
            engine.execute(truncate)

            # Insertar datos
            insert_query = f""" INSERT INTO {nombre_tabla} (Date, ciudad, temp)
                                VALUES (%s, %s, %s)"""
                
            values = [(row["Date"], row["ciudad"], row["temp"]) for _, row in filtrado.iterrows()]
            engine.execute(insert_query, values)
    except Exception as exception:
        # Tratamiento de Exceciones
        error = str(exception)

        # Envio correo con tipo de error
        enviar(f"The program execution was incorrect while the 'read_transform_insert' task was working, the error was {error}")
        
        # Generación error
        raise exception

# Definición de argumentos
default_args = {
    "owner" : "Jorge",
    "retries" : 1,
    "retry_delay" : timedelta(minutes = 1),
    "email_on_failure" : True,
    "sla_miss_callback" : enviar,
    "sla_miss_callback_kwargs": {"subject": "SLA Fail"},
}

# Creación DAG
with DAG(
    default_args = default_args,
    dag_id = "Dag_cuarta_entrega",
    description = "Dag de la cuarta entrega de coderhouse",
    start_date = datetime(2023,1,1),
    schedule_interval = "0 0 * * *",
    catchup = False
) as dag:
    
    # Creación Tareas
    extract = PythonOperator(
        task_id = "extract",
        python_callable = extract_data_to_db,
        sla = timedelta(seconds = 360),
    )

    read_transform_insert = PythonOperator(
        task_id = "read_transform_insert",
        python_callable = extract_manipulate_insert_data,
        op_kwargs = {"table": "weather"},
    )
    time_alert = PythonOperator(
        task_id = 'time_alert',
        python_callable = check_and_send_email,
        op_kwargs = {"subject": "Temporary alert you must check the code every month,remember to change the date"}
    )

    envio_task = PythonOperator(
        task_id = "envio_task",
        python_callable = enviar,
        trigger_rule = "all_success",
        op_kwargs = {"subject": "The program execution was successful"}
    )

    envio_task_fail = PythonOperator(
        task_id = "envio_task_fail",
        python_callable = enviar,
        trigger_rule = "all_failed",
        op_kwargs = {"subject": "The program execution was incorrect"} 
    )
    
    # Orden ejecución tareas
    extract  >> read_transform_insert  >> time_alert >> [envio_task, envio_task_fail]

