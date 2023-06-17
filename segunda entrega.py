import pandas as pd 
import requests
from sqlalchemy import create_engine, DateTime,Float,String
from claves import USUARIO, CONTRASEÑA,ENDPOINT,PUERTO,BASE_DE_DATOS,API_KEY

def extract_data():
    fecha=[]
    grados=[]
    ciudad=[]
    latylon_ciudades={"Madrid":[40.4165000,-3.7025600],"Barcelona":[41.3887900,2.1589900],
                      "Valencia":[39.4697500,-0.3773900],"Sevilla":[37.3828300,-5.9731700],
                      "Zaragoza":[41.6560600,-0.8773400],"Málaga":[36.7201600,-4.4203400]}
    for i in range(len(list(latylon_ciudades.keys()))):
        BASE_URL= f"https://api.tomorrow.io/v4/timelines?location={latylon_ciudades[list(latylon_ciudades.keys())[i]][0]},{latylon_ciudades[list(latylon_ciudades.keys())[i]][1]}&fields=temperature&timesteps=1h&units=metric&apikey={API_KEY}"
        resp = requests.get(BASE_URL)
        data = resp.json()
        for s,a in data.items():
            for cu in range(24):
                try:
                    fecha.append(a["timelines"][0]["intervals"][cu]["startTime"])
                    grados.append(a["timelines"][0]["intervals"][cu]["values"]["temperature"])
                    ciudad.append(list(latylon_ciudades.keys())[i])
                except:
                    pass
        df=pd.DataFrame([fecha,grados,ciudad]).T
        df=df.rename(columns={0:"Fecha",1:"Temp",2:"Ciudad"})
        df['Fecha'] = pd.to_datetime(df['Fecha']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
    return df

def data_to_db(df):
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


df = extract_data()
data_to_db(df)
data = read_from_db("weather")
fecha_ciudad = transform_data(data)
load_by_city(fecha_ciudad)
        