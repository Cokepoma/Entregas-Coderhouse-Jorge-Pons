import pandas as pd 
import requests
from sqlalchemy import create_engine, DateTime,Float,String
from claves import USUARIO, CONTRASEÑA,ENDPOINT,PUERTO,BASE_DE_DATOS,API_KEY

def extradct_data():
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



df = extradct_data()
data_to_db(df)
