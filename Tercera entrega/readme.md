# Tercera entrega
### Misión
Este proyecto tiene como objetivo obtener la temperatura de las principales ciudades de España. El programa está diseñado para obtener los datos con una actualización diaria a través de una consulta programada a una API del tiempo. El programa utiliza un script de Python que se encuentra empaquetado en un contenedor de Docker.

El archivo cuenta con los siguientes componentes:

## Dags: 
El programa DAG se compone de un archivo .py que contiene el código que ejecuta la consulta. El programa se estructura de la siguiente manera:
    1-  la importacion de las librerias necesarias
    2-  Creación de las variables que se utilizarán posteriormente (es necesario crear estas variables en Airflow para poder reutilizarlas de manera segura en el código).
    3-  Creación del DAG.
    4- Definición de las tareas del DAG.
    5-  Las tareas del dag.
    6-  Definición de las funciones.

## docker-compose.yaml:
Documento donde describimos todo lo necesario para que el docker pueda trabajar correctamente


## Recordatorio:
Para el correcto funcionamiento del script, es importante asegurarse de usar la imagen adecuada en el contenedor Docker. Además, se debe mantener sincronizada la base de datos en DBeaver y asegurarse de haber creado las variables necesarias en Airflow. Las claves de la API Key serán entregadas por mensaje directo en el chat.

¡Gracias por ser parte de este proyecto! Si tienes alguna pregunta no dudes en comunicarte conmigo.
