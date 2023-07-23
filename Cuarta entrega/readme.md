# Tercera entrega
### Misión
Este proyecto tiene como objetivo obtener la temperatura de las principales ciudades de España. El programa está diseñado para obtener los datos con una actualización diaria a través de una consulta programada a una API del tiempo. El programa utiliza un script de Python que se encuentra empaquetado en un contenedor de Docker.

El archivo cuenta con los siguientes componentes:

## Dags: 
El programa DAG se compone de un archivo .py que contiene el código que ejecuta la consulta. El programa se estructura de la siguiente manera: 
    1- la importacion de las librerias necesarias.
    2- Creación de las variables que se utilizarán posteriormente (es necesario crear estas variables en Airflow para poder reutilizarlas de manera segura en el código). 
    3- Creación del DAG. 
    4- Definición de las tareas del DAG. 
    5- Las tareas del dag. 
    6- Definición de las funciones.

## docker-compose.yaml:
Documento donde describimos todo lo necesario para que el docker pueda trabajar correctamente

## dockerfile:
Creación de imagen contenedora con todas las librerias necesarias y acciones que se necesitan para que cualquier dispostivo pueda ejecutar el programa.

## Recordatorio.
Para el correcto funcionamiento del script, es importante asegurarse de usar la imagen adecuada en el contenedor Docker. Además, se debe mantener sincronizada la base de datos en DBeaver y asegurarse de haber creado las variables necesarias en Airflow. Las claves de la API Key serán entregadas por mensaje directo en el chat.

## Explicación
Este script esta pensado para tener acceso al tiempo diario en las principales ciudades de España estos datos serán tratados y subidos a una base de datos. la segunda fase del script lee datos de la base de datos, los transforma para obtener la temperatura media diaria y los sube de nuevo a una base de datos  creando la base de datos en caso de ser necesaria.
Contamos con un tipo de alerta en caso de que las funciones tanto de lectura de los datos de la api como obtención de los valores medios fallasen reportandonos via email con la alerta y el tipo de fallo.

¡Gracias por ser parte de este proyecto! Si tienes alguna pregunta no dudes en comunicarte conmigo.