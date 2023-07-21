# Tercera entrega
### Misión
Archivo creado para la obtener la temperatura de las principales ciudades de españa,  el programa esta creado para la obtención de los datos con actualización diaria a través de una consulta programada a una api del tiempo. el programa utiliza un script de python el cual esta empaquetado en un contenedor de docker.

el arhivo cuenta con:

## Dags: 
el programa dag se compone del archivo .py que tiene el programa que ejecuta la consulta, este programa se compone:
    1-  la importacion de las librerias necesarias
    2-  la creación de las variables a utilizar a posteriori
    3-  la creación de las funciones
    4-  la creación de un dag 
    5-  Las tareas del dag.
    6-  Flujo del dag

## claves.py:
En este documento tenemos las claves necesarias para la ejecucuión del código. (oculto con gitignore)

## docker-compose.yaml:
Documento donde describimos todo lo necesario para que el docker pueda trabajar correctamente

## dockerfile:
Creación de imagen contenedora con todas las librerias necesarias y acciones que se necesitan para que cualquier dispostivo pueda ejecutar el programa.
Tercera entrega
Misión
Este proyecto tiene como objetivo obtener la temperatura de las principales ciudades de España. El programa está diseñado para obtener los datos con una actualización diaria a través de una consulta programada a una API del tiempo. El programa utiliza un script de Python que se encuentra empaquetado en un contenedor de Docker.

El archivo cuenta con los siguientes componentes:

Dags:
El programa DAG se compone de un archivo .py que contiene el código que ejecuta la consulta. El programa se estructura de la siguiente manera:

Importación de las librerías necesarias.
Creación de las variables que se utilizarán posteriormente (es necesario crear estas variables en Airflow para poder reutilizarlas de manera segura en el código).
Definición de las funciones.
Creación del DAG.
Definición de las tareas del DAG.
Flujo del DAG.
docker-compose.yaml:
Este documento describe todos los elementos necesarios para que el contenedor de Docker funcione correctamente.

dockerfile:
Este archivo contiene las instrucciones para crear una imagen de contenedor que incluye todas las librerías necesarias y las acciones requeridas para que el programa pueda ejecutarse en cualquier dispositivo.

Recordatorio:
Para el correcto funcionamiento del script, es importante asegurarse de usar la imagen adecuada en el contenedor Docker. Además, se debe mantener sincronizada la base de datos en DBeaver y asegurarse de haber creado las variables necesarias en Airflow. Las claves de la API Key serán entregadas por mensaje directo en el chat.

¡Gracias por ser parte de este proyecto! Si tienes alguna pregunta no dudes en comunicarte conmigo.
