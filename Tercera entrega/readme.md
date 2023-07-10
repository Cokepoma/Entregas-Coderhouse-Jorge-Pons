Archivo creado para la obtener la temperatura de las principales ciudades de españa,  el programa esta creado para la obtención de los datos con actualización diaria a través de una consulta prograda a una ai del tiempo. el programa utiliza un script de python el cual esta empaquetado en un contenedor de docker.

el arhivo cuenta con:

Dags: 
el programa dag se compone del archivo .py que tiene el programa que ejecuta la consulta, este programa se compone:
    1-  la importacion de las librerias necesarias
    2-  la creación de las variables a utilizar a posteriori
    3-  la creación de las funciones
    4-  la creación de un dag 
    5-  Las tareas del dag.
    6-  Flujo del dag

claves.py:
En este documento tenemos las claves necesarias para la ejecucuión del código. (oculto con gitignore)

docker-compose.yaml:
Documento donde describimos todo lo necesario para que el docker pueda trabajar correctamente

dockerfile:
Creación de imagen contenedora con todas las librerias necesarias y acciones que se necesitan para que cualquier dispostivo pueda ejecutar el programa.