# Cuarta entrega

### Links Necesarios herramientas utilizadas 

* [Descarga de gestor de base de datos:](https://dbeaver.io/download/)
* [Descarga e instalación de docker:](https://www.docker.com/products/docker-desktop/)
* [Descarga Docker compose:](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
* [Descarga WSL Linux para windows:](https://learn.microsoft.com/es-es/windows/wsl/install)
* [Descarga Visual stidio Code:](https://code.visualstudio.com/)

### Pasos Seguidos.
* 1- Instalación herramientas necesarias (WSL, Visual studio, dbeaver)
* 2- Descargar Docker Desktop.
* 3- Instalar Extensiones en visual studio, recomendable(Docker).
![Alt text](./fotos/image-1.png)
* 4- Configurar base de datos AWS en dbeaver (Credenciales aportados por Coderhouse).
![Alt text](./fotos/image-2.png)
* 5- Creación de proyecto final para esto tenemos que crear una carpeta en nuestro escritorio y abrir con VScode.
* 6- Descargar docker-compose.yaml predefinido por airflow en su página web puedes descargarlo desde la consola con el siguiente comando: curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml'
* 7- Quitar la descarga automática de dags de prueba AIRFLOW__CORE__LOAD_EXAMPLES: 'false'.
* 8- Crear las carpetas necesarias Dags, plugins, logs desde consola. mkdir -p ./dags ./logs ./plugins ./config
* 8.1- Al seguir los pasos de la pagina oficial de airflow tenemos que insertar el siguiente comando. echo -e "AIRFLOW_UID=$(id -u)" > .env
* 8.2- dentro del propio arichivo .env insertaremos lo siguiente AIRFLOW_UID=50000
* 9- Creación de docker compose desde la consola: docker compose up airflow-init
* 10- Una vez terminado de levantar el contenedor hay que limpiar el entorno desde consola: docker compose down --volumes --remove-orphans
* 11- Levantar contenedor desde consola: docker compose up
* 12- Crear el script que generará el dag, este mismo hay que guardarlo en la carpeta dags
* 13- Creación de las conexiones en airflow para conectar la base de datos (airflow/admin/connectios)
* 13.1- Creamos la conexión a la base de datos necesarias para trabajar
* ![Alt text](./fotos/image.png)
* 14- Crear las variables para que estén ocultas (airflow/admin/variables)
* 14.1- Tenemos que crear las siguientes variables. (USER, PASSWORD,DATABASE,ENDPOINT,API_KEY Y PORT)
* ![Alt text](./fotos/image-3.png)
* 15- Como la aplicación envia un email en caso de error Hay que realizar la configuración para ello:
* 15.1- Vamos a nuestra cuenta de google "Gestionar tu cuenta de google".
* ![Alt text](./fotos/image-4.png)
* 15.2- En el buscador insertamos buscar contraseña aplicaciones.
* ![Alt text](./fotos/image-5.png)  
* 15.3 Creamos una nueva diciendo que queremos que sea de correo para windows.
* ![Alt text](./fotos/image-6.png) 
* 16- Crear las variables con los correos emisores y receptores en airflow:(SMTP_EMAIL_FROM, SMTP_EMAIL_TO,  SMTP_PASSWORD) (airflow/admin/variables) 
* 17- Modificar el archivo airflow.cfg en el apartado smtp 
* 18- Con las librerias usadas en el script, empatar todas ellas en una imagen para que todo el mundo que lance esta aplicación obtenga las mismas librerias, para ello creamos un archivo dockerfile y ejecutamos en la linea de comandos docker image -t nombre_imagen .
* 19- En docker compose cambiamos el nombre de la imagen que va a utilizar por defecto y le ponemos el nombre de la imagen creada, en este caso es image_coderhouse_final
* 20- Levantamos el contenedor con docker compose up 
* 21- Comprobar el correcto funcionamiento Acceder a airflow desde el navegador: Localhost:8080
* 22- En la parte derecha de en el apartado actions pinchamos en el simbolo PLAY.
* ![Alt text](./fotos/image-7.png)
* 23- Para revisar los estados pinchamos dentro del dag, y en el apartado grid vamos viendo los pasos.
* 24- Verificamos que todo la ejecución ha sido correcta.
*![Alt text](./fotos/image-10.png)
* 25- Vemos si hemos recibido el correo con la correcta ejecución del programa
*![Alt text](./fotos/image-11.png)
* RESULTADO
* Tablas creadas para almacenar la temperaturas de las ciudades
* ![Alt text](./fotos/image-8.png)
* Ejemplo de los campos 
* ![Alt text](./fotos/image-13.png)

### Alertas
En el proyecto hemos puesto diferentes alertas para verificar el buen funcionamiento del programa.
* 1- SLa , si el tiempo se excede a la marca el sistema nos manda un correo para darnos el aviso. 
![Alt text](./fotos/image-14.png)
* 2- Tanto si el programa se ejecuta correctamente como si no lo hace. emitimos un email para tener el control de la situación donde vemos el estado y el tiempo de ejecución.
![Alt text](./fotos/image-15.png)
![Alt text](./fotos/image-16.png)
* 3- Hemos puesto una alerta en una fecha para emitir un correo para la revisión del código.
![Alt text](./fotos/image-17.png)
* 4- La ejecución es correcta como podemos ver en el Grid.
![Alt text](./fotos/image-18.png)
![Alt text](./fotos/image-19.png)

### Misión
Este proyecto tiene como objetivo obtener la temperatura de las principales ciudades de España. El programa está diseñado para obtener los datos con una actualización diaria a través de una consulta programada a una API del tiempo. El programa utiliza un script de Python que se encuentra empaquetado en un contenedor de Docker.

El archivo cuenta con los siguientes componentes:

## Dags: 
El programa DAG se compone de un archivo .py que contiene el código que ejecuta la consulta. El programa se estructura de la siguiente manera: 
* 1- la importacion de las librerias necesarias.
* 2- Creación de las variables que se utilizarán posteriormente (es necesario crear estas variables en Airflow para poder reutilizarlas de manera segura en el código). 
* 3- Creación del DAG. 
* 4- Definición de las tareas del DAG. 
* 5- Las tareas del dag. 
* 6- Definición de las funciones.

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
