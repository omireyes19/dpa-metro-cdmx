# Descripción de la ETL

La idea es adquirir datos de las fuentes descritas en https://github.com/omireyes19/dpa-metro-cdmx/blob/master/README.md#obtenci%C3%B3n-de-la-informaci%C3%B3n-1, para ello es que necesitamos nuestra máquina EC2 de ingesta en dónde ocurre la parte del ETL que en nuestro caso será más bien un ELT por la descripción siguiente. Esta descripción es técnica y puede cambiar (principalmente el pipeline de ML) según el problema que estemos resolviendo y las implicaciones éticas que consideremos.

- requests a la API del metro de CDMX de forma mensual (*Extract*)
- los JSON/CSVs que recibimos los cargamos directamente a un bucket de S3 (*Load*)
- Levantamos un EMR en aws y con Spark transformamos los datos a Parquet (*Transform*)
- **ML pipeline**
  - Leemos los datos desde una instancia de Sagemaker con Dask o Modlin
  - hacemos el EDA y la limpieza de los datos en un jupyter notebook dentro de sagemaker  
  - hacemos la selección de variables
  - hacemos feature engineering
  - hacemos nuestros scripts reproducibles
  - Entrenamiento de un modelo de Machine Learning usando python y algún framework de ML ó DL.
  - Creación de una API REST con Flask y gunicorn
  - Implementar el modelo de Machine Learning ya en H5 o .pkl en producción usando Nginx y enviar todo el paquete en un contenedor de Docker
  - Cargamos el contenedor a un ECR
- **Batch**
  - Realizamos predicciones y las enviamos a un container de S3  
- **Online**
  - Levantamos un Cliente Kinesis que este escuchando los datos de la API del metro de CDMX
  - Leemos los datos desde nuestra API REST
  - Luego levantamos una instancia serverless Lambda desde la que escuchamos las predicciones que arroja nuestra API REST y este es el Gateway que consume nuestro cliente
