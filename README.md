# dpa-metro-cdmx

## Descripción del Proyecto

De manera muy general se llevará a cabo un proyecto de arquitectura de
datos con las siguientes consideraciones:

  ###  Planteamiento del problema.

  ###  Obtención de la información.

  ###  Plantemiento del modelo.

  ### Planteamiento del flujo de información.

  ### Planteamiento del entregable del proyecto.

A continuación se detalla brevemente cada uno de los proecesos
anteriores.

## Planteamiento del problema

El metro de la ciudad de México es utilizado como medio de transporte
por millones de personas al día. Es fácil intuir que el dinero que
percibe el gobierno por la venta de entradas a este toma mucha relevancia
en el presupuesto mensual. Hay distintas situaciones que provocan el
cierre definitivo de algunas estaciones: desastres naturales,
manifestaciones y mantenimiento, por mencionar algunas. Resulta pues
indispensable una correcta medida de posibles pérdidas diarias en caso
de que cualquier estación se viera en una situación como la mencionada
previamente para poder hacer un recorte al presupuesto esperado y tomar
medidas económicas derivadas de ello.

## Obtención de la información

Para llevar a cabo el proyecto se usará la información de afluencias
registrada por la ciudad de méxico, dicha información puede obtenerse en
**https://datos.cdmx.gob.mx**. Adicionalmente a esta información se
está planteando utilizar la información correspondiente al número de
viajes y llegadas de las ecobicis, esta información puede obtenerse en
la página **https://www.ecobici.cdmx.gob.mx/**.

## Limpieza de la información

Antes de proceder a usar la información será necesario verificar la
calidad de esta, para ello, tentativamente construirán ciertos índices
que nos ayuden a validar la información.

## Plantemiento del modelo 

Una vez que se cuenta con la información limpia, tentativamente se
propondrá utilizar un modelo predictivo de machine learning, para ello
se intentará encontrar y construir un set de variables con alto poder
predictivo.

## Planteamiento del flujo de información

Una vez identificada la información con la que se trabajará, se
planteará cómo será el flujo de la información, así como la manera en la
que se actualiza y se obtienen los resultados del modelo predicho. Para
el flujo de la información deberán considerarse lo siguiente:

###  Protocolo de lectura, carga y actualización de la información.

Partimos del supuesto de que los usuarios están creados en el EC2 según el proceso visto en las [notas de clase](https://github.com/ITAM-DS/data-product-architecture/blob/master/03_infrastructure.md). El `password` de cada usuario es el mismo `user`.

##### 1. Una vez dentro de la EC2 con el user personal, hacer un clone al [repositorio](https://github.com/omireyes19/dpa-metro-cdmx) del proyecto.

~~~~bash
cd 
git clone https://github.com/omireyes19/dpa-metro-cdmx
~~~~

##### 2. Vamos a instalar `pyenv` y `virtualenv` para generar una versión de python ad hoc a nuestro proceso llamado `dpa-metro-cdmx`.

~~~~bash
cd dpa-metro-cdmx/
chmod +x pyenv_installment.sh
chmod +x create_virtualenv.sh
./pyenv_installment.sh
./create_virtualenv.sh
~~~~

###  Protocolo de validación de la información.

###  Protocolo de errores y recuperación.

###  Protocolo de alimentación del modelo.

###  Protocolo del entregable.

###  Consideraciones generales para producción.

###  Linaje de datos.
#### 1. Extracción: Obtenemos los datos de la API de Datos Abiertos Ciudad de México.
#### 2. Loading: Subimos los datos a S3 en carpetas nombradas de acuerdo a la fecha con archivos en formato json con procesamiento en EC2. El layout contiene las variables: 
* Fecha: fecha a la que corresponden los datos en formato DD/MM/YYYY.
* Año: año al que corresponden los datos.
* Línea: nombre de la línea a la que corresponde la estación.
* Estación: nombre de la estación.
* Afluencia: número de personas que entran a la estación.

#### 3. Transformation: Usamos Zeppeling para transformar los datos de json a parquet porque le damos una estructura hdfs. La estructura de directorios está definida por el particionamiento de las variables *fecha* y *estación*. Además, añadimos variables dicotómicas correspondientes a día de la semana, día festivo y fin de semana.


## Planteamiento del entregable del proyecto

De manera tentativa se entregará una interfaz, donde el usuario consulte
la afluencia predicha para cierta estación y día.
