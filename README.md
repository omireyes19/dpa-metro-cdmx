# dpa-metro-cdmx

## Descripción del Proyecto

En este proyecto simularemos la puesta en producción de un producto de datos,
es decir, contemplaremos desde la ingesta de la información, su tratamiento
y limpieza para que sea consumida por un modelo que entrenaremos y usaremos
para hacer pronósticos de los datos no observados del futuro de manera
automática. Así mismo, a lo largo de todo el pipeline que mencionamos, 
usaremos gobierno para tener el linaje correcto del flujo completo para
detectar y ser capaces de corregir cualquier eventualidad, derivada de
posibles fallos en las ingestas o incluso un mal rendimiento del modelo
una vez automatizado.

## Planteamiento del problema

El metro de la ciudad de México es utilizado como medio de transporte
por millones de personas al día. Hay días en los que puede estar más afluido de lo normal debido a distintos factores.  Intentaremos capturar algunos de esos factores para predecir de forma diaria si la afluencia en cada estación será baja, normal o alta con el objetivo de que la administración del metro de la CDMX pueda asignar recursos de seguridad, limpieza y operadores de manera eficiente.

## Implicaciones éticas

No hacer una buena interpretación de los resultados del modelo que resulte en una mala asignación de recursos puede ocasionar accidentes en caso de saturación de una estación, además de que puede subir el índice delictivo como resultado de lo mismo.

## Obtención de la información

La información de la afluencia diaria del metro en la CDMX se puede
obtener fácilmente desde el sitio de [datos de la Ciudad](https://datos.cdmx.gob.mx)
a través de una API. 

## Descripción de la información

Las siguientes variables hacen referencia a la afluencia diaria del
metro de la CDMX. La información está disponible a partir
del 1 de enero del 2010, la cual se puede descargar en distintos formatos, 
nosotros por facilidad escogimos `.json`

1.  **Fecha**:esta variable es de escala de intervalo, y representa la
    fecha en la que se registró la afluencia. Esta variable está en
    formato **DD_/MM_/AAAA**.

2.  **Año**: esta variable es de escala de intervalo, y representa el
    año en la que se registró la afluencia.

3.  **Linea**: esta variable es de escala nominal, y representa el
    nombre de la línea del metro donde se registró la afluencia. Esta
    variable toma como valor el nombre de alguna de las 12 líneas del
    metro de la ciudad de México.

4.  **Estacion**: esta variable es de escala nominal, y representa el
    nombre de la estación del metro donde se registró la afluencia. Esta
    variable toma como valor el nombre de alguna de las 163
    estaciones del metro de la ciudad de México.

5.  **Afluencia**: esta variable es de escala de intervalo, y representa
    la afluencia total registrada.


## Flujo de ingesta (ELT)

###  Protocolo de lectura, carga y actualización de la información.

Partimos del supuesto de que los usuarios están creados en el EC2 según el proceso visto en las [notas de clase](https://github.com/ITAM-DS/data-product-architecture/blob/master/03_infrastructure.md). El `password` de cada usuario es el mismo `user`.

Dependencias
============

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

RAW
===

##### 3. Una vez creado el entorno virtual, hacemos la ingesta con un script de bash que carga toda la historia (de enero 2010 hasta febrero 2020).

Es importante mencionar que este script manda a ejecutar dos tasks de luigi concatenados, desde donde hacemos la consulta a la API, para descargar los `.json` de cada año-mes-estación, y a su vez generar la metadata de dicha ingesta. Todo esto bajo el esquema de RAW.

~~~~bash
cd scripts/
chmod +x ingesta.sh
./ingesta.sh
~~~~


La metadata que estaríamos generando en este paso es la siguiente:

1.  Fecha de ejecución. 

2.  Parámetros de la carga: Año, Mes, Estación

3.  Número de registros. Este valor nos servirá como control de calidad, ya que para cada día, el número de registros debe ser `días del mes * número de líneas por las que pasa cada estación`

4.  Usuario de ejecución

5.  Nombre de la base de datos

6.  Esquema

7.  Tabla

8.  Usuario BD

Nuestro esquema de datos es:
- Año -> int
- Estación -> string
- Fecha -> string
- Línea -> string
- Afluencia -> int

Preprocessed
============

##### 4. Ya con las ingestas en RAW, generaremos un script para pasar los `.json` a formato `.csv` simulando una estructura hdfs, es decir, teniendo
carpetas como variables pues sabemos que la lectura de spark es óptima cuando se hace a nivel partición. Además, traducimos las variables al inglés.

La metadata que estaríamos generando en este paso es la siguiente:

1.  Fecha de ejecución.

2.  Parámetros del archivo modificado: Año, Mes, Estación

3.  Número de registros modificados.

4.  Especificación del cambio: `.json` a  `.csv`.

Nuestro esquema de datos es:
- year -> int (como partición)
- month -> int (como partición)
- station -> string (como partición)
- date -> string
- line -> string
- influx -> int

Cleaned
=======

##### 5. Finalmente, generamos la base Cleaned manteniendo una estructura de directorios definida por el particionamiento de las variables *year*, *month* y *station*. Además, añadimos las variables: día de la semana, día festivo y número de líneas que cruzan en cada estación.

La metadata que estaríamos generando en este paso es la siguiente:

1.  Fecha de ejecución.

2.  Parámetros del archivo modificado: Año, Mes, Estación

3.  Número de registros modificados.

Nuestro esquema de datos es:
- year -> int (como partición)
- month -> int (como partición)
- station -> string (como partición)
- date -> string
- line -> string
- influx -> int
- day_of_week -> int
- holiday -> int
- line_crossing -> int

Construcción de variable objetivo
=================================

##### 6. Partiendo de la base limpia procedemos a crear nuestra variable objetivo. Para lo cual calculamos un intervalo definido por el rango intercuartil a nivel estación-línea de la afluencia en los últimos 3 meses. Esto debido a que como podemos esperar, cada estación-línea tendrá una afluencia muy distinta, existen estaciones más recurridas que otras. Una vez definido el intervalo, etiquetamos cada uno de los días por la afluencia que tuvo: si está por debajo del rango decimos que la afluencia es Baja, si está dentro del intervalo decimos que es Media y si está por encima lo etiquetaremos como Alta.

Es importante recalcar que cada vez que entrenemos, guardaremos en un dataframe el valor mínimo y máximo de cada estación-línea para poder utilizarlo en la fase productiva. Es decir, cada que llegue una observación nueva es necesario calcular la precisión de nuestro modelo en producción, de tal forma que cruzaremos la afluencia observada con los intervalos para pegarle la etiqueta acorde a lo comentado en el párrafo anterior.

La metadata que estaríamos generando en este paso es la siguiente:

1.  Fecha de ejecución.

2.  Parámetros del archivo modificado: Año, Mes, Estación

3.  Número de registros modificados.

Nuestro esquema de datos es:
- year -> int (como partición)
- month -> int (como partición)
- station -> string (como partición)
- date -> string
- line -> string
- influx -> int
- day_of_week -> int
- holiday -> int
- line_crossing -> int
- label -> int

Entrenamiento
=============

##### 7. Generaremos un modelo por cada estación-línea para una ventana de 30 días mensualmente. Para la fase de entrenamiento se decidió construir la siguiente partición de datos Entrenamiento-Validación-Producción

![Image description](https://github.com/omireyes19/dpa-metro-cdmx/blob/master/images/flujo.png)

La base de datos del metro se aprovisiona mensualmente, en donde se incorpora la historia del mes terminado. Esta carga de información, dispararía el proceso de entrenamiento, considerando como inicio el nuevo mes, generando tres tablones: el de entrenamiento, el de validación y el de producción.

Para generar estos tablones generamos Pipelines cuyas fases son las siguientes:
- `OneHotEncoding` para la variable `day_of_week`
- `StringIndexer` para la variable `label`
- Generamos el `VectorAssembler` con las variables input
- `RandomForestClassifier` con hiperpáramentros definidos en un `ParamGrid` con distintas configuraciones.
- Finalmente un `CrossValidator` de 3 folds 

Al final del entrenamiento, guardamos el mejor modelo en test para usarlo en producción y la salida con las estimaciones para el siguiente mes.

Esta parte quedará bastante interesante cuando lleguemos a Producción. Continuará...


## Linaje de datos.
### 1. Extracción: Obtenemos los datos de la API de Datos Abiertos Ciudad de México.
### 2. Loading: Subimos los datos a S3 en carpetas nombradas de acuerdo a la fecha con archivos en formato json con procesamiento en EC2. 
### 3. Transformation: Transformamos los datos de json a parquet y damos una estructura hdfs. La estructura de directorios está definida por el particionamiento de las variables *fecha* y *estación*. Además, añadimos como variables el mes, el día del mes y con cuántas líneas cruza la estación, junto con variables dicotómicas correspondientes a día de la semana, día festivo y fin de semana.
### 4. Creamos la variable objetivo 'label' a partir de los datos históricos por línea y estación y asignamos las etiquetas 1: afluencia baja, 2: afluencia normal y 3: afluencia alta.

## Modelado
La variable a predecir es la etiqueta de afluencia que creamos 'label', como parte de la preparación de datos, utilizamos one hot encoder para transformar las variables dia de la semana, linea y estacion y utilizamos un Random Forest para hacer clasificación multiclase.

## Planteamiento del entregable del proyecto

De manera tentativa se entregará un tablero dinámico en donde el usuario será capaz de ver la predicción de la afluencia diaria por estación del mes siguiente.
