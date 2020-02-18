# dpa-metro-cdmx

## Descripción del Proyecto

De manera muy general se llevará a cabo un proyecto de arquitectura de
datos, para ello se deberá considerar y planificar con cuidado los
siguientes procesos:

  ###  Planteamiento del problema.

  ###  Obtención de la información.

  ###  Plantemiento del modelo.

  ### Planteamiento del flujo de información.

  ### Planteamiento del entregable del proyecto.

A continuación se detalla brevemente cada uno de los proecesos
anteriores.

## Planteamiento del problema

El metro de la ciudad de México es utilizado como medio de transporte
por millones de personas al día. Alguno de los problemas que presenta el
metro de la ciudad de México es la gran afluencia de personas que ocurre
en determinadas estaciones en determinadas horas del día, prevenir tales
afluencias puede ayudar a tomar decisiones con anticipación, es decir,
puede ayudar a predecir la afluencia de ciertas estaciones a ciertas
horas, lo cual puede ser utilizada para gestionar la oferta de servicio
en tales estaciones. También puede ser útil para conocer cuántos boletos
deben mantenerse por estación en determinadas horas, o que mantenimiento
debe darsele a las estaciones con mayor afluencia. Dicho lo anterior se
pretende entregar un proyecyo de datos, el cual prediga la alfuencia
diaria a nivel estación, para la toma de decisiones.

## Obtención de la información

Para llevar a cabo el protecto se usará la información de afluencias
registrada por la ciudad de méxico, dicha información puede obtenerse en
**https://datos.cd** **mx.gob.mx**. Adicionalmente a esta información se
está panteando utilizar la información correspondiente al número de
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

###  Protocolo de validación de la información.

###  Protocolo de errores y recuperación.

###  Protocolo de alimentación del modelo.

###  Protocolo del entregable.

###  Consideraciones generales para producción.

## Planteamiento del entregable del proyecto

De manera tentativa se entregará una interfaz, donde el usuario consulte
la afluencia predicha para cierta estación y día.
