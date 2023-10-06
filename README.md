
<h1 align="center"> PROYECTO INDIVIDUAL Nº2 <br> Data Analytics</h1>
<center><img src='./src/Images/img1.png'></center>
<div style="text-align: right; color: #4682A9; font-size: 1.1em; ">
  <a href="https://www.linkedin.com/in/rodrigohuarachi/" style="color: #4682A9;">
    by Rodrigo Huarachi
  </a>
</div>

# **Introducción** 
Los siniestros viales es una de las principales causas de muertes en Argentina segun informes del ``Sistema Nacional de Información Criminal (SNIC)``. Estos pueden tener diversas causas tales como colisiones entre automóviles, motocicletas, bicicletas o peatones, atropellos, choques con objetos fijos o caídas de vehículos.

En la ``Ciudad de Buenos Aires`` con mucho tráfico y alta densidad de población, los siniestros viales pueden ser un gran problema. Estos eventos pueden afectar significativamente la seguridad de los residentes y visitantes de la ciudad, así como la infraestructura vial y los servicios operativos.

La prevención de siniestros viales es importante y requiere un enfoque integral que incluya educación vial, cumplimiento de las normas de tráfico, infraestructura segura y vehículos seguros. Además, es necesario recopilar datos y aplicar políticas efectivas para abordar este problema adecuadamente.

Por lo tanto, el Observatorio de Movilidad y Seguridad Vial (OMSV), un centro de estudios que se encuentra bajo la órbita de la *Secretaría de Transporte* del Gobierno de la Ciudad Autónoma de Buenos Aires, nos ha solicitado la elaboración de un proyecto de anális de datos, con el fin de generar información que le permita a las autoridades locales tomar medidas para disminuir la cantidad de víctimas fatales de los siniestros viales. 


# **Datos**
Los datos utilizados en este análisis fue extraído de la página de ``Buenos Aires Data`` , éstos abarcan siniestros viales desde el año 2016 hasta el 2021 incluyendo información sobre víctimas, vehículos involucrados, lugar del echo, entre otros.<p>
- En este [Diccionario de datos](./Diccionario%20de%20datos/Diccionario%20Datos%20Homicidio.xlsx) puede visualizar todas las definiciones de las variables del dataset.
- Puede ingresar a la página donde se obtuvo los datos haciendo click [aquí](https://data.buenosaires.gob.ar/dataset/victimas-siniestros-viales).



# **Desarrollo**

## **Tecnologías, Herramientas y Lenguajes utilizados**

- Para el desarrollo del EDA:<br><center>
![Visual Studio Code](https://img.shields.io/badge/-Visual%20Studio%20Code-333333?style=flat&logo=visual-studio-code&logoColor=007ACC)
![Jupyter Notebooks](https://img.shields.io/badge/-Jupyter_Notebook-333333?style=flat&logo=jupyter)
![Python](https://img.shields.io/badge/-Python-333333?style=flat&logo=python)
![Pandas](https://img.shields.io/badge/-Pandas-333333?style=flat&logo=pandas)
![Numpy](https://img.shields.io/badge/-Numpy-333333?style=flat&logo=numpy)
![Matplotlib](https://img.shields.io/badge/Matplotlib-333333?style=flat&logo=WordCloud)
![Seaborn](https://img.shields.io/badge/Seaborn-333333?style=flat&logo=Seaborn) </center>

- Para el desarrollo de la Base de Datos:<br><center>
![MySQL Workbench](https://img.shields.io/badge/-MySQL%20Workbench-333333?style=flat&logo=mysql)
![SQL](https://img.shields.io/badge/-SQL-333333?style=flat&logo=sql) </center>

- Para el Dashboard se utilizó:<br><center>
![Power BI](https://img.shields.io/badge/PowerBI-333333?style=flat&logo=powerbi)
![DAX](https://img.shields.io/badge/-DAX-333333?style=flat&logo=powerbi)
 </center>

## **Análisis Exploratorio de Datos (EDA)**

El primer paso que se realizó fue la de extraer los datos del archivo ``.xlsx`` ya que contenia 4 páginas:
1. **HECHOS**: Tabla de datos de los homicidios por siniestros viales en la ``Ciudad Autónoma de Buenos Aires``.
2. **DICCIONARIO_HECHOS**: Diccionario de las variables usadas en la tabla de *HECHOS*.
3. **VICTIMAS**: Tabla de datos de las víctimas involucradas en los homicidios por siniestros viales.
4. **DICCIONARIO_VICTIMAS**: Diccionario de las variables usadas en la tabla de *VICTIMAS*.

Una vez hecho ésto, se analizó la información general de los datos, tanto como cantidad de registros, valores faltantes, cantidad de variables y duplicados. En base a esto se realizó una limpieza y modificación en los registros y tambien en los nombres de las columnas, para un mejor entendimiento de los datos.<br>
Además, se implemento la técnica de ``Web Scraping`` a la **API** de ``Buenos Aires BA DATA`` para poder obtener los datos de los Barrios donde ocurrió el siniestro en base a los datos de Latitud y Longitud que nos brindaba el dataset, y a la página de **Wikipedia** para poder extraer la cantidad de población en la Ciudad Autónoma de Buenos Aires según los censos del 2010 y 2022, ya que para poder generar uno de los KPIs solicitados, era necesario obtener esta información.

> Puede ingresar a este [documento](./Notebooks/EDA.ipynb) para poder ver el paso a paso del EDA.<br>
Tambien puede ver como se realizo la técnica de Web Scraping en este [archivo](./Notebooks/Datos%20complementearios.ipynb).

## **Creación de Base de Datos**
Luego del punto anterior, se creo una *BASE DE DATOS* en ``MySQL Workbench`` para poder normalizar el dataset y asi poder conectarlo a ``Power BI``. En este caso, creamos 11 tablas:
- **Homicidios_hechos**: *Registros de homicidios por siniestros viales.*
- **Homicidios_victimas**: *Registros de las víctimas por siniestro viales.*
- **Barrios**: *Barrios donde ocurrió el siniestro.*
- **Acusados**: *Información del acusado del siniestro.*
- **Días_semana**: *Nombre de los días de la semana.*
- **Población_CABA**: *Cantidad de población en la Ciudad Autónoma de Buenos Aires desde el 2016 hasta el 2022.*
- **Rol**: *Posición relativa al vehículo que presentaba la víctima en el momento del siniestro.*
- **Sexo**: *Sexo de la víctima.*
- **Tipo_calle**: *Tipo de calle donde ocurrió el siniestro.*
- **Tipo_día**: *Tipo de día (Día de semana / Fín de Semana)*
- **Tipo_víctima**: *Vehículo que ocupaba quien haya fallecido o se haya lastimado a raíz del siniestro, o bien peatón/a.*

Y en este proceso modificamos y cambiamos algunos registros que tenian errores, y creamos una función que modifica las primeras letras de cada palabra en mayúsculas y la aplicamos a los registros de las tablas que contenian datos como Direcciones, Calles, etc.
> Puede ingresar a este [documento](./SQL/Create%20DataBase.sql) para poder ver el paso a paso de la creación de la Base de Datos.<br>


## **Análisis de los datos**

El primer análisis se enfocó en la distribución anual de víctimas en los siniestros. Se observó una tendencia descendente a partir de **2018**, con un mínimo notable en **2020** debido a la estricta cuarentena implementada como medida preventiva por el ``COVID-19``. Sin embargo, al examinar la distribución mensual de ese año, se evidenció un aumento significativo a partir de **Diciembre**. Esto se debió a una flexibilización de las restricciones debido a las festividades.

Posteriormente, se evaluó la distribución de víctimas según los días de la semana. Se identificó que los ``Lunes`` y ``Sábados`` presentaban los valores más altos. Esta tendencia es comprensible: los Lunes marcan el inicio de la semana laboral, generando mayor tráfico debido a los desplazamientos laborales. Los Sábados, por otro lado, son el comienzo del fin de semana, momento en el cual las personas salen para reunirse con amigos o familiares, lo que también aumenta la movilidad y, en consecuencia, el tráfico.

Asimismo, se analizó la distribución horaria de los siniestros. Se notó un aumento significativo entre las 5 y las 7 de la tarde. Durante la semana, este incremento se vincula con el horario de ingreso laboral, mientras que los fines de semana, está relacionado con la salida de boliches y fiestas, aumentando las posibilidades de siniestros debido a la presencia de conductores y/o personas en estado de **embriaguez**.

En cuanto a los responsables de los siniestros, se identificó que los ``Autos``, ``Colectivos`` y ``Camiones de carga`` eran los principales protagonistas, destacándose por su incidencia en las **Avenidas**.

Además, se observó que el 76,7% de las víctimas eran masculinos, principalmente entre los 20 y 40 años. Al analizar los tipos de víctimas, se encontró que las ``Motos`` y los ``Peatones`` eran los más afectados. Las Motos, debido a su vulnerabilidad en la carretera, y los Peatones, posiblemente debido al comportamiento imprudente de algunos conductores.

Esta concentración de siniestros en el grupo etario de 20 a 40 años sugiere una posible conexión con comportamientos riesgosos asociados a la juventud y a la falta de experiencia en la conducción. Estos hallazgos subrayan la necesidad de medidas preventivas específicas y de una mayor conciencia pública para mejorar la seguridad vial en estos segmentos de la población.


## **KPIs**

Para este análisis se propuso crear 3 KPIs (Indicadores Claves de Rendimiento):

1. ***Reducir en un 10% la tasa de homicidios en siniestros viales de los últimos seis meses, en CABA, en comparación con la tasa de homicidios en siniestros viales del semestre anterior***.

    > *Definimos a la tasa de homicidios en siniestros viales como el número de víctimas fatales en accidentes de tránsito por cada 100.000 habitantes en un área geográfica durante un período de tiempo específico. Su fórmula es:* <center>
$\frac{\text{Número de homicidios en siniestros viales}}{\text{Población total}} * 100000$<br><br>
</center >

2. ***Reducir en un 7% la cantidad de accidentes mortales de motociclistas en el último año, en CABA, respecto al año anterior***.

    > *Definimos a la cantidad de accidentes mortales de motociclistas en siniestros viales como el número absoluto de accidentes fatales en los que estuvieron involucradas víctimas que viajaban en moto en un determinado periodo temporal. Su fórmula para medir la evolución de los accidentes mortales con víctimas en moto es:*<center>
$\frac{\text{Víctimas en moto del año anterior - Víctimas en moto del año actual}}{\text{Víctimas en moto del año anterior}}*100$<br>
<br></center>

3. ***Reducir en un 10% la cantidad de accidentes en las avenidas en los ultimos 6 meses, en CABA, respecto al semestre anterior***.

    > *Definimos a la cantidad de accidentes mortales en avenidas como el número absoluto de accidentes fatales en los que estuvieron involucradas víctimas que viajaban por avenidas en un determinado periodo temporal. Su fórmula para medir la evolución de los accidentes mortales con víctimas en avenidas es*: <center>
$\frac{\text{Accidentes en avenidas del primer semestre - Accidentes en avenidas del segundo semestre}}{\text{Accidentes en avenidas del primer semestre}}*100$<br>
<br></center>

## **Conclusiones**

Basándonos en las observaciones y datos analizados sobre los accidentes de tráfico, se pueden derivar conclusiones significativas. Estas conclusiones ofrecen una comprensión completa de la situación y podrían desempeñar un papel crucial en la toma de decisiones y la creación de políticas destinadas a mejorar la seguridad vial en la Ciudad Autónoma de Buenos Aires.

- Se registraron 717 víctimas fatales entre los años 2016 a 2021. Con una tendencia descendente apartir del 2019 con pico mínimo en el 2020.

- Diciembre es el mes con mayor cantidad de víctimas, lo cual tiene sentido debido a las festividades.

- Se registran una mayor cantidad de víctimas en los días *Lunes* y *Sabado*.

- El horario crítico de los siniestros es entre las 5AM y las 7AM.

- Mas del 76% de las víctimas son de sexo masculino.

- Las principales víctimas de siniestros son motociclistas y peatones.

- La mayor cantidad de siniestros ocurren en la comuna 1.

- Se ha determinado la urgente necesidad de tomar medidas inmediatas en los controles y regulaciones de las avenidas, dado que estas vías representan un mayor número de víctimas en comparación con otros tipos de calles.

- Se observó que en las avenidas representa un porcentaje significativamente alto de ocurrencia de los siniestros en comparación con otros tipos de calles.