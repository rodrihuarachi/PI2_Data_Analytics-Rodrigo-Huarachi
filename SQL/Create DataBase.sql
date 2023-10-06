-- Creamos la Base de Datos
CREATE DATABASE PI_RodrigoHuarachi; 

-- Elegimos la Base de Datos creada
USE PI_RodrigoHuarachi;

-- Creamos una tabla para el dataset de Homicidios_hechos
CREATE TABLE homicide_facts (
	Id_hecho VARCHAR (50),
    Nro_víctimas INT,
    Fecha DATE,
    Año INT,
    Mes VARCHAR (50),
    Día INT,
    Hora VARCHAR(100),
    Hora_entera INT,
    Lugar_hecho VARCHAR (200),
    Tipo_calle VARCHAR (100),
    Calle VARCHAR (200),
    Cruce VARCHAR (200),
    Dirección_normalizada VARCHAR (200),
    Comuna INT,
    YX_CABA VARCHAR (200),
    POS_X DOUBLE,
    POS_Y DOUBLE,
    Participantes VARCHAR (200),
    Víctima VARCHAR (200),
    Acusado VARCHAR (200),
    Día_de_Semana VARCHAR (100),
    Tipo_día VARCHAR (200),
    Barrio VARCHAR (200),
    PRIMARY KEY (Id_hecho)
);

-- Creamos una tabla para el dataset de Homicidios_víctimas
CREATE TABLE homicide_victims (
	Id_víctima INT NOT NULL AUTO_INCREMENT,
	Id_hecho VARCHAR (100),
    Fecha DATE,
    Año INT,
    Mes INT,
    Día INT,
    Rol VARCHAR(100),
    Víctima VARCHAR (200),
    Sexo VARCHAR (100),
    Edad INT,
    Fecha_fallecimiento DATETIME,
    PRIMARY KEY (Id_víctima),
    FOREIGN KEY (Id_hecho) REFERENCES homicide_facts(Id_hecho)
);

-- Creamos una tabla para los datos de población
CREATE TABLE poblacion_caba (
	Año INT,
    Población INT
);

--  Habilitamos la funcionalidad de carga local de datos
SET GLOBAL local_infile=1;

-- Ingesta de datos a las tabla creadas
LOAD DATA LOCAL INFILE 'E:/Projects/Henry/Individual Project 2/PI_Data_Analytics_Rodrigo-Huarachi/Datasets/Cleans/homicidios_hechos.csv'
INTO TABLE homicide_facts
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'E:/Projects/Henry/Individual Project 2/PI_Data_Analytics_Rodrigo-Huarachi/Datasets/Cleans/homicidios_víctimas.csv'
INTO TABLE homicide_victims
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' IGNORE 1 LINES
(Id_hecho, Fecha, Año, Mes, Día, Rol, Víctima, Sexo, @Edad, @FechaFallecimiento)
SET Fecha_fallecimiento = NULLIF(TRIM(@FechaFallecimiento), 'SD'),
	Edad = NULLIF(@Edad, 'SD');
    
LOAD DATA LOCAL INFILE 'E:/Projects/Henry/Individual Project 2/PI_Data_Analytics_Rodrigo-Huarachi/Datasets/Cleans/población_caba.csv'
INTO TABLE poblacion_caba
FIELDS TERMINATED BY ',' ENCLOSED BY ''
LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- Vista general de los datos.
SELECT * FROM homicide_facts;

SELECT * FROM homicide_victims;

SELECT * FROM poblacion_caba;

-- Corroboramos la cantidad de filas .
SELECT COUNT(*) FROM homicide_facts;

SELECT COUNT(*) FROM homicide_victims;

-- Desactivamos el modo de actualización segura
SET SQL_SAFE_UPDATES = 0;

-- Aplicamos la función creada
SELECT * FROM homicide_facts;

UPDATE homicide_facts
SET Lugar_hecho = UC_Words(Lugar_hecho),
	Tipo_calle = UC_Words(Tipo_calle),
    Calle = UC_Words(Calle),
    Cruce = UC_Words(Cruce),
    Dirección_normalizada = UC_Words(Dirección_normalizada),
    Participantes = UC_Words(Participantes),
    Víctima = UC_Words(Víctima),
    Acusado = UC_Words(Acusado);
    
SELECT * FROM homicide_victims;

UPDATE homicide_victims
SET Rol = UC_Words(Rol),
	Víctima = UC_Words(Víctima),
    Sexo = UC_Words(Sexo);

-- Normalización
	-- Eliminación de columnas de la tabla 'homicide_facts'
SELECT * FROM homicide_facts;

ALTER TABLE homicide_facts
DROP COLUMN YX_CABA,
DROP COLUMN Dirección_normalizada;

	-- Eliminación de columnas de la tabla 'homicide_victims'
SELECT * FROM homicide_victims;

ALTER TABLE homicide_victims
DROP COLUMN Fecha,
DROP COLUMN Año,
DROP COLUMN Mes,
DROP COlUMN Día;

	-- Modificación de datos
-- En este caso, vamos a modificar la columna 'Calle', ya que tiene las palabras al revés 
ALTER TABLE homicide_facts
ADD COLUMN Calle_ VARCHAR(200) AFTER Calle;

-- Actualiza la nueva columna con la primera parte de la dirección que nos ofrece la columna 'Lugar_hecho'
UPDATE homicide_facts
SET Calle_ = 
    CASE
        -- Si la dirección contiene 'Y', extraer la parte antes de 'Y'
        WHEN Lugar_hecho LIKE '% Y %' THEN TRIM(SUBSTRING_INDEX(Lugar_hecho, ' Y ', 1))
        -- Si la dirección no contiene 'Y', extraer la parte antes del número (si hay un número)
        WHEN Lugar_hecho REGEXP '^[A-Za-z ]+[0-9]' THEN TRIM(SUBSTRING_INDEX(Lugar_hecho, ' ', LENGTH(Lugar_hecho) - LENGTH(REPLACE(Lugar_hecho, ' ', ''))))
        -- Si la dirección no contiene 'Y' ni números, mantenerla tal cual
        ELSE Lugar_hecho
    END;

-- Eliminamos la columna original y cambiamos el nombre de la columna creada
ALTER TABLE homicide_facts
DROP COLUMN Calle,
RENAME COLUMN Calle_ TO Calle;

-- Ahora modificaremos la columna 'Cruce', obteniendo los dato de la columna 'Lugar_hecho'
ALTER TABLE homicide_facts
ADD COLUMN Cruce_ VARCHAR(200) AFTER Cruce;

-- Actualiza la nueva columna con la segunda parte de la dirección que nos ofrece la columna 'Lugar_hecho'
UPDATE homicide_facts
SET Cruce_ = 
    CASE
        -- Si la dirección contiene 'Y', extraer la segunda parte antes de 'Y'
        WHEN Lugar_hecho LIKE '% Y %' THEN TRIM(SUBSTRING_INDEX(Lugar_hecho, ' Y ', -1))
        -- Si la dirección no contiene 'Y' ni números, mantenerla tal cual
        ELSE ''
    END;

-- Eliminamos la columna original y cambiamos el nombre de la columna creada
ALTER TABLE homicide_facts
DROP COLUMN Cruce,
RENAME COLUMN Cruce_ TO Cruce;

SELECT * FROM homicide_facts;

-- Creación de tablas de dimensiones
-- Tabla de Tipos de calles
SELECT DISTINCT(Tipo_calle) FROM homicide_facts;

CREATE TABLE tipo_calle (
    Id_tipo_calle INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Tipo_calle VARCHAR(100)
);

INSERT INTO tipo_calle (Tipo_calle)
SELECT DISTINCT Tipo_calle
FROM homicide_facts;

SELECT * FROM tipo_calle;

	-- Agregamos una nueva columna a la tabla 'homicide_facts'
ALTER TABLE homicide_facts
ADD COLUMN Id_tipo_calle INT AFTER Tipo_calle;

	-- Actualizamos los datos de esa nueva columna
UPDATE homicide_facts AS h
JOIN tipo_calle AS tc ON h.Tipo_calle = tc.Tipo_calle
SET h.Id_tipo_calle = tc.Id_tipo_calle;

	-- Corroboramos
SELECT Tipo_calle,Id_tipo_calle FROM homicide_facts;

	-- Eliminamos la columna de 'Tipo_calle'
ALTER TABLE homicide_facts
DROP COLUMN Tipo_calle;

-- Tabla de Víctimas
SELECT DISTINCT(Víctima) FROM homicide_facts;

/* Notamos que tenemos datos como 'Objeto Fijo' y 'Peaton_Moto', esto puede deberse a un error de tipeo, ya que en el
Diccionario, no tenemo ninguno de esos datos como Víctimas. Asi que procedemos a modificar esos registros.
*/

SELECT * FROM homicide_facts
WHERE Víctima = 'Objeto Fijo';

/* Vemos que en la columna 'Participantes' nos brinda la informacion que en realidad la Víctima es un 'Auto' y el 'Acusado' un Objeto Fijo.
Asi que procedemos a cambiar ese dato.
*/
UPDATE homicide_facts
SET Víctima = 'Auto',
	Acusado = 'Objeto Fijo'
WHERE Id_hecho = '2017-0108';

SELECT * FROM homicide_facts
WHERE Id_hecho = '2017-0108';

-- Ahora veamos el registro de 'Peaton_Moto'
SELECT * FROM homicide_facts
WHERE Víctima = 'Peaton_Moto';

/* En este caso, vemos que es un error de tipeo, ya que se refiere a que la Víctima es un 'Peaton' y el Acusado una 'Moto'.
Asi que procedemos a modificar esos datos.
*/
UPDATE homicide_facts
SET Víctima = 'Peaton',
	Participantes = 'Peaton-Moto'
WHERE Id_hecho = '2020-0063';

SELECT * FROM homicide_facts
WHERE Id_hecho = '2020-0063';

-- Bien, ahora si podriamos crear una tabla de Víctimas
SELECT DISTINCT(Víctima) FROM homicide_facts;

CREATE TABLE tipo_víctimas (
    Id_tipo_víctima INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Víctima VARCHAR(100)
);

INSERT INTO tipo_víctimas (Víctima)
SELECT DISTINCT Víctima
FROM homicide_facts;

SELECT * FROM tipo_víctimas;

	-- Agregamos una nueva columna a la tabla 'homicide_facts'
ALTER TABLE homicide_facts
ADD COLUMN Id_tipo_víctima INT AFTER Víctima;

	-- Actualizamos los datos de esa nueva columna
UPDATE homicide_facts AS h
JOIN tipo_víctimas AS v ON h.Víctima = v.Víctima
SET h.Id_tipo_víctima = v.Id_tipo_víctima;

	-- Corroboramos
SELECT Víctima,Id_tipo_víctima FROM homicide_facts;

-- Eliminamos la columna de 'Víctima'
ALTER TABLE homicide_facts
DROP COLUMN Víctima;

-- Tabla de Acusados
SELECT DISTINCT(Acusado) FROM homicide_facts;

/*Notamos que tenemos un valor 'Pasajeros' como acusado, pero entendemos que se refiere a los transportes públicos
como Colectivo, además hay muchos registros con este valor como para que un pasajero de un transporte público
sea el Acusado. Asi que modificaremos este valor.
*/
SELECT COUNT(*) FROM homicide_facts
WHERE Acusado = 'Pasajeros';

UPDATE homicide_facts
SET Acusado = 'Colectivo'
WHERE Acusado = 'Pasajeros';

/* Bien, ahora que cambiamos eso valores, tambien tenemos que modificar los valores de la columna 'Participantes'
*/
ALTER TABLE homicide_facts
ADD COLUMN Participantes_ VARCHAR(200) AFTER Participantes;

UPDATE homicide_facts AS h
JOIN tipo_víctimas AS v ON h.Id_tipo_víctima = v.Id_tipo_víctima
SET h.Participantes_ = CONCAT(v.Víctima, '-', h.Acusado);

-- Eliminamos la columna 'Participantes'
ALTER TABLE homicide_facts
DROP COLUMN Participantes,
RENAME COLUMN Participantes_ TO Participantes;

-- Ahora si creamos la tabla Acusados
CREATE TABLE acusados (
    Id_acusado INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Acusado VARCHAR(200)
);

INSERT INTO acusados (Acusado)
SELECT DISTINCT Acusado
FROM homicide_facts;

SELECT * FROM acusados;

-- Ahora reemplazamos los registros de la columna 'Acusado' con los Id de la tabla creada.
ALTER TABLE homicide_facts
ADD COLUMN Id_acusado INT AFTER Acusado;

	-- Actualizamos los datos de esa nueva columna
UPDATE homicide_facts AS h
JOIN acusados AS a ON h.Acusado = a.Acusado
SET h.Id_acusado = a.Id_acusado;

	-- Corroboramos
SELECT Acusado,Id_acusado FROM homicide_facts;

-- Eliminamos la columna de 'Acusado'
ALTER TABLE homicide_facts
DROP COLUMN Acusado;

-- Tabla Tipo de día
SELECT DISTINCT(Tipo_día) FROM homicide_facts;

CREATE TABLE tipo_día (
    Id_tipo_día INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Tipo_día VARCHAR(200)
);

INSERT INTO tipo_día (Tipo_día)
SELECT DISTINCT Tipo_día
FROM homicide_facts;

SELECT * FROM tipo_día;

	-- Actualizamos los datos de la columna Tipo_día
UPDATE homicide_facts AS h
JOIN tipo_día AS td ON h.Tipo_día = td.Tipo_día
SET h.Tipo_día = td.Id_tipo_día;

	-- Renombramos la columna Tipo_día
ALTER TABLE homicide_facts
RENAME COLUMN Tipo_día TO Id_tipo_día;

-- Tabla de Día de la semana
SELECT DISTINCT(Día_de_Semana) FROM homicide_facts;

CREATE TABLE días_semana (
    Id_día INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Nombre_día VARCHAR(100)
);

INSERT INTO días_semana (Id_día, Nombre_día) VALUES
(1, 'Lunes'),
(2, 'Martes'),
(3, 'Miércoles'),
(4, 'Jueves'),
(5, 'Viernes'),
(6, 'Sábado'),
(7, 'Domingo');

SELECT * FROM días_semana;

	-- Actualizamos los datos de la columna Día_de_Semana
UPDATE homicide_facts AS h
JOIN días_semana AS d ON h.Día_de_Semana = d.Nombre_día
SET h.Día_de_Semana = d.Id_día;

	-- Renombramos la columna Día_de_Semana
ALTER TABLE homicide_facts
RENAME COLUMN Día_de_Semana TO Id_día;


-- Tabla Barrios
SELECT DISTINCT(Barrio) FROM homicide_facts;

CREATE TABLE barrios (
    Id_barrio INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Barrio VARCHAR(100)
);

/* Vemos que hay un registro que tiene valor como Desconocido, pero hay datos de Latitud y Longitud, asi que podemos imputarlo consultando
A la API del Gobierno de Bueno Aires */

SELECT * FROM homicide_facts
WHERE Barrio = "Desconocido\r";

-- El dato del barrio que nos brinda la API según las coordenadas es 'Recoleta'. Asi que vamos a modificar ese registro.
UPDATE homicide_facts
SET Barrio = 'Recoleta'
WHERE Id_hecho = '2018-0034' AND Barrio = 'Desconocido\r';

-- Reemplazamos el caracter \r, para evitar problemas
UPDATE homicide_facts
SET Barrio = REPLACE(Barrio, '\r', '');

-- Ahora insertamos registros a la tabla Barrios
INSERT INTO barrios (Barrio)
SELECT DISTINCT(Barrio)
FROM homicide_facts;

SELECT * FROM barrios;

	-- Actualizamos los datos de la columna Barrios
UPDATE homicide_facts AS h
JOIN barrios AS b ON h.Barrio = b.Barrio
SET h.Barrio = b.Id_barrio;

	-- Renombramos la columna Barrio
ALTER TABLE homicide_facts
RENAME COLUMN Barrio TO Id_barrio;



-- Tabla 'homicide_victims'
	-- Modificación de la columna 'Víctima'
UPDATE homicide_victims AS hv
JOIN tipo_víctimas AS tv ON hv.Víctima = tv.Víctima
SET hv.Víctima = tv.Id_tipo_víctima;

	-- Cambiamos el nombre de la columna
ALTER TABLE homicide_victims
RENAME COLUMN Víctima TO Id_tipo_víctima;

-- Tabla de Rol
CREATE TABLE rol (
	Id_rol INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Rol VARCHAR(200)
);

INSERT INTO rol (Rol)
SELECT DISTINCT(Rol)
FROM homicide_victims;

SELECT * FROM rol;

-- Modificación de la columna 'Rol'
UPDATE homicide_victims AS hv
JOIN rol AS r ON hv.Rol = r.Rol
SET hv.Rol = r.Id_rol;

ALTER TABLE homicide_victims
RENAME COLUMN Rol TO Id_rol;

-- Tabla Sexo
CREATE TABLE sexo (
	Id_sexo INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    Sexo VARCHAR(100)
);

INSERT INTO sexo (Sexo)
SELECT DISTINCT(Sexo)
FROM homicide_victims;

SELECT * FROM sexo;

UPDATE homicide_victims AS hv
JOIN sexo AS s ON hv.Sexo = s.Sexo
SET hv.Sexo = s.Id_sexo;

-- Cambiamos el nombre de la columna 'Sexo
ALTER TABLE homicide_victims
RENAME COLUMN Sexo TO Id_sexo;

SELECT * FROM homicide_victims;
