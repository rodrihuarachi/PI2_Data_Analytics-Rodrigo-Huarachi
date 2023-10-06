import pandas as pd
from datetime import datetime
from prettytable import PrettyTable
import requests
from bs4 import BeautifulSoup
import re


def porcentaje_nulos(data):
    
    """
    Informa el porcentaje de nulos.

    Esta función informa el porcentaje de registros
    nulos por columna en un ``DataFrame``.

    Parameters
    ----------
    data : pandas.DataFrame
        El DataFrame que se va a analizar.

    Returns
    -------
    message
        Por columna muestra el porcentaje de registros vacios.
    """
    
    for columna in data.columns:
        porcentaje_nulos = round(data[columna].isna().mean() * 100, 1)
        print(f'La columna {columna} tiene un {porcentaje_nulos}% de valores nulos.')



def registros_anidados(data):
    
    """
    Informa las columnas con registros anidados.

    Esta función informa que columnas tiene
    registros anidados en un ``DataFrame``.

    Parameters
    ----------
    data : pandas.DataFrame
        El DataFrame que se va a analizar.

    Returns
    -------
    list
        Columnas con registros anidados.
    """

    columnas = [] # Lista para almacenar los nombres de las columnas con valores que comienzan con '{' o '['

    # Iteramos a través de las columnas del DataFrame
    for columna in data.columns:
        if any(data[columna].astype(str).str.startswith('[{')) or any(data[columna].astype(str).str.startswith('{')): # Verificamos si al menos un valor de la columna comienza con '[' o '{'
            columnas.append(columna) # Si cumple con la condicion, agregamos el nombre de la columna en la lista

    return columnas


def filas_duplicadas(data):
    
    """
    Informa cantidad de filas duplicadas.

    Esta función informa la cantidad de filas duplicadas
    que hay en un ``DataFrame``.

    Parameters
    ----------
    data : pandas.DataFrame
        El DataFrame que se va a analizar.

    Returns
    -------
    message
        Cantidad de filas duplicadas si las hay.
    """

    cantidad = data.duplicated().sum()

    if cantidad == 0:
        return f'El DataFrame no tiene filas duplicadas'
    else:
        return f'El Dataframe tiene {cantidad} filas duplicadas'
    

def registros_unicos(data,columna):

    """
    Informa cantidad de registros unicos.

    Esta funcion devuelve la cantidad de registros
    unicos que hay en una columna de un ``DataFrame``.

    Parameters
    ----------
    data : pandas.DataFrame
        El DataFrame que se va a analizar.
    columna : str
        La columna del dataframe donde se van a buscar los valores únicos.

    Returns
    -------
    message
        Cantidad de registros unicos si las hay.
    """

    registros = data[columna].nunique()
    
    if registros == 0:
        return f'La columna {columna} no tiene registros unicos'
    else:
        return f'La columna {columna} tiene {registros} registros unicos'
    

def obtener_release_year(url):

    """
    Extraccion del año del lanzamiento del juego.

    Esta función utiliza el ``Web Scrapping`` para 
    extraer el año del lanzamiento del juego en Steam.

    Parameters
    ----------
    url : str
        Url del juego de Steam.

    Returns
    -------
    int
        Año del lanzamiento del juego.    
    """

    try:
        response = requests.get(url)
        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            game_year_element = soup.find('div', {'class': 'date'})
            if game_year_element:
                game_year = game_year_element.text.strip()
                return game_year
            else:
                return None
        else:
            return None
    except Exception as e:
        print(f"Error en la URL {url}: {str(e)}")
        return None
    


def convert_to_time(x):
    """
    Convierte valores a un objeto de tiempo.

    Esta función convierte los valores de una columna de un
    DataFrame a objetos de tiempo, en este caso en objeto ``time``.

    Parameters:
    -----------
    x (str, datetime, or any)
        El valor que se desea convertir a un objeto de tiempo (time).

    Returns
    -------
    datetime.time or None
        Un objeto de tiempo (time) de Python si la conversión es exitosa,
        o None si no es posible realizar la conversión.
    """
    if isinstance(x, str):
        try:
            return datetime.strptime(x, "%H:%M:%S").time()
        except ValueError:
            return x
    elif isinstance(x, datetime):
        return x.time()
    return x



def tipos_de_datos_unico(dataframe):
    """
    Informa tipos de datos únicos en cada columna.

    Esta función muestra los tipos de datos en una tabla
    por columna en un DataFrame.
    
    Parameters
    -----------
    dataframe (pd.DataFrame)
        El DataFrame del cual deseas conocer los tipos de datos.
    
    Returns
    -------
    prettytable.PrettyTable
        Una tabla PrettyTable que muestra el nombre de la columna y los tipos de datos en esa columna.
    """
    tabla = PrettyTable()
    tabla.field_names = ["Campo", "Tipo de datos"]
    
    for columna in dataframe.columns:
        tipos_de_datos = dataframe[columna].apply(type).unique()
        tipos_de_datos_str = ', '.join(map(str, tipos_de_datos))
        tabla.add_row([columna, tipos_de_datos_str])
    
    print(tabla) 


def count_data_by_type(data,column:str,type:type):
    
    """
    Informa cantidad de registros de un tipo de dato específico.

    Esta función toma un DataFrame, una columna específica y un tipo de datos como entrada,
    y devuelve la cantidad de registros en esa columna que son del tipo especificado.

    Parameters:
    -----------
    data : pandas.DataFrame
        El DataFrame en el que se realizará el conteo.

    column : str
        El nombre de la columna en la que se buscarán los registros del tipo especificado.

    data_type : type
        El tipo de datos (por ejemplo, int, float, str) que se utilizará para identificar los registros.

    Returns
    -------
    int
        La cantidad de registros del tipo especificado en la columna dada del DataFrame.
    """
    cantidad= data.loc[data[column].apply(lambda x: isinstance(x,type))].shape[0]
    
    return f'En la columna {column} del DataFrame hay {cantidad} de registros del tipo {type}'


def calculate_percentage_of_sd(data):
    """
    Informa el porcentaje de valores "SD" (Sin Dato) en cada columna de un DataFrame.

    Esta función toma un DataFrame como entrada y calcula el porcentaje de valores "SD"
    en cada columna del DataFrame. Luego, muestra los resultados en una tabla utilizando la librería ``PrettyTable``.

    Parameters:
    -----------
    data : pandas.DataFrame
        El DataFrame que se utilizará para calcular el porcentaje de valores "SD" en cada columna.

    Returns:
    --------
    prettytable.PrettyTable
        Una tabla PrettyTable que muestra el nombre de la columna y el porcentaje de valores "SD" en esa columna.
    """
    result_table = PrettyTable()
    result_table.field_names = ["Columna", "Porcentaje de 'SD'"]

    
    total_rows = len(data)
    for column in data.columns:
        sd_count = (data[column] == 'SD').sum()
        percentage = (sd_count / total_rows) * 100
        result_table.add_row([column, f"{percentage:.2f}%"])

    print(result_table) 


def barrios_por_comuna(comuna):
    """
    Informa los barrios por comuna.

    Esta función obtiene los nombres de los barrios correspondientes a una comuna
    específica en la ``Ciudad Autónoma de Buenos Aires`` haciendo uso del ``Web Scraping``.

    Parameters:
    -----------
        comuna (int): Número de la comuna de la que se desean obtener los barrios.

    Returns:
    --------
        None: La función imprime los nombres de los barrios de la comuna encontrados en el sitio web.
              Si no se encuentran nombres de barrios, imprime un mensaje indicando que no se encontraron Barrios para esa comuna.
    """
    
    if comuna in range(1,16):
        url = f'https://buenosaires.gob.ar/sede-comunal-{comuna}'

        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        barrios_info = soup.find('div', class_='content').find_all('p')[3].text

        barrios = ['Agronomía', 'Almagro', 'Balvanera', 'Barracas', 'Belgrano', 'Boedo', 'Caballito', 'Chacarita', 'Coghlan', 'Colegiales', 'Constitución', 'Flores', 'Floresta', 'La Boca', 'La Paternal', 'Liniers', 'Mataderos', 'Monserrat', 'Monte Castro', 'Nueva Pompeya', 'Núñez', 'Palermo', 'Parque Avellaneda', 'Parque Chacabuco', 'Parque Chas', 'Parque Patricios', 'Paternal', 'Puerto Madero', 'Recoleta', 'Retiro', 'Saavedra', 'San Cristóbal', 'San Nicolás', 'San Telmo', 'Vélez Sársfield', 'Versalles', 'Villa Crespo', 'Villa del Parque', 'Villa Devoto', 'Villa General Mitre', 'Villa Lugano', 'Villa Luro', 'Villa Ortúzar', 'Villa Pueyrredón', 'Villa Real', 'Villa Riachuelo', 'Villa Santa Rita', 'Villa Soldati']

        # Creamos una expresión regular para encontrar palabras en el texto
        patron = r'\b(?:' + '|'.join(barrios) + r')\b'  # Vemos si coincide con barrios en la lista

        # Buscamos todas las palabras en el texto
        barrios_encontrados = re.findall(patron, barrios_info)

        
        if barrios_encontrados:
            num_barrios = len(barrios_encontrados)
            if num_barrios > 1:
                barrios_formateados = ', '.join(barrios_encontrados[:-1]) + ' y ' + barrios_encontrados[-1]
            else:
                barrios_formateados = barrios_encontrados[0]
            return f'Los barrios que conforman la Comuna {comuna} son: {barrios_formateados}'
        else:
            return 'No se encontraron barrios correspondientes a esa Comuna.'
    else:
        return 'Ingrese un número de Comuna válido'



