"""
BiciMAD Data to Database - Script para crear la base de datos de BiciMAD

Este script procesa los datos limpios de las estaciones de BiciMAD,
los carga en DataFrames de pandas y los guarda en una base de datos SQLite.
Crea las tablas 'stations' y 'station_status' y define índices para optimizar
las consultas.

Características principales:
- Lee datos limpios de estaciones y estado de estaciones desde archivos CSV.
- Crea una base de datos SQLite para almacenar los datos.
- Crea tablas en la base de datos a partir de los DataFrames de pandas.
- Define un índice único para 'public_station_id' en la tabla 'stations' (simulando clave primaria).
- Crea índices en la tabla 'station_status' para mejorar el rendimiento de las consultas.
- Maneja errores de forma robusta con un bloque try-except.

Requisitos:
- Python 3.6+
- Bibliotecas: sqlite3, pandas, os

Uso:
- Asegúrate de que los archivos CSV 'bicimad_stations.csv' y 'bicimad_station_status.csv'
  existan en el directorio 'data/cleaned/'.
- Ejecuta el script. La base de datos 'bicimad_database.db' se creará en el directorio
  'data/processed/'.

Nota importante:
- Este script (create_db.py) debe ejecutarse desde la carpeta raíz del proyecto
  para que las rutas a los archivos y directorios se resuelvan correctamente.

Autor: Leonardo Leal Vivas
Fecha: [Creación: 19/04/2025 | Modificación: 29/04/2025]
"""


import sqlite3  # Importa la biblioteca sqlite3 para trabajar con bases de datos SQLite.
import pandas as pd  # Importa la biblioteca pandas para manejar DataFrames (estructuras de datos tabulares).
import os  # Importa la biblioteca os para interactuar con el sistema operativo (manejo de rutas de archivos, etc.).

# Define la ruta a la carpeta que contiene los datos limpios (archivos CSV procesados).
data_cleaned_dir = os.path.join('data', 'cleaned')  # Usa os.path.join para construir la ruta de forma segura, independientemente del sistema operativo.

# Define la ruta al directorio donde se guardará el archivo de la base de datos (en una subcarpeta 'processed' dentro de 'data').
db_dir = os.path.join('data', 'processed')
db_file = os.path.join(db_dir, 'bicimad_database.db')  # Define el nombre del archivo de la base de datos.

# Asegúrate de que los directorios necesarios existan. Si no, los crea.
os.makedirs(data_cleaned_dir, exist_ok=True)  # Crea el directorio para los datos limpios. 'exist_ok=True' evita errores si el directorio ya existe.
os.makedirs(db_dir, exist_ok=True)  # Crea el directorio para la base de datos.

# Define las rutas completas a los archivos CSV que contienen los datos de las estaciones y el estado de las estaciones.
stations_csv_file = os.path.join(data_cleaned_dir, 'bicimad_stations.csv')
station_status_csv_file = os.path.join(data_cleaned_dir, 'bicimad_station_status.csv')

# Verificar si los archivos CSV existen antes de intentar cargarlos. Esto es importante para evitar errores.
if not os.path.exists(stations_csv_file):
    print(f"Error: No se encontró el archivo de estaciones en: {stations_csv_file}")
    exit()  # Si el archivo no existe, imprime un mensaje de error y detiene la ejecución del script.

if not os.path.exists(station_status_csv_file):
    print(f"Error: No se encontró el archivo de estado de estaciones en: {station_status_csv_file}")
    exit()  # Si el archivo no existe, imprime un mensaje de error y detiene la ejecución del script.

try:  # Usa un bloque try-except para manejar posibles errores durante la conexión y manipulación de la base de datos.
    # Crear una conexión a la base de datos SQLite. Esto crea el archivo si no existe, o se conecta a él si ya existe.
    conn = sqlite3.connect(db_file)
    print(f"Conexión establecida a la base de datos: {db_file}")

    # Cargar los datos desde los archivos CSV a DataFrames de pandas.
    stations_df = pd.read_csv(stations_csv_file)
    print(f"Archivo de estaciones cargado desde: {stations_csv_file}")

    station_status_df = pd.read_csv(station_status_csv_file)
    print(f"Archivo de estado de estaciones cargado desde: {station_status_csv_file}")

    # Guardar los DataFrames en la base de datos como tablas.
    # 'to_sql' permite escribir DataFrames directamente a una base de datos.
    # 'if_exists='replace'' significa que si la tabla ya existe, se reemplazará con los nuevos datos.
    # 'index=False' evita que pandas escriba el índice del DataFrame como una columna en la tabla.
    stations_df.to_sql('stations', conn, if_exists='replace', index=False)
    print("Tabla 'stations' creada.")

    # Configurar 'public_station_id' como clave primaria (si no lo es ya).
    # En SQLite, no se define la clave primaria de la misma forma que en otros sistemas de bases de datos.
    # En su lugar, se crea un índice único en la columna que se quiere como clave primaria.
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_stations_public_id ON stations(public_station_id)')
    print("Índice único creado en 'stations' para 'public_station_id'.")

    # Guardar la tabla de estado de estaciones.
    station_status_df.to_sql('station_status', conn, if_exists='replace', index=False)
    print("Tabla 'station_status' creada.")

    # Crear índices en la tabla de estado de estaciones para acelerar las consultas.
    conn.execute('CREATE INDEX IF NOT EXISTS idx_station_status_public_id ON station_status(public_station_id)')  # Índice en la columna 'public_station_id'.
    conn.execute('CREATE INDEX IF NOT EXISTS idx_station_status_timestamp ON station_status(timestamp)')  # Índice en la columna 'timestamp'.
    conn.execute('CREATE INDEX IF NOT EXISTS idx_station_status_real_status ON station_status(real_status)')  # Índice en la columna 'real_status'.
    print("Índices creados en 'station_status'.")

    # Cerrar la conexión a la base de datos. Es importante cerrar la conexión para liberar recursos.
    conn.close()
    print(f"Conexión a la base de datos '{db_file}' cerrada.")

except sqlite3.Error as e:  # Captura errores específicos de SQLite.
    print(f"Error de SQLite: {e}")
except Exception as e:  # Captura cualquier otro tipo de error.
    print(f"Error inesperado: {e}")