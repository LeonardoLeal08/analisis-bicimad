"""
BiciMAD Data Collector - Script para recolectar datos de estaciones de BiciMAD en Madrid

Este script se conecta a la API oficial de BiciMAD para obtener información en tiempo real
sobre las estaciones de bicicletas compartidas. Recopila datos como la ubicación de cada estación,
disponibilidad de bicicletas, bases libres, etc. y los almacena en formato JSON y CSV.

Características principales:
- Conecta con la API de BiciMAD usando un token de acceso
- Recopila datos periódicamente (configurable)
- Almacena datos en formato JSON (respaldo) y CSV (análisis)
- Organiza los datos por fecha y hora
- Registra estadísticas y actividad en archivos de log

Requisitos:
- Python 3.6+
- Bibliotecas: requests, pandas, dotenv, schedule
- Archivo .env con el token de acceso

Configuración del intervalo de recolección:
- Por defecto: cada 120 minutos
- Para cambiar: modificar la variable COLLECTION_INTERVAL_MINUTES en el archivo .env
- Ejemplo del contenido del archivo .env:
  BICIMAD_ACCESS_TOKEN=tu_token_aquí
  COLLECTION_INTERVAL_MINUTES=120

Autor: Leonardo Leal Vivas
Fecha: [Creación: 06/04/2025 | Modificación: 29/04/2025]
"""

import requests
import pandas as pd
import json
import time
import logging
import os
import re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import schedule
import unicodedata


class BiciMADCollector:
    """
    Clase principal para la recolección de datos de estaciones BiciMAD.

    Esta clase gestiona la conexión con la API, la recolección de datos,
    el procesamiento y almacenamiento de la información en archivos.
    """

    def __init__(self):
        """
        Inicializa el recolector de datos de BiciMAD.

        Configura los parámetros iniciales, carga el token de acceso,
        establece la estructura de directorios y prepara el sistema de logging.

        Raises:
            ValueError: Si no se encuentra el token de acceso en el archivo .env
        """
        # Cargar variables de entorno desde archivo .env
        load_dotenv()
        self.access_token = os.getenv('BICIMAD_ACCESS_TOKEN')
        if not self.access_token:
            raise ValueError("No se encontró BICIMAD_ACCESS_TOKEN en el archivo .env")

        # Configurar parámetros de la API
        self.base_url = "https://openapi.emtmadrid.es/v1/transport/"
        self.headers = {
            'accessToken': self.access_token,
            'Content-Type': 'application/json'
        }

        # Configurar estructura de directorios para almacenar datos
        self.base_dir = Path("bicimad_data")
        self.data_dir = self.base_dir / "collections"  # Para archivos JSON
        self.csv_dir = self.base_dir / "csv"  # Para archivos CSV

        # Crear directorios necesarios si no existen
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.csv_dir.mkdir(parents=True, exist_ok=True)

        # Inicializar estadísticas de recolección
        self.collection_stats = {
            'start_time': datetime.now(),
            'collections_made': 0,
            'total_files': 0,
            'errors': 0
        }

        # CSV principal que se actualizará con cada recolección
        self.main_csv_path = self.csv_dir / "bicimad_stations_data.csv"

        # Configurar logging
        self.setup_logging()

    def setup_logging(self):
        """
        Configura el sistema de logging para registrar la actividad.

        Crea un archivo de log por día con el formato 'collection_YYYYMMDD.log'
        y configura salida tanto a archivo como a consola.
        """
        log_file = self.base_dir / f"collection_{datetime.now().strftime('%Y%m%d')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def normalize_text(self, text):
        """
        Normaliza texto eliminando caracteres especiales y acentos.

        Esta función prepara textos como nombres de estaciones y direcciones
        para que sean consistentes y no causen problemas de codificación.

        Args:
            text (str): Texto a normalizar

        Returns:
            str: Texto normalizado sin caracteres especiales ni acentos
        """
        if not text:
            return ""

        # Reemplazar "nº" por "n." y otros caracteres especiales
        text = text.replace("nº", "n.").replace("º", ".")

        # Normalizar caracteres Unicode (acentos, etc.)
        text = unicodedata.normalize('NFKD', text)
        text = ''.join([c for c in text if not unicodedata.combining(c)])

        # Eliminar caracteres no ASCII y controlar la codificación
        text = re.sub(r'[^\x00-\x7F]+', '', text)

        return text

    def get_station_info(self):
        """
        Obtiene información de estaciones BiciMAD desde la API oficial.

        Realiza la petición HTTP a la API de BiciMAD y maneja posibles errores.

        Returns:
            dict: Datos JSON de las estaciones o None si hay error
        """
        endpoint = "bicimad/stations/"
        try:
            response = requests.get(self.base_url + endpoint, headers=self.headers)
            response.raise_for_status()  # Lanza excepción si hay error HTTP
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error obteniendo información de estaciones: {e}")
            self.collection_stats['errors'] += 1
            return None

    def save_json_data(self, data):
        """
        Guarda los datos originales en formato JSON como respaldo.

        Organiza los archivos en directorios por fecha (YYYYMMDD).

        Args:
            data (dict): Datos JSON a guardar

        Returns:
            Path: Ruta del archivo guardado o None si hay error
        """
        if data is None:
            return None

        # Crear directorio con la fecha actual
        current_date = datetime.now().strftime('%Y%m%d')
        date_dir = self.data_dir / current_date / "stations"
        date_dir.mkdir(parents=True, exist_ok=True)

        # Crear nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%H%M%S")
        filename = date_dir / f"stations_{timestamp}.json"

        # Guardar los datos en formato JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        self.collection_stats['total_files'] += 1
        logging.info(f"Datos guardados en JSON: {filename}")
        return filename

    def process_stations_data(self, data):
        """
        Procesa y transforma los datos de estaciones a formato tabular (DataFrame).

        Extrae la información relevante de cada estación y la estructura
        para facilitar análisis posteriores.

        Args:
            data (dict): Datos JSON de las estaciones

        Returns:
            DataFrame: Datos procesados en formato tabular o None si hay error
        """
        if not data or 'data' not in data:
            logging.error("Formato de datos inválido o vacío")
            return None

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Formato: YYYY-MM-DD HH:MM:SS
        rows = []

        for station in data['data']:
            # Normalizar los campos de texto para evitar problemas de codificación
            name = self.normalize_text(station.get('name', ''))
            address = self.normalize_text(station.get('address', ''))

            # Extraer coordenadas geográficas (primero de 'geometry', luego directo)
            longitude = station.get('geometry', {}).get('coordinates', [0, 0])[0]
            latitude = station.get('geometry', {}).get('coordinates', [0, 0])[1]

            if longitude == 0 and 'longitude' in station:
                longitude = station.get('longitude', 0)
            if latitude == 0 and 'latitude' in station:
                latitude = station.get('latitude', 0)

            # Extraer información de disponibilidad
            availability = station.get('dock_bikes', 0)  # Bicicletas disponibles
            free_bases = station.get('free_bases', 0)  # Bases libres
            total_bases = station.get('total_bases', 0)  # Total de bases

            # En caso de que la API cambie su estructura, buscar datos en ubicaciones alternativas
            if availability == 0 and 'light' in station and isinstance(station.get('light'), dict):
                availability = station.get('light', {}).get('availability', 0)
            if free_bases == 0 and 'light' in station and isinstance(station.get('light'), dict):
                free_bases = station.get('light', {}).get('free', 0)
            if total_bases == 0:
                total_bases = availability + free_bases

            # Crear diccionario con la información relevante de la estación
            station_data = {
                'timestamp': timestamp,
                'station_id': station.get('id'),
                'name': name,
                'address': address,
                'longitude': longitude,
                'latitude': latitude,
                'total_bases': total_bases,
                'active_bases': availability + free_bases,
                'available_bikes': availability,
                'free_bases': free_bases,
                'reservations': station.get('reservations', 0),
                'status': 1 if station.get('activate', False) else 0
            }

            # Registrar advertencias para campos problemáticos
            if longitude == 0 or latitude == 0:
                logging.warning(f"Coordenadas no encontradas para estación {station.get('id')} - {name}")

            rows.append(station_data)

        # Verificar que se hayan extraído datos
        if not rows:
            logging.error("No se pudieron extraer datos de ninguna estación")
            return None

        # Crear DataFrame con los datos procesados
        df = pd.DataFrame(rows)
        logging.info(f"Datos procesados: {len(df)} estaciones")

        # Registrar la primera fila para depuración
        if not df.empty:
            logging.info(f"Ejemplo de datos (primera fila): \n{df.iloc[0].to_dict()}")

        return df

    def update_csv(self, df):
        """
        Actualiza el archivo CSV principal con los nuevos datos recolectados.

        Mantiene un CSV histórico con todas las recolecciones y también
        crea archivos CSV individuales por cada recolección.

        Args:
            df (DataFrame): DataFrame con los datos procesados
        """
        if df is None or df.empty:
            logging.warning("No hay datos para actualizar el CSV")
            return

        # Actualizar CSV principal: añadir al final si existe, o crear nuevo
        if self.main_csv_path.exists():
            df.to_csv(self.main_csv_path, mode='a', header=False, index=False, encoding='utf-8')
            logging.info(f"CSV actualizado: {self.main_csv_path}")
        else:
            # Si es la primera vez, crear el archivo con encabezados
            df.to_csv(self.main_csv_path, index=False, encoding='utf-8')
            logging.info(f"CSV creado: {self.main_csv_path}")

        # Crear también un archivo CSV para esta recolección específica
        current_date = datetime.now().strftime('%Y%m%d')
        timestamp = datetime.now().strftime("%H%M%S")
        daily_csv_dir = self.csv_dir / current_date
        daily_csv_dir.mkdir(parents=True, exist_ok=True)
        daily_csv_path = daily_csv_dir / f"stations_{timestamp}.csv"
        df.to_csv(daily_csv_path, index=False, encoding='utf-8')

        logging.info(f"Datos de recolección guardados en: {daily_csv_path}")

    def collect_data(self):
        """
        Función principal que realiza todo el proceso de recolección de datos.

        Coordina la obtención, procesamiento y almacenamiento de los datos
        y maneja posibles errores durante el proceso.
        """
        try:
            logging.info("Iniciando recolección")

            # Obtener datos de la API
            data = self.get_station_info()

            if data:
                # Guardar versión JSON original (para backup)
                self.save_json_data(data)

                # Procesar y convertir a DataFrame
                df = self.process_stations_data(data)

                if df is not None and not df.empty:
                    # Actualizar CSV principal
                    self.update_csv(df)

                    self.collection_stats['collections_made'] += 1
                    logging.info("Recolección completada con éxito")
                else:
                    logging.error("Falló el procesamiento de datos")
                    self.collection_stats['errors'] += 1
            else:
                logging.error("No se pudieron obtener datos en esta recolección")
                self.collection_stats['errors'] += 1

        except Exception as e:
            logging.error(f"Error durante la recolección: {e}")
            self.collection_stats['errors'] += 1

    def show_stats(self):
        """
        Muestra estadísticas de recolección acumuladas durante la ejecución.

        Útil para evaluar el rendimiento y resultados del recolector
        cuando se detiene el programa.
        """
        duration = datetime.now() - self.collection_stats['start_time']
        logging.info("=" * 50)
        logging.info("Estadísticas de recolección:")
        logging.info(f"Tiempo de ejecución: {duration}")
        logging.info(f"Recolecciones realizadas: {self.collection_stats['collections_made']}")
        logging.info(f"Total de archivos guardados: {self.collection_stats['total_files']}")
        logging.info(f"Errores: {self.collection_stats['errors']}")
        logging.info("=" * 50)

    def schedule_collection(self, interval_minutes=120):
        """
        Programa recolecciones periódicas a intervalos regulares.

        Args:
            interval_minutes (int): Intervalo en minutos entre recolecciones (por defecto: 120)
        """
        schedule.every(interval_minutes).minutes.do(self.collect_data)
        logging.info(f"Recolección programada cada {interval_minutes} minutos")


def main():
    """
    Función principal que inicia y mantiene en funcionamiento el recolector.

    Crea una instancia del recolector, ejecuta la primera recolección
    y mantiene el programa en ejecución para recolecciones periódicas.
    """
    try:
        # Inicializar el recolector
        collector = BiciMADCollector()

        # Ejecutar primera recolección inmediatamente
        collector.collect_data()

        # Programar recolecciones periódicas (configurable desde .env)
        # Si no se especifica en .env, usa 120 minutos por defecto
        interval = int(os.getenv('COLLECTION_INTERVAL_MINUTES', 120))
        collector.schedule_collection(interval)

        # Mantener el programa en ejecución
        logging.info(f"Recolector iniciado. Presiona Ctrl+C para detener.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Recolector detenido por el usuario")
            collector.show_stats()

    except Exception as e:
        logging.error(f"Error crítico: {e}")


if __name__ == "__main__":
    main()

# Cómo modificar el intervalo de recolección:
#
# 1. Mediante el archivo .env:
#    Crea o edita el archivo .env y añade la siguiente línea:
#    COLLECTION_INTERVAL_MINUTES=30
#    (Reemplaza 30 con el número de minutos deseado)
#
# 2. Modificando directamente el código:
#    Busca la línea: interval = int(os.getenv('COLLECTION_INTERVAL_MINUTES', 120))
#    Cambia el valor 120 por el intervalo en minutos que desees usar por defecto
#
# 3. Al ejecutar el script desde la línea de comandos:
#    En Linux/Mac: COLLECTION_INTERVAL_MINUTES=45 python bicimad_collector.py
#    En Windows (PowerShell): $env:COLLECTION_INTERVAL_MINUTES=45; python bicimad_collector.py