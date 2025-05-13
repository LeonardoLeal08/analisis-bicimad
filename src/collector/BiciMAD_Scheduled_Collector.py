"""
BiciMAD Scheduled Collector - Programador de recolecciones automáticas

Este script complementa al recolector BiciMAD_Data_Collector.py, proporcionando una planificación
automatizada para ejecutar recolecciones de datos dentro de un período de tiempo específico.
En lugar de recolectar datos continuamente sin parar, este script permite:

- Programar una fecha y hora de inicio
- Establecer una fecha y hora de finalización
- Definir el intervalo exacto entre recolecciones
- Ejecutar recolecciones de forma autónoma durante el período especificado

Este enfoque es ideal para estudios temporales sobre el uso de BiciMAD, permitiendo recopilar
datos durante días, semanas o meses sin intervención manual.

Configuración del intervalo de recolección:
- Por defecto: 120 minutos (2 horas)
- Para cambiar:
  1. Modificar directamente: interval_seconds = X * 60 (donde X = minutos)
  2. A través del archivo .env: COLLECTION_INTERVAL_MINUTES=X

Ejemplo de contenido del archivo .env:
  BICIMAD_ACCESS_TOKEN=tu_token_aquí
  COLLECTION_INTERVAL_MINUTES=60

Autor: Leonardo Leal Vivas
Fecha: [Creación: 07/04/2025 | Modificación: 29/04/2025]
"""

import time
import datetime
import logging
import os
from pathlib import Path
from BiciMAD_Data_Collector import BiciMADCollector  # Importar la clase
from dotenv import load_dotenv  # Añadido para cargar variables de entorno

# Cargar variables de entorno desde archivo .env
load_dotenv()

# Configurar logging
log_dir = Path("scheduled_run")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "scheduled_execution.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


def main():
    """
    Función principal que configura y ejecuta el programa de recolección programada.

    Esta función:
    1. Define los tiempos de inicio y finalización para las recolecciones
    2. Configura el intervalo entre recolecciones
    3. Espera hasta el momento de inicio si es necesario
    4. Ejecuta recolecciones periódicas hasta la fecha de finalización
    5. Maneja el tiempo de espera entre recolecciones
    """
    # Definir tiempos de inicio y fin
    now = datetime.datetime.now()

    # Por defecto: comenzar mañana a las 9:00 AM
    tomorrow = now.date() + datetime.timedelta(days=1)
    start_time = datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 9, 0, 0)

    # Fecha de finalización: 15 de abril de 2025 a las 9:00 AM
    end_time = datetime.datetime(2025, 4, 15, 9, 0, 0)

    # Configurar el intervalo entre recolecciones
    # Primero intentamos obtener el valor desde una variable de entorno
    interval_minutes = int(os.getenv('COLLECTION_INTERVAL_MINUTES', 120))
    interval_seconds = interval_minutes * 60

    # Mostrar la configuración inicial
    logging.info(f"Iniciando programa de recolección programada")
    logging.info(f"Inicio: {start_time}")
    logging.info(f"Fin: {end_time}")
    logging.info(f"Intervalo: {interval_minutes} minutos")

    # Esperar hasta la hora de inicio si es necesario
    now = datetime.datetime.now()
    if now < start_time:
        wait_seconds = (start_time - now).total_seconds()
        logging.info(f"Esperando {wait_seconds / 60:.1f} minutos hasta la hora de inicio ({start_time})")
        time.sleep(wait_seconds)

    # Ejecutar recolecciones hasta la fecha de finalización
    next_run = datetime.datetime.now()

    while next_run <= end_time:
        logging.info("Iniciando Proceso de recolección de datos")

        # Ejecutar la recolección directamente
        try:
            # Crear una nueva instancia del recolector para cada ejecución
            collector = BiciMADCollector()
            collector.collect_data()
            logging.info("Proceso de recolección de datos completado")
        except Exception as e:
            logging.error(f"Error durante la recolección: {e}")

        # Calcular próxima ejecución
        next_run = datetime.datetime.now() + datetime.timedelta(seconds=interval_seconds)

        # Si la próxima ejecución está después del tiempo de finalización, terminar
        if next_run > end_time:
            logging.info("Próxima ejecución excede el tiempo de finalización. Terminando programa.")
            break

        # Calcular tiempo de espera hasta la próxima ejecución
        wait_seconds = (next_run - datetime.datetime.now()).total_seconds()
        if wait_seconds > 0:
            logging.info(f"Esperando {wait_seconds / 60:.1f} minutos hasta la próxima recolección")
            logging.info(f"Próxima recolección programada para: {next_run}")
            time.sleep(wait_seconds)

    logging.info("Programa de recolección finalizado según lo programado")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Programa detenido manualmente por el usuario")
    except Exception as e:
        logging.error(f"Error crítico en el programa de recolección: {e}")

# Cómo modificar el intervalo de recolección:
#
# 1. Mediante el archivo .env:
#    Crea o edita el archivo .env y añade la siguiente línea:
#    COLLECTION_INTERVAL_MINUTES=60
#    (Reemplaza 60 con el número de minutos deseado entre cada recolección)
#
# 2. Modificando directamente el código:
#    Busca la línea: interval_minutes = int(os.getenv('COLLECTION_INTERVAL_MINUTES', 120))
#    Cambia el valor 120 por el intervalo en minutos que desees usar por defecto
#
# 3. Al ejecutar el script desde la línea de comandos:
#    En Linux/Mac: COLLECTION_INTERVAL_MINUTES=45 python
#    En Windows (PowerShell): $env:COLLECTION_INTERVAL_MINUTES=45; python BiciMAD_Scheduled_Collector.py
#
# Cómo modificar las fechas de inicio y fin:
#
# - Para cambiar la fecha de inicio:
#   start_time = datetime.datetime(año, mes, día, hora, minuto, segundo)
#
# - Para cambiar la fecha de finalización:
#   end_time = datetime.datetime(año, mes, día, hora, minuto, segundo)
#
# - Para iniciar inmediatamente:
#   start_time = datetime.datetime.now()