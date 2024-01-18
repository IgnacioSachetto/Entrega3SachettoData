from datetime import datetime, timedelta
import requests
import psycopg2
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from credentials import FOOTBALL_API_TOKEN, HOST_REDSHIFT, USUARIO_REDSHIFT, CONTRASEÑA_REDSHIFT, PUERTO_REDSHIFT, BASEDEDATOS_REDSHIFT
from psycopg2.extras import execute_values


url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base = 'data-engineer-database'
user = 'nachosachetto1998_coderhouse'
pwd = '7obT4RE7uI'

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

default_args = {
    'owner': 'NacchoSachetto',
    'start_date': datetime(2023, 12, 20),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

futbol_dag = DAG(
    dag_id='ETLFutbolll',
    default_args=default_args,
    description='Agrega datos de los partidos de las principales ligas del mundo diariamente.',
    schedule_interval="@daily",
    catchup=False
)


def conexion_redshift(exec_date, **kwargs):
    logging.info(f"Conectandose a la BD en la fecha: {exec_date}")
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        logging.info("Connected to Redshift successfully!")
    except Exception as e:
        logging.error("Unable to connect to Redshift.")
        logging.error(e)



def verificar_respuesta_api(exec_date, **kwargs):
    base_url = 'https://api.football-data.org/v4/'
    url_team_random = f'{base_url}teams/99'
    response_team_data = requests.get(url_team_random, headers={'X-Auth-Token': '08b983fb988b4db0a2ea9e21d1e15f51'})

    if response_team_data.status_code == 200:
        data_team = response_team_data.json()
        logging.info(data_team)
    else:
        logging.error('Error al obtener los partidos')


def extraccion(**kwargs):
    url_matches = 'https://api.football-data.org/v4/matches'
    competition_codes = ['PL', 'BL1', 'SA', 'PD', 'FL1']
    dataframes = []

    date_from = '2023-12-01'
    date_to = '2023-12-10'

    try:
        for code in competition_codes:
            params = {
                'dateFrom': date_from,
                'dateTo': date_to,
                'competitions': code
            }
            response = requests.get(url_matches, params=params, headers={'X-Auth-Token': '08b983fb988b4db0a2ea9e21d1e15f51'})

            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data['matches'])
                df['fecha_partido'] = df['utcDate']
                df['competicion'] = df['competition'].apply(lambda x: x['name'])
                df['estado'] = df['status']
                df['etapa'] = df['stage']
                df['jornada'] = df['matchday']
                df['equipo_local'] = df['homeTeam'].apply(lambda x: x['name'])
                df['equipo_visitante'] = df['awayTeam'].apply(lambda x: x['name'])
                df['arbitro'] = df['referees'].apply(lambda x: x[0]['name'] if x else None)
                df['resultado'] = df['score'].apply(lambda x: f"{x['fullTime']['home']} - {x['fullTime']['away']}" if x['fullTime'] else None)

                logging.info(f'Data obtenida: {df.shape[0]} partidos recuperados.')
                dataframes.append(df)
            else:
                logging.error(f'Error al obtener partidos: {response.status_code}')

        combined_df = pd.concat(dataframes, ignore_index=True)

        xcom_value = combined_df.to_dict(orient='records')

        return xcom_value
    except Exception as e:
        logging.error(f'Error during data extraction: {e}')
        raise e





from datetime import datetime
import psycopg2
import logging
from psycopg2.extras import execute_values

def cargar_datos_redshift(exec_date, **kwargs):

    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')


    xcom_value = kwargs.get('task_instance').xcom_pull(task_ids='extraccion_datos')

    if xcom_value is None or not xcom_value:
        logging.error("No se recibieron datos válidos desde la tarea de extracción.")
        return

    cur = conn.cursor()

    try:
        table_name = 'Partidos'
        columns = ['fecha_partido', 'competicion', 'estado',
                   'etapa', 'jornada', 'equipo_local',
                   'equipo_visitante', 'arbitro', 'resultado']

        values = [tuple(row[column] for column in columns) for row in xcom_value]


        insert_sql = 'INSERT INTO "{}" ({}) VALUES %s'.format(table_name, ', '.join('"' + col + '"' for col in columns))

        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")

        logging.info("Datos cargados correctamente en Redshift")

    except Exception as e:
        logging.error(f"Error al cargar los datos: {str(e)}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()

        logging.info("Finalizado")


task_1_conexion = PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=futbol_dag,
)

task_2_api = PythonOperator(
    task_id='respuesta_api',
    python_callable=verificar_respuesta_api,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=futbol_dag,
)

task_3_extraccion = PythonOperator(
    task_id='extraccion_datos',
    python_callable=extraccion,
    provide_context=True,
    dag=futbol_dag,
)

cargar_redshift_task = PythonOperator(
    task_id='cargar_redshift_task',
    python_callable=cargar_datos_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    provide_context=True,
    dag=futbol_dag,
)

task_1_conexion >> task_2_api >> task_3_extraccion >> cargar_redshift_task
