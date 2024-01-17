from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import psycopg2
from airflow.providers.postgres.operators.postgres import PostgresOperator
from credentials import FOOTBALL_API_TOKEN, HOST_REDSHIFT, USUARIO_REDSHIFT, CONTRASEÑA_REDSHIFT, PUERTO_REDSHIFT, BASEDEDATOS_REDSHIFT

default_args = {
    'owner': 'NachoSachetto',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

# Parte de la entrega N°1 donde se verifica el acceso y la respuesta por parte de la API

def verificar_accesos():
    token = FOOTBALL_API_TOKEN
    base_url = 'https://api.football-data.org/v4/'
    url_team_random = f'{base_url}teams/99'
    response_team_data = requests.get(url_team_random, headers={'X-Auth-Token': token}, timeout=30)
    if response_team_data.status_code == 200:
        print("Validación Correcto")
    else:
        print('Error al obtener los partidos:', response_team_data.status_code)

def obtener_parametros_conexion():
    return {
        'dbname': BASEDEDATOS_REDSHIFT,
        'host': HOST_REDSHIFT,
        'port': PUERTO_REDSHIFT,
        'user': USUARIO_REDSHIFT,
        'password': CONTRASEÑA_REDSHIFT
    }

def conectar_redshift():
    conn = psycopg2.connect(
        dbname=BASEDEDATOS_REDSHIFT,
        host=HOST_REDSHIFT,
        port=PUERTO_REDSHIFT,
        user=USUARIO_REDSHIFT,
        password=CONTRASEÑA_REDSHIFT
    )
    return conn.cursor()


def cerrar_conexion(conn, cur):
    if conn:
        cur.close()
        conn.close()

def obtener_datos_partidos(date_from, date_to):
    url_matches = 'https://api.football-data.org/v4/matches'
    competition_codes = ['PL', 'BL1', 'SA', 'PD', 'FL1']
    dataframes = []

    for code in competition_codes:
        params = {
            'dateFrom': date_from,
            'dateTo': date_to,
            'competitions': code
        }
        df = get_matches_data(url_matches, params)
        if df is not None:
            dataframes.append(df)

    return dataframes

def cargar_datos_redshift(**kwargs):
    date_from = kwargs['logical_date']
    date_to = date_from + timedelta(days=1)

    dataframes = obtener_datos_partidos(date_from, date_to)

    conn, cur = conectar_redshift()

    if conn:
        try:
            for df in dataframes:
                column_names = df.columns.tolist()
                for index, row in df.iterrows():
                    fecha_partido = datetime.strptime(row['Fecha Partido'], '%Y-%m-%dT%H:%M:%SZ').date()
                    competicion = str(row['Competición'])
                    estado = str(row['Estado'])
                    etapa = str(row['Etapa'])
                    jornada = int(row['Jornada'])
                    equipo_local = str(row['Equipo Local'])
                    equipo_visitante = str(row['Equipo Visitante'])
                    arbitro = str(row['Árbitro'])
                    resultado = str(row['Resultado'])

                    cur.execute("""
                        SELECT COUNT(*) FROM Partidos
                        WHERE fecha_partido = %s AND competicion = %s AND equipo_local = %s AND equipo_visitante = %s
                    """, (
                        fecha_partido, competicion, equipo_local, equipo_visitante
                    ))

                    cur.execute("""
                        INSERT INTO Partidos (fecha_partido, competicion, estado, etapa, jornada, equipo_local, equipo_visitante, arbitro, resultado)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        fecha_partido, competicion, estado, etapa, jornada, equipo_local, equipo_visitante, arbitro, resultado
                    ))

            conn.commit()
            print("Datos cargados correctamente en Redshift")

        except Exception as e:
            print("Error al cargar los datos:", str(e))
            conn.rollback()

        finally:
            cerrar_conexion(conn, cur)

dag = DAG(
    'dag_redshift',
    default_args=default_args,
    description='DAG para cargar datos en Redshift',
    schedule_interval='@daily',  # Para correr diariamente
    start_date=datetime(2023, 12, 20),
)

with dag:
    tarea_verificar_accesos = PythonOperator(
        task_id='verificar_accesos',
        python_callable=verificar_accesos,
    )

    tarea_conectar_redshift = PythonOperator(
        task_id='conectar_redshift',
        python_callable=conectar_redshift,
    )

    tarea_cargar_datos_redshift = PythonOperator(
        task_id='cargar_datos_redshift',
        python_callable=cargar_datos_redshift,
        provide_context=True,
    )

    tarea_verificar_accesos >> tarea_conectar_redshift >> tarea_cargar_datos_redshift
