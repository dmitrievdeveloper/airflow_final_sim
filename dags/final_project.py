from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
import logging
import pandas as pd
from io import StringIO


# Конфигурация таблиц
config = [
    {
        'table_name': 'test',
        'table_ddl': 'CREATE TABLE IF NOT EXISTS test (id BIGINT, category VARCHAR(255), amount DECIMAL(10,2));',
        'table_dml': '''
            SELECT 
                id,
                category,
                SUM(amount) AS amount
            FROM raw_table
            GROUP BY id, category;
        ''',
        'need_to_export': True,
    },
    {
        'table_name': 'test_2',
        'table_ddl': 'CREATE TABLE IF NOT EXISTS test_2 (id BIGINT, category VARCHAR(255), amount DECIMAL(10,2));',
        'table_dml': '''
            SELECT
                id,
                category,
                SUM(amount) AS amount
            FROM raw_table_2
            GROUP BY id, category;
        ''',
        'need_to_export': False,
    }
    # ... дополнительные таблицы
]

config.append({
    'table_name': 'new_table',
    'table_ddl': 'CREATE TABLE IF NOT EXISTS new_table (id BIGINT, new_category VARCHAR(255), new_amount DECIMAL(10,2));',
        'table_dml': '''
            SELECT 
                id,
                category,
                SUM(amount) AS new_amount
            FROM raw_table
            GROUP BY id, category;
        ''',
        'need_to_export': True,
})

# Параметры DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def export_to_minio(table_name: str, **kwargs):
    """
    Функция для экспорта данных из таблицы в Minio S3 в формате CSV.
    """
    try:
        # Получаем соединение с БД через хук Airflow
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Читаем данные
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

        if df.empty:
            logging.info(f"Таблица {table_name} пуста, экспорт пропущен.")
            return

        # Сохраняем в CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Загружаем в Minio
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=f'exports/{table_name}.csv',
            bucket_name='data-exports',
            replace=True
        )
        logging.info(f"Данные из {table_name} успешно экспортированы в Minio")
    except Exception as e:
        logging.error(f"Ошибка при экспорте {table_name}: {e}")
        raise

# Создаём DAG
with DAG(
    dag_id='dynamic_table_generation',
    default_args=default_args,
    description='Динамическое создание таблиц и задач на основе конфигурации',
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    # Группа задач для каждой таблицы
    for table_config in config:
        table_name = table_config['table_name']


        with TaskGroup(group_id=f'process_{table_name}') as table_group:
            # Задача создания таблицы
            create_table = PostgresOperator(
                task_id=f'create_{table_name}',
                sql=table_config['table_ddl'],
                postgres_conn_id='postgres_default'
            )

            # Задача наполнения таблицы данымин
            load_data = PostgresOperator(
                task_id=f'load_{table_name}',
                sql=f"DELETE FROM {table_name}; INSERT INTO {table_name} {table_config['table_dml']}",
                postgres_conn_id='postgres_default'
            )

            # Условная задача экспорта в Minio
            if table_config['need_to_export']:
                export_task = PythonOperator(
                    task_id=f'export_{table_name}_to_minio',
                    python_callable=export_to_minio,
                    op_kwargs={'table_name': table_name},
                    provide_context=True
                )
                # Связываем задачи
                create_table >> load_data >> export_task
            else:
                # Если экспорт не нужен, просто связываем создание и наполнение
                create_table >> load_data

        # Добавляем группу задач в DAG
        table_group