from pendulum import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from typing import Any


class Curly:
    def __init__(self, source: str):
        self.source = source

    def __getattr__(self, attribute: str):
        return Curly(f'{self.source}.{attribute}')

    def __getitem__(self, key: Any):
        # TODO check for funny characters
        if isinstance(key, str):
            return getattr(self, key)
        return Curly(f'{self.source}["{key}"]')

    def __repr__(self):
        return '{{%s}}' % self.source


def greet(name: str):
    print(f'Hello, {name}!')


with DAG(
    'hello_dag',
    params={'user': Param({'id': 1, 'name': 'root'}, type='object')},
    schedule=None,
    start_date=datetime(2021, 1, 1),
) as dag:
    curly_greeting = PythonOperator(
        task_id='curly_greeting', op_kwargs={'name': '{{params.user.name}}'}, python_callable=greet
    )
    dotted_greeting = PythonOperator(
        task_id='dotted_greeting',
        op_kwargs={'name': str(Curly('params').user.name)},
        python_callable=greet,
    )

    curly_greeting >> dotted_greeting
