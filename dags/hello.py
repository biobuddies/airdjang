"""Compare Jinja templating of DAG params via literal strings and attribute access."""

from dataclasses import dataclass, replace
from typing import Any, Self

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from pendulum import datetime


@dataclass
class Curly:
    """Build `{{params.user.name}}` style Jinja expressions with Python attribute access."""

    source: str

    def __getattr__(self, attribute: str) -> Self:  # noqa: D105 self-documenting
        return replace(self, source=f'{self.source}.{attribute}')

    def __getitem__(self, key: Any) -> Self:
        """Append `.key` for strings, `["key"]` otherwise."""
        # TODO check for funny characters
        if isinstance(key, str):
            return getattr(self, key)
        return replace(self, source=f'{self.source}["{key}"]')

    def __repr__(self) -> str:  # noqa: D105 self-documenting
        return '{{%s}}' % self.source


def greet(name: str) -> None:
    print(f'Hello, {name}!')  # noqa: T201 task log


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
