import unittest

from airflow.contrib.sensors.external_task_sensor_basic import ExternalTaskSensorBasic
from airflow.operators.dummy_operator import DummyOperator
from airflow import configuration, DAG
from airflow.models import DagBag, TaskInstance
from datetime import timedelta
from mock import MagicMock
from airflow.utils.timezone import datetime


DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'
TEST_TASK_ID = 'time_sensor_check'
DEV_NULL = '/dev/null'


class TestExternalTaskSensorBasic(unittest.TestCase):
    """
    using built test config in airflow package to fill in missing parts of task params
    context dict is meant to simulate context that airflow passes to poke by default
    mocking out the airflow meta store query to return ints as needed for testing via session param
    """

    def setUp(self):
        configuration.load_test_config()
        self.dagbag = DagBag(
            dag_folder=DEV_NULL,
            include_examples=True
        )
        self.args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

    # ==================== #
    # ==== UNIT TESTS ==== #
    # ==================== #

    def test_poke_false(self):
        context = {
            'execution_date': datetime(2020, 1, 1, 0, 0)
        }
        session = MagicMock()
        session.query().filter().count.return_value = 0
        test_task = ExternalTaskSensorBasic(task_id='test_task',
                                            external_dag_id=TEST_DAG_ID,
                                            external_task_id=TEST_TASK_ID,
                                            allowed_states=['success'],
                                            execution_delta=timedelta(days=1)
                                            )
        self.assertFalse(test_task.poke(context, session))

    def test_poke_true(self):
        context = {
            'execution_date': datetime(2020, 1, 1, 0, 0)
        }
        session = MagicMock()
        session.query().filter().count.return_value = 1
        test_task = ExternalTaskSensorBasic(task_id='test_task',
                                            external_dag_id=TEST_DAG_ID,
                                            external_task_id=TEST_TASK_ID,
                                            allowed_states=['success'],
                                            execution_delta=timedelta(days=1)
                                            )
        self.assertTrue(test_task.poke(context, session))

    def test_poke_true_no_delta(self):
        context = {
            'execution_date': datetime(2020, 1, 1, 0, 0)
        }
        session = MagicMock()
        session.query().filter().count.return_value = 1
        test_task = ExternalTaskSensorBasic(task_id='test_task',
                                            external_dag_id=TEST_DAG_ID,
                                            external_task_id=TEST_TASK_ID,
                                            allowed_states=['success']
                                            )
        self.assertTrue(test_task.poke(context, session))

    # ========================== #
    # ==== FUNCTIONAL TESTS ==== #
    # ========================== #

    def test_dummy_operator(self):
        d = DummyOperator(
            task_id=TEST_TASK_ID,
            dag=self.dag
        )
        d.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_external_task_sensor_basic(self):
        self.test_dummy_operator()
        t = ExternalTaskSensorBasic(
            task_id='test_external_task_sensor_check',
            external_dag_id=TEST_DAG_ID,
            external_task_id=TEST_TASK_ID,
            dag=self.dag
        )
        t.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True
        )

    def test_templated_sensor_basic(self):

        with self.dag:
            sensor = ExternalTaskSensorBasic(
                task_id='templated_task',
                external_dag_id='dag_{{ ds }}',
                external_task_id='task_{{ ds }}',
                start_date=DEFAULT_DATE
            )

        instance = TaskInstance(sensor, DEFAULT_DATE)
        instance.render_templates()

        self.assertEqual(sensor.external_dag_id,
                         "dag_{}".format(DEFAULT_DATE.date()))
        self.assertEqual(sensor.external_task_id,
                         "task_{}".format(DEFAULT_DATE.date()))


if __name__ == '__main__':
    unittest.main()
