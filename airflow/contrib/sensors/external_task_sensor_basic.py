# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.models import TaskInstance
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from datetime import timedelta


class ExternalTaskSensorBasic(BaseSensorOperator):
    """
    Waits for a task to complete in a different DAG based on last success greater than or equal to time delta

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: time difference from the current execution date
        look at.  If dependent task execution date is greater than or equal to
        resulting time delta return True.
        For yesterday, use [positive!] datetime.timedelta(days=1).
    :type execution_delta: datetime.timedelta
    """
    template_fields = ['external_dag_id', 'external_task_id']
    ui_color = '#19647e'

    @apply_defaults
    def __init__(self,
                 external_dag_id,
                 external_task_id,
                 allowed_states=None,
                 execution_delta=None,
                 *args,
                 **kwargs):
        super(ExternalTaskSensorBasic, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.execution_delta = execution_delta
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        else:
            dttm = context['execution_date'] - timedelta(days=1)

        self.log.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} for execution dates >= '
            '{} ... '.format(dttm.isoformat(), **locals()))
        TI = TaskInstance

        count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state.in_(self.allowed_states),
            TI.execution_date >= dttm,
            ).count()
        session.commit()
        return count >= 1
