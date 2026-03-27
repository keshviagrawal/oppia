# coding: utf-8
#
# Copyright 2026 The Oppia Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Jobs to audit and fix created_on in LearnerPlaylistModel and
CompletedActivitiesModel.

The created_on field in these models may have incorrect values due to a
historical bug in how timestamps were saved (similar to issue #10386 for
UserSettingsModel). These jobs cross-reference timestamps from related
user models to find the earliest valid date and correct the created_on
field if it is later than the earliest known timestamp for that user.
"""

from __future__ import annotations

import datetime

from core.jobs import base_jobs
from core.jobs.io import ndb_io
from core.jobs.transforms import job_result_transforms
from core.jobs.types import job_run_result
from core.platform import models

import apache_beam as beam
from typing import Dict, List, Optional, Tuple

MYPY = False
if MYPY:  # pragma: no cover
    from mypy_imports import datastore_services, user_models

(user_models,) = models.Registry.import_models([models.Names.USER])

datastore_services = models.Registry.import_datastore_services()

# Threshold for considering created_on as incorrect. If the earliest
# known timestamp for a user is more than this duration before the
# model's created_on, the created_on is considered wrong.
CREATED_ON_THRESHOLD = datetime.timedelta(minutes=5)


def _get_earliest_timestamp_from_user_settings(
    model: user_models.UserSettingsModel,
) -> Optional[datetime.datetime]:
    """Returns the earliest datetime field from a UserSettingsModel.

    Args:
        model: UserSettingsModel. The user settings model.

    Returns:
        datetime.datetime or None. The earliest datetime found.
    """
    dates: List[datetime.datetime] = []
    if model.created_on is not None:
        dates.append(model.created_on)
    if model.last_updated is not None:
        dates.append(model.last_updated)
    if model.last_agreed_to_terms is not None:
        dates.append(model.last_agreed_to_terms)
    if model.last_logged_in is not None:
        dates.append(model.last_logged_in)
    if model.last_started_state_editor_tutorial is not None:
        dates.append(model.last_started_state_editor_tutorial)
    if model.last_started_state_translation_tutorial is not None:
        dates.append(model.last_started_state_translation_tutorial)
    if model.last_edited_an_exploration is not None:
        dates.append(model.last_edited_an_exploration)
    if model.last_created_an_exploration is not None:
        dates.append(model.last_created_an_exploration)
    if model.first_contribution_msec is not None:
        dates.append(
            datetime.datetime.utcfromtimestamp(
                model.first_contribution_msec / 1000.0
            )
        )
    return min(dates) if dates else None


def _get_earliest_timestamp_from_model(
    model: user_models.UserSubscriptionsModel,
) -> Optional[datetime.datetime]:
    """Returns the earliest of created_on and last_updated from a model.

    Args:
        model: BaseModel. A model with created_on and last_updated.

    Returns:
        datetime.datetime or None. The earliest datetime found.
    """
    dates: List[datetime.datetime] = []
    if model.created_on is not None:
        dates.append(model.created_on)
    if model.last_updated is not None:
        dates.append(model.last_updated)
    return min(dates) if dates else None


class AuditCreatedOnForLearnerModelsJob(base_jobs.JobBase):
    """Audit job that identifies LearnerPlaylistModel and
    CompletedActivitiesModel instances whose created_on is later than
    the earliest known timestamp for that user.
    """

    def run(self) -> beam.PCollection[job_run_result.JobRunResult]:
        """Returns a PCollection of audit results.

        Returns:
            PCollection. A PCollection of JobRunResult instances.
        """
        # Collect earliest timestamps from UserSettingsModel.
        user_settings_timestamps = (
            self.pipeline
            | 'Get all UserSettingsModels'
            >> ndb_io.GetModels(
                user_models.UserSettingsModel.get_all(include_deleted=False)
            )
            | 'Extract earliest timestamp from UserSettingsModel'
            >> beam.Map(
                lambda m: (
                    m.id,
                    _get_earliest_timestamp_from_user_settings(m),
                )
            )
            | 'Filter out None user settings timestamps'
            >> beam.Filter(lambda item: item[1] is not None)
        )

        # Collect earliest timestamps from UserContributionsModel.
        user_contributions_timestamps = (
            self.pipeline
            | 'Get all UserContributionsModels'
            >> ndb_io.GetModels(
                user_models.UserContributionsModel.get_all(
                    include_deleted=False
                )
            )
            | 'Extract earliest timestamp from UserContributionsModel'
            >> beam.Map(lambda m: (m.id, _get_earliest_timestamp_from_model(m)))
            | 'Filter out None contributions timestamps'
            >> beam.Filter(lambda item: item[1] is not None)
        )

        # Collect earliest timestamps from UserSubscriptionsModel.
        user_subscriptions_timestamps = (
            self.pipeline
            | 'Get all UserSubscriptionsModels'
            >> ndb_io.GetModels(
                user_models.UserSubscriptionsModel.get_all(
                    include_deleted=False
                )
            )
            | 'Extract earliest timestamp from UserSubscriptionsModel'
            >> beam.Map(lambda m: (m.id, _get_earliest_timestamp_from_model(m)))
            | 'Filter out None subscriptions timestamps'
            >> beam.Filter(lambda item: item[1] is not None)
        )

        # Merge all timestamps and find the minimum per user.
        earliest_per_user = (
            (
                user_settings_timestamps,
                user_contributions_timestamps,
                user_subscriptions_timestamps,
            )
            | 'Flatten all timestamps' >> beam.Flatten()
            | 'Group timestamps by user_id' >> beam.GroupByKey()
            | 'Find minimum timestamp per user'
            >> beam.MapTuple(
                lambda user_id, timestamps: (user_id, min(timestamps))
            )
        )

        # Check CompletedActivitiesModel.
        completed_models = (
            self.pipeline
            | 'Get all CompletedActivitiesModels'
            >> ndb_io.GetModels(
                user_models.CompletedActivitiesModel.get_all(
                    include_deleted=False
                )
            )
            | 'Key CompletedActivitiesModel by user_id'
            >> beam.Map(lambda m: (m.id, m.created_on))
        )

        completed_invalid = (
            {
                'model': completed_models,
                'earliest': earliest_per_user,
            }
            | 'CoGroup CompletedActivities' >> beam.CoGroupByKey()
            | 'Find invalid CompletedActivities'
            >> beam.FlatMap(
                self._find_invalid_created_on, 'CompletedActivitiesModel'
            )
        )

        # Check LearnerPlaylistModel.
        playlist_models = (
            self.pipeline
            | 'Get all LearnerPlaylistModels'
            >> ndb_io.GetModels(
                user_models.LearnerPlaylistModel.get_all(include_deleted=False)
            )
            | 'Key LearnerPlaylistModel by user_id'
            >> beam.Map(lambda m: (m.id, m.created_on))
        )

        playlist_invalid = (
            {
                'model': playlist_models,
                'earliest': earliest_per_user,
            }
            | 'CoGroup LearnerPlaylist' >> beam.CoGroupByKey()
            | 'Find invalid LearnerPlaylist'
            >> beam.FlatMap(
                self._find_invalid_created_on, 'LearnerPlaylistModel'
            )
        )

        invalid_reports = (
            completed_invalid,
            playlist_invalid,
        ) | 'Flatten invalid reports' >> beam.Flatten()

        count_report = (
            invalid_reports
            | 'Count invalid models'
            >> job_result_transforms.CountObjectsToJobRunResult(
                'INVALID CREATED_ON'
            )
        )

        return (
            invalid_reports,
            count_report,
        ) | 'Combine audit results' >> beam.Flatten()

    @staticmethod
    def _find_invalid_created_on(
        item: Tuple[str, Dict[str, List[datetime.datetime]]],
        model_name: str,
    ) -> List[job_run_result.JobRunResult]:
        """Checks if a model's created_on is invalid.

        Args:
            item: tuple. A tuple of (user_id, grouped_data) from
                CoGroupByKey.
            model_name: str. Name of the model being checked.

        Returns:
            list(JobRunResult). A list with one result if invalid,
            empty otherwise.
        """
        user_id, grouped = item
        model_dates = list(grouped['model'])
        earliest_dates = list(grouped['earliest'])

        if not model_dates or not earliest_dates:
            return []

        model_created_on = model_dates[0]
        earliest = earliest_dates[0]

        if model_created_on - earliest > CREATED_ON_THRESHOLD:
            return [
                job_run_result.JobRunResult.as_stderr(
                    '%s with id %s has created_on %s but earliest '
                    'known timestamp is %s'
                    % (model_name, user_id, model_created_on, earliest)
                )
            ]
        return []


class FixCreatedOnForLearnerModelsJob(base_jobs.JobBase):
    """Migration job that fixes created_on in LearnerPlaylistModel and
    CompletedActivitiesModel by setting it to the earliest known
    timestamp for each user.
    """

    DATASTORE_UPDATES_ALLOWED = True

    def run(self) -> beam.PCollection[job_run_result.JobRunResult]:
        """Returns a PCollection of migration results.

        Returns:
            PCollection. A PCollection of JobRunResult instances.
        """
        # Collect earliest timestamps from UserSettingsModel.
        user_settings_timestamps = (
            self.pipeline
            | 'Get all UserSettingsModels'
            >> ndb_io.GetModels(
                user_models.UserSettingsModel.get_all(include_deleted=False)
            )
            | 'Extract earliest timestamp from UserSettingsModel'
            >> beam.Map(
                lambda m: (
                    m.id,
                    _get_earliest_timestamp_from_user_settings(m),
                )
            )
            | 'Filter out None user settings timestamps'
            >> beam.Filter(lambda item: item[1] is not None)
        )

        # Collect earliest timestamps from UserContributionsModel.
        user_contributions_timestamps = (
            self.pipeline
            | 'Get all UserContributionsModels'
            >> ndb_io.GetModels(
                user_models.UserContributionsModel.get_all(
                    include_deleted=False
                )
            )
            | 'Extract earliest timestamp from UserContributionsModel'
            >> beam.Map(lambda m: (m.id, _get_earliest_timestamp_from_model(m)))
            | 'Filter out None contributions timestamps'
            >> beam.Filter(lambda item: item[1] is not None)
        )

        # Collect earliest timestamps from UserSubscriptionsModel.
        user_subscriptions_timestamps = (
            self.pipeline
            | 'Get all UserSubscriptionsModels'
            >> ndb_io.GetModels(
                user_models.UserSubscriptionsModel.get_all(
                    include_deleted=False
                )
            )
            | 'Extract earliest timestamp from UserSubscriptionsModel'
            >> beam.Map(lambda m: (m.id, _get_earliest_timestamp_from_model(m)))
            | 'Filter out None subscriptions timestamps'
            >> beam.Filter(lambda item: item[1] is not None)
        )

        # Merge all timestamps and find the minimum per user.
        earliest_per_user = (
            (
                user_settings_timestamps,
                user_contributions_timestamps,
                user_subscriptions_timestamps,
            )
            | 'Flatten all timestamps' >> beam.Flatten()
            | 'Group timestamps by user_id' >> beam.GroupByKey()
            | 'Find minimum timestamp per user'
            >> beam.MapTuple(
                lambda user_id, timestamps: (user_id, min(timestamps))
            )
        )

        # Fix CompletedActivitiesModel.
        completed_models = (
            self.pipeline
            | 'Get all CompletedActivitiesModels'
            >> ndb_io.GetModels(
                user_models.CompletedActivitiesModel.get_all(
                    include_deleted=False
                )
            )
            | 'Key CompletedActivitiesModel by user_id'
            >> beam.Map(lambda m: (m.id, m))
        )

        fixed_completed = (
            {
                'model': completed_models,
                'earliest': earliest_per_user,
            }
            | 'CoGroup CompletedActivities' >> beam.CoGroupByKey()
            | 'Fix CompletedActivities created_on'
            >> beam.FlatMap(self._fix_created_on)
        )

        # Fix LearnerPlaylistModel.
        playlist_models = (
            self.pipeline
            | 'Get all LearnerPlaylistModels'
            >> ndb_io.GetModels(
                user_models.LearnerPlaylistModel.get_all(include_deleted=False)
            )
            | 'Key LearnerPlaylistModel by user_id'
            >> beam.Map(lambda m: (m.id, m))
        )

        fixed_playlist = (
            {
                'model': playlist_models,
                'earliest': earliest_per_user,
            }
            | 'CoGroup LearnerPlaylist' >> beam.CoGroupByKey()
            | 'Fix LearnerPlaylist created_on'
            >> beam.FlatMap(self._fix_created_on)
        )

        # Write fixed models back to the datastore.
        models_to_put = (
            fixed_completed,
            fixed_playlist,
        ) | 'Flatten fixed models' >> beam.Flatten()

        if self.DATASTORE_UPDATES_ALLOWED:
            unused_put_result = (
                models_to_put | 'Put fixed models' >> ndb_io.PutModels()
            )

        count_report = (
            models_to_put
            | 'Count fixed models'
            >> job_result_transforms.CountObjectsToJobRunResult(
                'FIXED CREATED_ON'
            )
        )

        detail_report = models_to_put | 'Report fixed models' >> beam.Map(
            lambda m: job_run_result.JobRunResult.as_stdout(
                'FIXED: %s with id %s' % (m.__class__.__name__, m.id)
            )
        )

        return (
            count_report,
            detail_report,
        ) | 'Combine results' >> beam.Flatten()

    @staticmethod
    def _fix_created_on(
        item: Tuple[
            str,
            Dict[str, list],
        ],
    ) -> list:
        """Fixes created_on for a model if it is incorrect.

        Args:
            item: tuple. A tuple of (user_id, grouped_data) from
                CoGroupByKey.

        Returns:
            list. A list with the fixed model, or empty if no fix needed.
        """
        user_id, grouped = item
        model_list = list(grouped['model'])
        earliest_dates = list(grouped['earliest'])

        if not model_list or not earliest_dates:
            return []

        model = model_list[0]
        earliest = earliest_dates[0]

        if model.created_on - earliest > CREATED_ON_THRESHOLD:
            model.created_on = earliest
            model.update_timestamps(update_last_updated_time=False)
            return [model]
        return []
