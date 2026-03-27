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

"""Unit tests for
jobs.batch_jobs.fix_created_on_for_learner_models_jobs.
"""

from __future__ import annotations

import datetime

from core.jobs import job_test_utils
from core.jobs.batch_jobs import fix_created_on_for_learner_models_jobs
from core.jobs.types import job_run_result
from core.platform import models

from typing import Final, Type

MYPY = False
if MYPY:
    from mypy_imports import user_models

(user_models,) = models.Registry.import_models([models.Names.USER])

# Timestamps used across tests.
EARLY_DATE = datetime.datetime(2020, 1, 1)
LATE_DATE = datetime.datetime(2023, 6, 15)
NEAR_DATE = datetime.datetime(2023, 6, 15, 0, 3)  # Within 5-min threshold.


class AuditCreatedOnForLearnerModelsJobTests(job_test_utils.JobTestBase):
    """Tests for AuditCreatedOnForLearnerModelsJob."""

    JOB_CLASS: Type[
        fix_created_on_for_learner_models_jobs.AuditCreatedOnForLearnerModelsJob
    ] = fix_created_on_for_learner_models_jobs.AuditCreatedOnForLearnerModelsJob

    USER_ID_1: Final = 'uid_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'

    def test_empty_storage(self) -> None:
        self.assert_job_output_is_empty()

    def test_valid_created_on_produces_no_output(self) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = EARLY_DATE
        user_settings.last_updated = EARLY_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = EARLY_DATE
        completed.last_updated = EARLY_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, completed])
        self.assert_job_output_is_empty()

    def test_invalid_completed_activities_created_on_is_reported(
        self,
    ) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = EARLY_DATE
        user_settings.last_updated = EARLY_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = LATE_DATE
        completed.last_updated = LATE_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, completed])
        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stderr(
                    'CompletedActivitiesModel with id %s has created_on '
                    '%s but earliest known timestamp is %s'
                    % (self.USER_ID_1, LATE_DATE, EARLY_DATE)
                ),
                job_run_result.JobRunResult.as_stdout(
                    'INVALID CREATED_ON SUCCESS: 1'
                ),
            ]
        )

    def test_invalid_learner_playlist_created_on_is_reported(
        self,
    ) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = EARLY_DATE
        user_settings.last_updated = EARLY_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        playlist = self.create_model(
            user_models.LearnerPlaylistModel,
            id=self.USER_ID_1,
        )
        playlist.created_on = LATE_DATE
        playlist.last_updated = LATE_DATE
        playlist.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, playlist])
        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stderr(
                    'LearnerPlaylistModel with id %s has created_on '
                    '%s but earliest known timestamp is %s'
                    % (self.USER_ID_1, LATE_DATE, EARLY_DATE)
                ),
                job_run_result.JobRunResult.as_stdout(
                    'INVALID CREATED_ON SUCCESS: 1'
                ),
            ]
        )

    def test_created_on_within_threshold_is_not_reported(self) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = LATE_DATE
        user_settings.last_updated = LATE_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = NEAR_DATE
        completed.last_updated = NEAR_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, completed])
        self.assert_job_output_is_empty()


class FixCreatedOnForLearnerModelsJobTests(job_test_utils.JobTestBase):
    """Tests for FixCreatedOnForLearnerModelsJob."""

    JOB_CLASS: Type[
        fix_created_on_for_learner_models_jobs.FixCreatedOnForLearnerModelsJob
    ] = fix_created_on_for_learner_models_jobs.FixCreatedOnForLearnerModelsJob

    USER_ID_1: Final = 'uid_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    USER_ID_2: Final = 'uid_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'

    def test_empty_storage(self) -> None:
        self.assert_job_output_is_empty()

    def test_valid_created_on_is_not_changed(self) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = EARLY_DATE
        user_settings.last_updated = EARLY_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = EARLY_DATE
        completed.last_updated = EARLY_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, completed])
        self.assert_job_output_is_empty()

    def test_invalid_completed_activities_created_on_is_fixed(
        self,
    ) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = EARLY_DATE
        user_settings.last_updated = EARLY_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = LATE_DATE
        completed.last_updated = LATE_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, completed])
        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    'FIXED: CompletedActivitiesModel with id %s'
                    % self.USER_ID_1
                ),
                job_run_result.JobRunResult.as_stdout(
                    'FIXED CREATED_ON SUCCESS: 1'
                ),
            ]
        )

    def test_invalid_learner_playlist_created_on_is_fixed(self) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = EARLY_DATE
        user_settings.last_updated = EARLY_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        playlist = self.create_model(
            user_models.LearnerPlaylistModel,
            id=self.USER_ID_1,
        )
        playlist.created_on = LATE_DATE
        playlist.last_updated = LATE_DATE
        playlist.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, playlist])
        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    'FIXED: LearnerPlaylistModel with id %s' % self.USER_ID_1
                ),
                job_run_result.JobRunResult.as_stdout(
                    'FIXED CREATED_ON SUCCESS: 1'
                ),
            ]
        )

    def test_created_on_within_threshold_is_not_fixed(self) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = LATE_DATE
        user_settings.last_updated = LATE_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = NEAR_DATE
        completed.last_updated = NEAR_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, completed])
        self.assert_job_output_is_empty()

    def test_uses_earliest_from_multiple_sources(self) -> None:
        user_settings = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings.created_on = LATE_DATE
        user_settings.last_updated = LATE_DATE
        user_settings.update_timestamps(update_last_updated_time=False)

        contributions = self.create_model(
            user_models.UserContributionsModel,
            id=self.USER_ID_1,
        )
        contributions.created_on = EARLY_DATE
        contributions.last_updated = EARLY_DATE
        contributions.update_timestamps(update_last_updated_time=False)

        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = LATE_DATE
        completed.last_updated = LATE_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([user_settings, contributions, completed])
        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    'FIXED: CompletedActivitiesModel with id %s'
                    % self.USER_ID_1
                ),
                job_run_result.JobRunResult.as_stdout(
                    'FIXED CREATED_ON SUCCESS: 1'
                ),
            ]
        )

    def test_multiple_users_are_fixed_independently(self) -> None:
        user_settings_1 = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_1,
            email='a@a.com',
        )
        user_settings_1.created_on = EARLY_DATE
        user_settings_1.last_updated = EARLY_DATE
        user_settings_1.update_timestamps(update_last_updated_time=False)

        user_settings_2 = self.create_model(
            user_models.UserSettingsModel,
            id=self.USER_ID_2,
            email='b@b.com',
        )
        user_settings_2.created_on = EARLY_DATE
        user_settings_2.last_updated = EARLY_DATE
        user_settings_2.update_timestamps(update_last_updated_time=False)

        completed_1 = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed_1.created_on = LATE_DATE
        completed_1.last_updated = LATE_DATE
        completed_1.update_timestamps(update_last_updated_time=False)

        playlist_2 = self.create_model(
            user_models.LearnerPlaylistModel,
            id=self.USER_ID_2,
        )
        playlist_2.created_on = LATE_DATE
        playlist_2.last_updated = LATE_DATE
        playlist_2.update_timestamps(update_last_updated_time=False)

        self.put_multi(
            [
                user_settings_1,
                user_settings_2,
                completed_1,
                playlist_2,
            ]
        )
        self.assert_job_output_is(
            [
                job_run_result.JobRunResult.as_stdout(
                    'FIXED: CompletedActivitiesModel with id %s'
                    % self.USER_ID_1
                ),
                job_run_result.JobRunResult.as_stdout(
                    'FIXED: LearnerPlaylistModel with id %s' % self.USER_ID_2
                ),
                job_run_result.JobRunResult.as_stdout(
                    'FIXED CREATED_ON SUCCESS: 2'
                ),
            ]
        )

    def test_model_without_matching_user_settings_is_not_fixed(
        self,
    ) -> None:
        completed = self.create_model(
            user_models.CompletedActivitiesModel,
            id=self.USER_ID_1,
        )
        completed.created_on = LATE_DATE
        completed.last_updated = LATE_DATE
        completed.update_timestamps(update_last_updated_time=False)

        self.put_multi([completed])
        self.assert_job_output_is_empty()
