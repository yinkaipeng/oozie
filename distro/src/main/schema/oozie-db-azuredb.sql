--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

DROP TABLE IF EXISTS oozie.BUNDLE_ACTIONS;
DROP TABLE IF EXISTS oozie.BUNDLE_JOBS;
DROP TABLE IF EXISTS oozie.COORD_ACTIONS;
DROP TABLE IF EXISTS oozie.COORD_JOBS;
DROP TABLE IF EXISTS oozie.OOZIE_SYS;
DROP TABLE IF EXISTS oozie.OPENJPA_SEQUENCE_TABLE;
DROP TABLE IF EXISTS oozie.SLA_EVENTS;
DROP TABLE IF EXISTS oozie.SLA_REGISTRATION;
DROP TABLE IF EXISTS oozie.SLA_SUMMARY;
DROP TABLE IF EXISTS oozie.VALIDATE_CONN;
DROP TABLE IF EXISTS oozie.WF_ACTIONS;
DROP TABLE IF EXISTS oozie.WF_JOBS;

CREATE TABLE oozie.BUNDLE_ACTIONS (bundle_action_id varchar(255) NOT NULL, bundle_id varchar(255), coord_id varchar(255), coord_name varchar(255), critical int, last_modified_time datetime2, pending int, status varchar(255));
CREATE TABLE oozie.BUNDLE_JOBS (id varchar(255) NOT NULL, app_name varchar(255), app_path varchar(255), conf image, created_time datetime2, end_time datetime2, external_id varchar(255), group_name varchar(255), job_xml image, kickoff_time datetime2, last_modified_time datetime2, orig_job_xml image, pause_time datetime2, pending int, start_time datetime2, status varchar(255), suspended_time datetime2, time_out int, time_unit varchar(255), user_name varchar(255));
CREATE TABLE oozie.COORD_ACTIONS (id varchar(255) NOT NULL, action_number int, action_xml image, console_url varchar(255), created_conf image, created_time datetime2, error_code varchar(255), error_message varchar(255), external_id varchar(255), external_status varchar(255), job_id varchar(255), last_modified_time datetime2, missing_dependencies image, nominal_time datetime2, pending int, push_missing_dependencies image, rerun_time datetime2, run_conf image, sla_xml image, status varchar(255), time_out int, tracker_uri varchar(255), job_type varchar(255));
CREATE TABLE oozie.COORD_JOBS (id varchar(255) NOT NULL, app_name varchar(255), app_namespace varchar(255), app_path varchar(255), bundle_id varchar(255), concurrency int, conf image, created_time datetime2, done_materialization int, end_time datetime2, execution varchar(255), external_id varchar(255), frequency varchar(255), group_name varchar(255), job_xml image, last_action_number int, last_action datetime2, last_modified_time datetime2, mat_throttling int, next_matd_time datetime2, orig_job_xml image, pause_time datetime2, pending int, sla_xml image, start_time datetime2, status varchar(255), suspended_time datetime2, time_out int, time_unit varchar(255), time_zone varchar(255), user_name varchar(255));
CREATE TABLE oozie.OOZIE_SYS (name varchar(100), data varchar(100));
CREATE TABLE oozie.OPENJPA_SEQUENCE_TABLE (ID tinyint NOT NULL, SEQUENCE_VALUE bigint);
CREATE TABLE oozie.SLA_EVENTS (event_id bigint identity(19,1) NOT NULL, alert_contact varchar(255), alert_frequency varchar(255), alert_percentage varchar(255), app_name varchar(255), dev_contact varchar(255), group_name varchar(255), job_data text, notification_msg text, parent_client_id varchar(255), parent_sla_id varchar(255), qa_contact varchar(255), se_contact varchar(255), sla_id varchar(255), upstream_apps text, user_name varchar(255), bean_type varchar(31), app_type varchar(255), event_type varchar(255), expected_end datetime2, expected_start datetime2, job_status varchar(255), status_timestamp datetime2);
CREATE TABLE oozie.SLA_REGISTRATION (job_id varchar(255) NOT NULL, app_name varchar(255), app_type varchar(255), created_time datetime2, expected_duration bigint, expected_end datetime2, expected_start datetime2, job_data varchar(255), nominal_time datetime2, notification_msg varchar(255), parent_id varchar(255), sla_config varchar(255), upstream_apps varchar(255), user_name varchar(255));
CREATE TABLE oozie.SLA_SUMMARY (job_id varchar(255) NOT NULL, actual_duration bigint, actual_end datetime2, actual_start datetime2, app_name varchar(255), app_type varchar(255), created_time datetime2, event_processed tinyint, event_status varchar(255), expected_duration bigint, expected_end datetime2, expected_start datetime2, job_status varchar(255), last_modified datetime2, nominal_time datetime2, parent_id varchar(255), sla_status varchar(255), user_name varchar(255));
CREATE TABLE oozie.VALIDATE_CONN (id bigint NOT NULL, dummy int);
CREATE TABLE oozie.WF_ACTIONS (id varchar(255) NOT NULL, conf image, console_url varchar(255), created_time datetime2, cred varchar(255), data image, end_time datetime2, error_code varchar(255), error_message varchar(500), execution_path varchar(1024), external_child_ids image, external_id varchar(255), external_status varchar(255), last_check_time datetime2, log_token varchar(255), name varchar(255), pending int, pending_age datetime2, retries int, signal_value varchar(255), sla_xml image, start_time datetime2, stats image, status varchar(255), tracker_uri varchar(255), transition varchar(255), type varchar(255), user_retry_count int, user_retry_interval int, user_retry_max int, wf_id varchar(255));
CREATE TABLE oozie.WF_JOBS (id varchar(255) NOT NULL, app_name varchar(255), app_path varchar(255), conf image, created_time datetime2, end_time datetime2, external_id varchar(255), group_name varchar(255), last_modified_time datetime2, log_token varchar(255), parent_id varchar(255), proto_action_conf image, run int, sla_xml image, start_time datetime2, status varchar(255), user_name varchar(255), wf_instance image);
CREATE INDEX I_BNDLTNS_BUNDLE_ID ON oozie.BUNDLE_ACTIONS (bundle_id);
CREATE UNIQUE INDEX PK__BUNDLE_A__05061E8B0EE01893 ON oozie.BUNDLE_ACTIONS (bundle_action_id);
CREATE INDEX I_BNDLJBS_CREATED_TIME ON oozie.BUNDLE_JOBS (created_time);
CREATE INDEX I_BNDLJBS_LAST_MODIFIED_TIME ON oozie.BUNDLE_JOBS (last_modified_time);
CREATE INDEX I_BNDLJBS_STATUS ON oozie.BUNDLE_JOBS (status);
CREATE INDEX I_BNDLJBS_SUSPENDED_TIME ON oozie.BUNDLE_JOBS (suspended_time);
CREATE UNIQUE INDEX PK__BUNDLE_J__3213E83FFEEA6A85 ON oozie.BUNDLE_JOBS (id);
CREATE INDEX I_CRD_TNS_CREATED_TIME ON oozie.COORD_ACTIONS (created_time);
CREATE INDEX I_CRD_TNS_EXTERNAL_ID ON oozie.COORD_ACTIONS (external_id);
CREATE INDEX I_CRD_TNS_JOB_ID ON oozie.COORD_ACTIONS (job_id);
CREATE INDEX I_CRD_TNS_LAST_MODIFIED_TIME ON oozie.COORD_ACTIONS (last_modified_time);
CREATE INDEX I_CRD_TNS_NOMINAL_TIME ON oozie.COORD_ACTIONS (nominal_time);
CREATE INDEX I_CRD_TNS_RERUN_TIME ON oozie.COORD_ACTIONS (rerun_time);
CREATE INDEX I_CRD_TNS_STATUS ON oozie.COORD_ACTIONS (status);
CREATE UNIQUE INDEX PK__COORD_AC__3213E83F71805D8A ON oozie.COORD_ACTIONS (id);
CREATE INDEX I_CRD_JBS_BUNDLE_ID ON oozie.COORD_JOBS (bundle_id);
CREATE INDEX I_CRD_JBS_CREATED_TIME ON oozie.COORD_JOBS (created_time);
CREATE INDEX I_CRD_JBS_LAST_MODIFIED_TIME ON oozie.COORD_JOBS (last_modified_time);
CREATE INDEX I_CRD_JBS_NEXT_MATD_TIME ON oozie.COORD_JOBS (next_matd_time);
CREATE INDEX I_CRD_JBS_STATUS ON oozie.COORD_JOBS (status);
CREATE INDEX I_CRD_JBS_SUSPENDED_TIME ON oozie.COORD_JOBS (suspended_time);
CREATE UNIQUE INDEX PK__COORD_JO__3213E83FA1951EF5 ON oozie.COORD_JOBS (id);
CREATE INDEX OOZIE_SYS_PK ON oozie.OOZIE_SYS (name);
CREATE UNIQUE INDEX PK__OPENJPA___3214EC271051AB6F ON oozie.OPENJPA_SEQUENCE_TABLE (ID);
CREATE INDEX I_SL_VNTS_DTYPE ON oozie.SLA_EVENTS (bean_type);
CREATE UNIQUE INDEX PK__SLA_EVEN__2370F72741226109 ON oozie.SLA_EVENTS (event_id);
CREATE INDEX I_SL_RRTN_NOMINAL_TIME ON oozie.SLA_REGISTRATION (nominal_time);
CREATE UNIQUE INDEX PK__SLA_REGI__6E32B6A5C7A41DAA ON oozie.SLA_REGISTRATION (job_id);
CREATE INDEX I_SL_SMRY_APP_NAME ON oozie.SLA_SUMMARY (app_name);
CREATE INDEX I_SL_SMRY_EVENT_PROCESSED ON oozie.SLA_SUMMARY (event_processed);
CREATE INDEX I_SL_SMRY_LAST_MODIFIED ON oozie.SLA_SUMMARY (last_modified);
CREATE INDEX I_SL_SMRY_NOMINAL_TIME ON oozie.SLA_SUMMARY (nominal_time);
CREATE INDEX I_SL_SMRY_PARENT_ID ON oozie.SLA_SUMMARY (parent_id);
CREATE UNIQUE INDEX PK__SLA_SUMM__6E32B6A5F72FF71C ON oozie.SLA_SUMMARY (job_id);
CREATE UNIQUE INDEX PK__VALIDATE__3213E83F894D0C3F ON oozie.VALIDATE_CONN (id);
CREATE INDEX I_WF_CTNS_PENDING_AGE ON oozie.WF_ACTIONS (pending_age);
CREATE INDEX I_WF_CTNS_STATUS ON oozie.WF_ACTIONS (status);
CREATE INDEX I_WF_CTNS_WF_ID ON oozie.WF_ACTIONS (wf_id);
CREATE UNIQUE INDEX PK__WF_ACTIO__3213E83FF42E5849 ON oozie.WF_ACTIONS (id);
CREATE INDEX I_WF_JOBS_END_TIME ON oozie.WF_JOBS (end_time);
CREATE INDEX I_WF_JOBS_EXTERNAL_ID ON oozie.WF_JOBS (external_id);
CREATE INDEX I_WF_JOBS_LAST_MODIFIED_TIME ON oozie.WF_JOBS (last_modified_time);
CREATE INDEX I_WF_JOBS_PARENT_ID ON oozie.WF_JOBS (parent_id);
CREATE INDEX I_WF_JOBS_STATUS ON oozie.WF_JOBS (status);
CREATE UNIQUE INDEX PK__WF_JOBS__3213E83FF68C1C15 ON oozie.WF_JOBS (id);

insert into oozie.OOZIE_SYS (name, data) values ('db.version', '3');
insert into oozie.OOZIE_SYS (name, data) values ('oozie.version', '4.2.0.2.6.0.0-479');