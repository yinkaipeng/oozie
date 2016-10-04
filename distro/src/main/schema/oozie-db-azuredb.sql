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

CREATE TABLE dbo.BUNDLE_ACTIONS (bundle_action_id varchar(255) NOT NULL, bundle_id varchar(255), coord_id varchar(255), coord_name varchar(255), critical int, last_modified_time datetime2, pending int, status varchar(255));
CREATE TABLE dbo.BUNDLE_JOBS (id varchar(255) NOT NULL, app_name varchar(255), app_path varchar(255), conf image, created_time datetime2, end_time datetime2, external_id varchar(255), group_name varchar(255), job_xml image, kickoff_time datetime2, last_modified_time datetime2, orig_job_xml image, pause_time datetime2, pending int, start_time datetime2, status varchar(255), suspended_time datetime2, time_out int, time_unit varchar(255), user_name varchar(255));
CREATE TABLE dbo.COORD_ACTIONS (id varchar(255) NOT NULL, action_number int, action_xml image, console_url varchar(255), created_conf image, created_time datetime2, error_code varchar(255), error_message varchar(255), external_id varchar(255), external_status varchar(255), job_id varchar(255), last_modified_time datetime2, missing_dependencies image, nominal_time datetime2, pending int, push_missing_dependencies image, rerun_time datetime2, run_conf image, sla_xml image, status varchar(255), time_out int, tracker_uri varchar(255), job_type varchar(255));
CREATE TABLE dbo.COORD_JOBS (id varchar(255) NOT NULL, app_name varchar(255), app_namespace varchar(255), app_path varchar(255), bundle_id varchar(255), concurrency int, conf image, created_time datetime2, done_materialization int, end_time datetime2, execution varchar(255), external_id varchar(255), frequency varchar(255), group_name varchar(255), job_xml image, last_action_number int, last_action datetime2, last_modified_time datetime2, mat_throttling int, next_matd_time datetime2, orig_job_xml image, pause_time datetime2, pending int, sla_xml image, start_time datetime2, status varchar(255), suspended_time datetime2, time_out int, time_unit varchar(255), time_zone varchar(255), user_name varchar(255));
CREATE TABLE dbo.OOZIE_SYS (name varchar(100), data varchar(100));
CREATE TABLE dbo.OPENJPA_SEQUENCE_TABLE (ID tinyint NOT NULL, SEQUENCE_VALUE bigint);
CREATE TABLE dbo.SLA_EVENTS (event_id bigint identity(19) NOT NULL, alert_contact varchar(255), alert_frequency varchar(255), alert_percentage varchar(255), app_name varchar(255), dev_contact varchar(255), group_name varchar(255), job_data text, notification_msg text, parent_client_id varchar(255), parent_sla_id varchar(255), qa_contact varchar(255), se_contact varchar(255), sla_id varchar(255), upstream_apps text, user_name varchar(255), bean_type varchar(31), app_type varchar(255), event_type varchar(255), expected_end datetime2, expected_start datetime2, job_status varchar(255), status_timestamp datetime2);
CREATE TABLE dbo.SLA_REGISTRATION (job_id varchar(255) NOT NULL, app_name varchar(255), app_type varchar(255), created_time datetime2, expected_duration bigint, expected_end datetime2, expected_start datetime2, job_data varchar(255), nominal_time datetime2, notification_msg varchar(255), parent_id varchar(255), sla_config varchar(255), upstream_apps varchar(255), user_name varchar(255));
CREATE TABLE dbo.SLA_SUMMARY (job_id varchar(255) NOT NULL, actual_duration bigint, actual_end datetime2, actual_start datetime2, app_name varchar(255), app_type varchar(255), created_time datetime2, event_processed tinyint, event_status varchar(255), expected_duration bigint, expected_end datetime2, expected_start datetime2, job_status varchar(255), last_modified datetime2, nominal_time datetime2, parent_id varchar(255), sla_status varchar(255), user_name varchar(255));
CREATE TABLE dbo.VALIDATE_CONN (id bigint NOT NULL, dummy int);
CREATE TABLE dbo.WF_ACTIONS (id varchar(255) NOT NULL, conf image, console_url varchar(255), created_time datetime2, cred varchar(255), data image, end_time datetime2, error_code varchar(255), error_message varchar(500), execution_path varchar(1024), external_child_ids image, external_id varchar(255), external_status varchar(255), last_check_time datetime2, log_token varchar(255), name varchar(255), pending int, pending_age datetime2, retries int, signal_value varchar(255), sla_xml image, start_time datetime2, stats image, status varchar(255), tracker_uri varchar(255), transition varchar(255), type varchar(255), user_retry_count int, user_retry_interval int, user_retry_max int, wf_id varchar(255));
CREATE TABLE dbo.WF_JOBS (id varchar(255) NOT NULL, app_name varchar(255), app_path varchar(255), conf image, created_time datetime2, end_time datetime2, external_id varchar(255), group_name varchar(255), last_modified_time datetime2, log_token varchar(255), parent_id varchar(255), proto_action_conf image, run int, sla_xml image, start_time datetime2, status varchar(255), user_name varchar(255), wf_instance image);
CREATE TABLE sys.trace_xe_action_map (trace_column_id smallint NOT NULL, package_name nvarchar(60) NOT NULL, xe_action_name nvarchar(60) NOT NULL);
CREATE TABLE sys.trace_xe_event_map (trace_event_id smallint NOT NULL, package_name nvarchar(60) NOT NULL, xe_event_name nvarchar(60) NOT NULL);
CREATE INDEX I_BNDLTNS_BUNDLE_ID ON dbo.BUNDLE_ACTIONS (bundle_id);
CREATE UNIQUE INDEX PK__BUNDLE_A__05061E8B0EE01893 ON dbo.BUNDLE_ACTIONS (bundle_action_id);
CREATE INDEX I_BNDLJBS_CREATED_TIME ON dbo.BUNDLE_JOBS (created_time);
CREATE INDEX I_BNDLJBS_LAST_MODIFIED_TIME ON dbo.BUNDLE_JOBS (last_modified_time);
CREATE INDEX I_BNDLJBS_STATUS ON dbo.BUNDLE_JOBS (status);
CREATE INDEX I_BNDLJBS_SUSPENDED_TIME ON dbo.BUNDLE_JOBS (suspended_time);
CREATE UNIQUE INDEX PK__BUNDLE_J__3213E83FFEEA6A85 ON dbo.BUNDLE_JOBS (id);
CREATE INDEX I_CRD_TNS_CREATED_TIME ON dbo.COORD_ACTIONS (created_time);
CREATE INDEX I_CRD_TNS_EXTERNAL_ID ON dbo.COORD_ACTIONS (external_id);
CREATE INDEX I_CRD_TNS_JOB_ID ON dbo.COORD_ACTIONS (job_id);
CREATE INDEX I_CRD_TNS_LAST_MODIFIED_TIME ON dbo.COORD_ACTIONS (last_modified_time);
CREATE INDEX I_CRD_TNS_NOMINAL_TIME ON dbo.COORD_ACTIONS (nominal_time);
CREATE INDEX I_CRD_TNS_RERUN_TIME ON dbo.COORD_ACTIONS (rerun_time);
CREATE INDEX I_CRD_TNS_STATUS ON dbo.COORD_ACTIONS (status);
CREATE UNIQUE INDEX PK__COORD_AC__3213E83F71805D8A ON dbo.COORD_ACTIONS (id);
CREATE INDEX I_CRD_JBS_BUNDLE_ID ON dbo.COORD_JOBS (bundle_id);
CREATE INDEX I_CRD_JBS_CREATED_TIME ON dbo.COORD_JOBS (created_time);
CREATE INDEX I_CRD_JBS_LAST_MODIFIED_TIME ON dbo.COORD_JOBS (last_modified_time);
CREATE INDEX I_CRD_JBS_NEXT_MATD_TIME ON dbo.COORD_JOBS (next_matd_time);
CREATE INDEX I_CRD_JBS_STATUS ON dbo.COORD_JOBS (status);
CREATE INDEX I_CRD_JBS_SUSPENDED_TIME ON dbo.COORD_JOBS (suspended_time);
CREATE UNIQUE INDEX PK__COORD_JO__3213E83FA1951EF5 ON dbo.COORD_JOBS (id);
CREATE INDEX OOZIE_SYS_PK ON dbo.OOZIE_SYS (name);
CREATE UNIQUE INDEX PK__OPENJPA___3214EC271051AB6F ON dbo.OPENJPA_SEQUENCE_TABLE (ID);
CREATE INDEX I_SL_VNTS_DTYPE ON dbo.SLA_EVENTS (bean_type);
CREATE UNIQUE INDEX PK__SLA_EVEN__2370F72741226109 ON dbo.SLA_EVENTS (event_id);
CREATE INDEX I_SL_RRTN_NOMINAL_TIME ON dbo.SLA_REGISTRATION (nominal_time);
CREATE UNIQUE INDEX PK__SLA_REGI__6E32B6A5C7A41DAA ON dbo.SLA_REGISTRATION (job_id);
CREATE INDEX I_SL_SMRY_APP_NAME ON dbo.SLA_SUMMARY (app_name);
CREATE INDEX I_SL_SMRY_EVENT_PROCESSED ON dbo.SLA_SUMMARY (event_processed);
CREATE INDEX I_SL_SMRY_LAST_MODIFIED ON dbo.SLA_SUMMARY (last_modified);
CREATE INDEX I_SL_SMRY_NOMINAL_TIME ON dbo.SLA_SUMMARY (nominal_time);
CREATE INDEX I_SL_SMRY_PARENT_ID ON dbo.SLA_SUMMARY (parent_id);
CREATE UNIQUE INDEX PK__SLA_SUMM__6E32B6A5F72FF71C ON dbo.SLA_SUMMARY (job_id);
CREATE UNIQUE INDEX PK__VALIDATE__3213E83F894D0C3F ON dbo.VALIDATE_CONN (id);
CREATE INDEX I_WF_CTNS_PENDING_AGE ON dbo.WF_ACTIONS (pending_age);
CREATE INDEX I_WF_CTNS_STATUS ON dbo.WF_ACTIONS (status);
CREATE INDEX I_WF_CTNS_WF_ID ON dbo.WF_ACTIONS (wf_id);
CREATE UNIQUE INDEX PK__WF_ACTIO__3213E83FF42E5849 ON dbo.WF_ACTIONS (id);
CREATE INDEX I_WF_JOBS_END_TIME ON dbo.WF_JOBS (end_time);
CREATE INDEX I_WF_JOBS_EXTERNAL_ID ON dbo.WF_JOBS (external_id);
CREATE INDEX I_WF_JOBS_LAST_MODIFIED_TIME ON dbo.WF_JOBS (last_modified_time);
CREATE INDEX I_WF_JOBS_PARENT_ID ON dbo.WF_JOBS (parent_id);
CREATE INDEX I_WF_JOBS_STATUS ON dbo.WF_JOBS (status);
CREATE UNIQUE INDEX PK__WF_JOBS__3213E83FF68C1C15 ON dbo.WF_JOBS (id);

create table OOZIE_SYS (name varchar(100), data varchar(100))
create clustered index OOZIE_SYS_PK on OOZIE_SYS (name);
insert into OOZIE_SYS (name, data) values ('db.version', '3')
insert into OOZIE_SYS (name, data) values ('oozie.version', '4.2.0.2.5.0.0-1245')
