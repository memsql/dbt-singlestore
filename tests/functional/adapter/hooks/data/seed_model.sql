-- Copyright 2021-2026 SingleStore, Inc.
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

drop table if exists on_model_hook;

create table on_model_hook (
    test_state       TEXT, -- start|end
    target_dbname    TEXT,
    target_host      TEXT,
    target_name      TEXT,
    target_schema    TEXT,
    target_type      TEXT,
    target_user      TEXT,
    target_pass      TEXT,
    target_threads   INTEGER,
    run_started_at   TEXT,
    invocation_id    TEXT,
    thread_id        TEXT
);
