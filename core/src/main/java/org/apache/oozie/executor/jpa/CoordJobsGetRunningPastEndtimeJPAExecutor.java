/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.executor.jpa;

import org.apache.oozie.ErrorCode;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.List;

/*
* Get the list of ids of jobs which are still running after end time
*/
public class CoordJobsGetRunningPastEndtimeJPAExecutor implements JPAExecutor<List<String>> {

    public CoordJobsGetRunningPastEndtimeJPAExecutor() {
    }

    @Override
    public String getName() {
        return "CoordJobsGetRunningPastEndtimeJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String >execute(EntityManager em) throws JPAExecutorException {
        try {
            Timestamp curr = new Timestamp(System.currentTimeMillis());
            Query q = em.createNamedQuery("GET_COORD_JOBS_RUNNING_PAST_ENDTIME");
            q.setParameter("endTime", curr);
            List<String> coordJobIds = q.getResultList();
            return coordJobIds;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }
}
