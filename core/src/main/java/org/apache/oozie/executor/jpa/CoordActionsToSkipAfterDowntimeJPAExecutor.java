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

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.client.CoordinatorAction;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

public class CoordActionsToSkipAfterDowntimeJPAExecutor implements JPAExecutor<Void> {

    private String coordJobId = null;

    private long startTime = 0;

    private long endTime = 0;

    private String recovery = null;

    public CoordActionsToSkipAfterDowntimeJPAExecutor(String id, long start, long end, String recovery) {
        this.coordJobId = id;
        this.startTime = start;
        this.endTime = end;
        this.recovery = recovery;
    }

    @Override
    public String getName() {
        return "CoordActionsToSkipAfterDowntimeJPAExecutor";
    }

    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {

        if (recovery.equalsIgnoreCase("ALL")) {
            return null;
        }

        Query q = em.createNamedQuery("GET_COORD_ACTIONS_FOR_DOWNTIME");
        q.setParameter("jobId", coordJobId);
        q.setParameter("endTime", new Timestamp(endTime));
        q.setParameter("startTime", new Timestamp(startTime));
        List<Object[]> objs = q.getResultList();

        boolean isFirst = true;
        for (Object[] arr : objs) {
            if (isFirst) {
                if (recovery.equalsIgnoreCase("LAST_ONLY")) {
                    isFirst = false;
                    continue;
                }
                isFirst = false;
            }

            CoordinatorActionBean bean = getCoordinatorActionBeanFromArray(arr);
            q = em.createNamedQuery("UPDATE_COORD_ACTION_STATUS_PENDING_TIME");
            q.setParameter("id", bean.getId());
            q.setParameter("status", bean.getStatus().toString());
            q.setParameter("lastModifiedTime", new Date());
            q.setParameter("pending", bean.getPending());
            q.executeUpdate();
        }
        return null;
    }

    private CoordinatorActionBean getCoordinatorActionBeanFromArray (Object[] arr) {
        CoordinatorActionBean bean = new CoordinatorActionBean();

        if (arr[0] != null) {
            bean.setId((String) arr[0]);
        }
        if (arr[1] != null) {
            bean.setJobId((String) arr[1]);
        }
        if (arr[2] != null) {
            bean.setStatus(CoordinatorAction.Status.SKIPPED);
        }
        if (arr[3] != null) {
            bean.setPending((Integer) arr[3]);
        }
        return bean;
    }
}
