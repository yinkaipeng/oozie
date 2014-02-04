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

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.oozie.CoordinatorJobBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class CoordJobsGetDowntimeRunningJPAExecutor implements JPAExecutor<List<CoordinatorJobBean>>{

    private long startTime = 0;

    public CoordJobsGetDowntimeRunningJPAExecutor(Long start) {
        this.startTime = start;
    }

    @Override
    public String getName() {
        return "CoordJobsGetDowntimeRunningJPAExecutor";
    }

    @Override
    public List<CoordinatorJobBean> execute(EntityManager em) throws JPAExecutorException {
        List<CoordinatorJobBean> beans = new ArrayList<CoordinatorJobBean>();
        Query q = em.createNamedQuery("GET_COORD_JOBS_RUNNING_DOWNTIME");
//        q.setParameter("startTime", new Timestamp(startTime));
        beans = q.getResultList();
        return beans;
    }
}
