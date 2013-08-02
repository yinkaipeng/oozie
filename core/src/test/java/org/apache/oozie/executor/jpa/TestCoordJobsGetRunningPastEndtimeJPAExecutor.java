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

import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;


import java.util.Date;
import java.util.List;
/*
* Add unit test cases for CoordJobsGetRunningPastEndtimeJPAExecutor
*/
public class TestCoordJobsGetRunningPastEndtimeJPAExecutor extends XDataTestCase {
    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testCoordJobsGetRunningPastEndtime() throws Exception {
        Date startTime = DateUtils.parseDateOozieTZ("2009-03-06T010:00Z");
        Date endTime = DateUtils.parseDateOozieTZ("2009-03-11T10:00Z");
        CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, startTime, endTime, false, true, 0);
        _testGetJobs(job.getId());
        addRecordToCoordJobTable(CoordinatorJob.Status.SUCCEEDED, startTime, endTime, false, true, 0);
        _testGetJobs(job.getId());
    }

    private void _testGetJobs(String jobId) throws Exception {
        JPAService jpaService = Services.get().get(JPAService.class);
        assertNotNull(jpaService);
        CoordJobsGetRunningPastEndtimeJPAExecutor coordGetCmd = new CoordJobsGetRunningPastEndtimeJPAExecutor();
        List<String> jobIds= jpaService.execute(coordGetCmd);
        assertNotNull(jobIds);
        assertEquals(jobIds.size(), 1);
        assertEquals(jobIds.get(0), jobId);
    }
}
