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
import org.apache.oozie.OozieSysBean;
import org.apache.oozie.util.ParamChecker;

import javax.persistence.EntityManager;

public class HeartbeatUpdateJPAExecutor implements JPAExecutor<Void> {
    private OozieSysBean sysBean = null;

    public HeartbeatUpdateJPAExecutor(OozieSysBean sysBean) {
        ParamChecker.notNull(sysBean, "sysBean");
        this.sysBean = sysBean;
    }

    @Override
    public String getName() {
        return "HeartbeatUpdateJPAExecutor";
    }

    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        try {
            em.merge(sysBean);
            return null;
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }
}
