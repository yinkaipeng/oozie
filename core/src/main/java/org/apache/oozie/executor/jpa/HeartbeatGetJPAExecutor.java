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

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.List;

public class HeartbeatGetJPAExecutor implements JPAExecutor<OozieSysBean> {

    public HeartbeatGetJPAExecutor(){
    }

    @Override
    public String getName() {
        return "HeartbeatGetJPAExecutor";
    }

    public OozieSysBean execute(EntityManager em) throws JPAExecutorException {
        try {
            List<OozieSysBean> OozieSysBeanList = new ArrayList<OozieSysBean>();
            Query q = em.createNamedQuery("GET_LAST_HEARTBEAT");
            q.setParameter("name", "oozie.heartbeat");
            OozieSysBeanList = q.getResultList();
            if (OozieSysBeanList == null || OozieSysBeanList.size() == 0){
                return null;
            }
            return OozieSysBeanList.get(0);
//            return GetBeanForOozieSysFromArray(OozieSysBeanList.get(0));
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

    private OozieSysBean GetBeanForOozieSysFromArray(Object[] arr) {
        OozieSysBean bean = new OozieSysBean();
        if (arr[0] != null) {
            bean.setName((String) arr[0]);
        }

        if (arr[1] != null) {
            bean.setData((String) arr[1]);
        }

        return bean;
    }
}
