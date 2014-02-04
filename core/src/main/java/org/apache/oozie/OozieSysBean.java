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
package org.apache.oozie;

import javax.persistence.Basic;
import javax.persistence.Id;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

@Entity
@NamedQueries( {
        @NamedQuery(name = "GET_HEARTBEAT_COUNT", query = "select count(w) from OozieSysBean w where w.name = :name"),

        @NamedQuery(name = "UPDATE_HEARTBEAT", query = "update OozieSysBean w set w.data = :value where w.name = :name"),

        @NamedQuery(name = "GET_LAST_HEARTBEAT", query = "select OBJECT(w) from OozieSysBean w where w.name = :name")

})
@Table(name = "OOZIE_SYS")
public class OozieSysBean {

    @Id
    @Basic
    @Column(name = "name")
    private String name;

    @Basic
    @Column(name = "data")
    private String data;

    public OozieSysBean(String name, String data) {
        this.name = name;
        this.data = data;
    }

    public OozieSysBean() {

    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }
}
