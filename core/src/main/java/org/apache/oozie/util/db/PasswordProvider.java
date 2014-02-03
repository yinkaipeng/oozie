/*
 * Copyright 2013 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.util.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ServiceException;

/**
 * The interface that every password provider must implement.
 */
public interface PasswordProvider {
  /**
   * Password providers must implement this method. Given a list of configuration
   * parameters for the metastore database password, retrieve the plaintext
   * password.
   * @param conf Configuration parameters
   * @param confPrefix The prefix of the password configuration parameter name
   * @return the plaintext password
   * @throws ServiceException
   */
  String getPassword(Configuration conf, String confPrefix)
      throws ServiceException;
}
