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

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.ServiceException;

/**
 * Shell decryption key provider which invokes an external script that will perform the password decryption.
 */
public class ShellPasswordProvider extends SimplePasswordProvider {

    public static final String CONF_PASSWORDPROVIDERSCRIPT = "jdbc.password.provider.script";

    @Override
    public String getPassword(Configuration conf, String confPrefix)
            throws ServiceException {
        String envelope = super.getPassword(conf, confPrefix);

        final String command = conf.get(confPrefix + CONF_PASSWORDPROVIDERSCRIPT);
        if (command == null) {
            throw new ServiceException(ErrorCode.E0608, 
                    "Script path is not specified via " + confPrefix + CONF_PASSWORDPROVIDERSCRIPT);
        }

        String[] cmd = command.split(" ");
        String[] cmdWithEnvelope = Arrays.copyOf(cmd, cmd.length + 1);
        cmdWithEnvelope[cmdWithEnvelope.length - 1] = envelope;

        String decryptedPassword = null;
        try {
            decryptedPassword = Shell.execCommand(cmdWithEnvelope);
        } catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0608, ex);
        }

        return decryptedPassword;
    }
}
