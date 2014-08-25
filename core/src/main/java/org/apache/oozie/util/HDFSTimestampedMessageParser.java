package org.apache.oozie.util;


import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashSet;

/**
 * A version of TimestampedMessageParser that reads log from HDFS
 */
public class HDFSTimestampedMessageParser extends TimestampedMessageParser{

    public HDFSTimestampedMessageParser(BufferedReader reader, XLogFilter filter) {
        super(reader, filter);
    }

    public List<String> getAllServers() throws IOException{
        String currentLine = "";
        Pattern pattern = Pattern.compile("SERVER\\[(.*?)\\]");
        HashSet<String> servers = new HashSet<String>();
        while ((currentLine = reader.readLine()) != null) {
            String server = "";
            Matcher matcher = pattern.matcher(currentLine);
            while (matcher.find()) {
                server = matcher.group(1);
                break;
            }

            //For efficiency purpose, we first store lines to be copied by jobId
            if (!server.trim().equals("-") && !server.isEmpty()) {
                if (!servers.contains(server)) {
                    servers.add(server);
                }
            }
        }

        List<String> allServers = new ArrayList<String>();
        for (String s: servers) {
            allServers.add(s);
        }
        return allServers;
    }
}
