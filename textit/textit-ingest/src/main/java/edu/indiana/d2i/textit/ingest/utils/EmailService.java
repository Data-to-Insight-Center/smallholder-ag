package edu.indiana.d2i.textit.ingest.utils;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.List;

/**
 * Created by charmadu on 10/10/16.
 */
public class EmailService {

    private static Logger logger = Logger.getLogger(EmailService.class);

    public static void sendNotificatinEmail(List<String> emails, String message){
        String cmd = "mail -s \"" + message + "\" " + StringUtils.join(emails, ",");
        try {
            ByteArrayOutputStream stdout = executeCommand(cmd);
            BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(stdout.toByteArray())));
            String output_line = null;
            String messageStr = "";
            while((output_line = br.readLine()) != null) {
                messageStr += output_line;
            }
            logger.info("Notification Email sent : " + messageStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static ByteArrayOutputStream executeCommand(String command) throws IOException {
        CommandLine cmdLine = CommandLine.parse(command);
        DefaultExecutor executor = new DefaultExecutor();
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        PumpStreamHandler psh = new PumpStreamHandler(stdout);
        executor.setStreamHandler(psh);
        int exitValue = executor.execute(cmdLine);
        return stdout;
    }

}
