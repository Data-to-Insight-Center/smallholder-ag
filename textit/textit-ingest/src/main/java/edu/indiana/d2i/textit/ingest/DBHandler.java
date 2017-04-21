package edu.indiana.d2i.textit.ingest;

import edu.indiana.d2i.textit.utils.EmailService;
import edu.indiana.d2i.textit.utils.MongoDB;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Created by charmadu on 6/15/16.
 */
public class DBHandler {

    private String output_dir =  "./output";
    public static final String FLOWS = "flows";
    public static final String RUNS = "runs";
    public static final String CONTACTS = "contacts";
    public static final String STATS = "stats";
    public final String END_DATE;
    public final String START_DATE;
    public final String INTERVAL;
    public final String EMAILS;

    private static Logger logger = Logger.getLogger(DBHandler.class);
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    public DBHandler(Properties properties) {
        this.output_dir = properties.getProperty("outputdir");
        this.END_DATE = properties.getProperty("end_date");;
        this.START_DATE = properties.getProperty("start_date");
        this.INTERVAL = properties.getProperty("interval");
        EMAILS = properties.getProperty("notification.email.addresses");
        df.setTimeZone(TimeZone.getTimeZone("timezone"));
    }

    public boolean persistData(){
        JSONObject statusObject = new JSONObject();
        statusObject.put(MongoDB.DATE, df.format(new Date()));
        statusObject.put(MongoDB.ACTION, MongoDB.WRITE_TO_MONGO);
        statusObject.put(MongoDB.TYPE, "all");
        statusObject.put(MongoDB.END_DATE, df.format(new DateTime(END_DATE).toDate()));
        if(INTERVAL.equals(MongoDB.DURATION)) {
            statusObject.put(MongoDB.START_DATE, df.format(new DateTime(START_DATE).toDate()));
        }
        statusObject.put(MongoDB.INTERVAL, INTERVAL);

        try {
            saveRawRuns();
            saveRawFlows();
            saveRawContacts();
            saveRuns();
            saveFlows();
            saveContacts();
        } catch (Exception e) {
            logger.error(e.getMessage());
            statusObject.put(MongoDB.STATUS, MongoDB.FAILURE);
            statusObject.put(MongoDB.MESSAGE, e.getMessage());
            MongoDB.addStatus(statusObject.toString());

            EmailService.sendNotificatinEmail(Arrays.asList(EMAILS.split("|")),
                    "TextIt Ingestor Script failed to persist data in MongoDB : " + e.getMessage());
            return false;
        }
        statusObject.put(MongoDB.STATUS, MongoDB.SUCCESS);
        MongoDB.addStatus(statusObject.toString());
        return true;
    }

    private boolean saveRawRuns() throws FileNotFoundException {
        String out_dir = output_dir+ "/" + RUNS;
        File dir = new File(out_dir);
        File[] directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".json");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                MongoDB.addRawRuns(out_dir, child.getName());
            }
        }
        return true;
    }

    private boolean saveRawFlows() throws FileNotFoundException {
        String out_dir = output_dir+ "/" + FLOWS;
        File dir = new File(out_dir);
        File[] directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".json");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                MongoDB.addRawFlows(out_dir, child.getName());
            }
        }
        return true;
    }

    private boolean saveRawContacts() throws FileNotFoundException {
        String out_dir = output_dir+ "/" + CONTACTS;
        File dir = new File(out_dir);
        File[] directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(CONTACTS + ".json");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                MongoDB.addRawContacts(out_dir, child.getName());
            }
        }
        return true;
    }

    private boolean saveRuns() throws IOException {
        String out_dir = output_dir+ "/" + RUNS;
        File dir = new File(out_dir);
        File[] directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".json");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                String fileString = new String(Files.readAllBytes(Paths.get(out_dir + "/" + child.getName())));
                JSONObject runs = new JSONObject(fileString);
                JSONArray runsArray = runs.getJSONArray("results");
                for(int i=0 ; i < runsArray.length() ; i++){
                    JSONObject run = runsArray.getJSONObject(i);
                    MongoDB.addRun(run.getInt("id"), runsArray.getJSONObject(i).toString());
                }
            }
        }
        if (directoryListing != null) {
            for (File child : directoryListing) {
                Files.deleteIfExists(Paths.get(out_dir + "/" + child.getName()));
            }
        }
        return true;
    }

    private boolean saveFlows() throws IOException {
        String out_dir = output_dir+ "/" + FLOWS;
        File dir = new File(out_dir);
        File[] directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".json");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                String fileString = new String(Files.readAllBytes(Paths.get(out_dir + "/" + child.getName())));
                JSONObject flows = new JSONObject(fileString);
                JSONArray flowsArray = flows.getJSONArray("results");
                for(int i=0 ; i < flowsArray.length() ; i++){
                    MongoDB.addFlow(flowsArray.getJSONObject(i).getString("uuid"),
                            flowsArray.getJSONObject(i).toString());
                }
            }
        }
        if (directoryListing != null) {
            for (File child : directoryListing) {
                Files.deleteIfExists(Paths.get(out_dir + "/" + child.getName()));
            }
        }
        return true;
    }

    private boolean saveContacts() throws IOException {
        String out_dir = output_dir+ "/" + CONTACTS;
        File dir = new File(out_dir);
        File[] directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(CONTACTS + ".json");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                String fileString = new String(Files.readAllBytes(Paths.get(out_dir + "/" + child.getName())));
                JSONObject contacts = new JSONObject(fileString);
                JSONArray contactsArray = contacts.getJSONArray("results");
                for(int i=0 ; i < contactsArray.length() ; i++){
                    MongoDB.addContact(contactsArray.getJSONObject(i).getString("uuid"),
                            contactsArray.getJSONObject(i).toString());
                }
            }
        }
        if (directoryListing != null) {
            for (File child : directoryListing) {
                Files.deleteIfExists(Paths.get(out_dir + "/" + child.getName()));
            }
        }

        directoryListing = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(STATS + ".json");
            }
        });
        if (directoryListing != null) {
            for (File child : directoryListing) {
                String fileString = new String(Files.readAllBytes(Paths.get(out_dir + "/" + child.getName())));
                JSONObject contacts = new JSONObject(fileString);
                MongoDB.addContactStat(contacts.toString());
            }
        }
        if (directoryListing != null) {
            for (File child : directoryListing) {
                Files.deleteIfExists(Paths.get(out_dir + "/" + child.getName()));
            }
        }
        return true;
    }
}
