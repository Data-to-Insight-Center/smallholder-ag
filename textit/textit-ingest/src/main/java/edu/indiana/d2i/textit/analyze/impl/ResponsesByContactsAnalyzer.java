package edu.indiana.d2i.textit.analyze.impl;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import edu.indiana.d2i.textit.analyze.Analyzer;
import edu.indiana.d2i.textit.utils.TextItUtils;
import edu.indiana.d2i.textit.utils.MongoDB;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by charmadu on 10/11/16.
 */
public class ResponsesByContactsAnalyzer implements Analyzer {

    private static Logger logger = Logger.getLogger(ResponsesByContactsAnalyzer.class);
    private static final String COLLECTION_NAME = "responses_by_contacts";

    private SimpleDateFormat df_Z = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private SimpleDateFormat df_SSS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private SimpleDateFormat df_dm = new SimpleDateFormat("d MMM");
    private static ArrayList<String> excludeFlowsMatch = new ArrayList<>();
    private static ArrayList<String> excludeFlows = new ArrayList<>();
    private static ArrayList<String> textLables = new ArrayList<>();

    private String EMAILS;
    private int TEXT_TIME;
    private String COUNTRY;
    //private Map<String, ArrayList<String>> FLOWS_TO_QTYPE;

    public ResponsesByContactsAnalyzer(Properties properties) {
        EMAILS = properties.getProperty("notification.email.addresses");
        TEXT_TIME = Integer.parseInt(properties.getProperty("text.time"));
        COUNTRY = properties.getProperty("mongodb.split.db.name");

//        String flows_to_qype_string = properties.getProperty("flows.to.qtype.map");
//        String[] list_string = flows_to_qype_string.split("|");
//
//        FLOWS_TO_QTYPE = new HashMap<>();
//
//
//        for (int i=0; i<list_string.length; i++) {
//
//            String season_name = (list_string[i].split(":"))[0];
//            String[] qtype_names = (list_string[i].split(":"))[1].split(",");
//            ArrayList<String> qtypes_list = new ArrayList<String>();
//            for (int j=0; j<qtype_names.length; j++) {
//                qtypes_list.add(qtype_names[j]);
//            }
//            FLOWS_TO_QTYPE.put(season_name, qtypes_list);
//        }
//
//        for (String s : FLOWS_TO_QTYPE.keySet()) {
//            //System.out.println(s + " is " + FLOWS_TO_QTYPE.get(s));
//        }

        excludeFlows.add("inter-season flow 1"); //TODO remove hardcoded values for test flows
        excludeFlows.add("inter-season flow 2");
        excludeFlowsMatch.add("test");
        excludeFlowsMatch.add("join");
        excludeFlowsMatch.add("copy");
        excludeFlowsMatch.add("15-16");

        textLables.add("variety 1");
        textLables.add("variety 1 of 1");
        textLables.add("variety 1 of 2");
        textLables.add("variety 2 of 2");
        textLables.add("when");
        //textLables.add("storage");
    }

    @Override
    public void analyze(Map<String, String> paramMap) {
        System.out.println("ResponsesByContactsAnalyzer started...");
        System.out.println("notification.email.addresses : " + EMAILS);

        String qTypeString = "all";
        qTypeString = paramMap.get("qTypes");
        List<String> qType = Arrays.asList(qTypeString.split(","));

        String fromDate = paramMap.get("fromDate");
        String toDate = paramMap.get("toDate");

        if(qTypeString.equals("all") || qType == null || qType.size() == 0) {
            logger.error("'qTypes' is a mandatory query parameter");
            System.exit(-1);
        }

        // TODO from=2016-08-08T11:00:00.000Z&to=2016-08-22T11:00:00.000Z
        MongoDatabase db = MongoDB.getDatabase();
        MongoDatabase integratedDB = MongoDB.getIntegratedDatabase();
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.FLOWS_COLLECTION_NAME);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.RUNS_COLLECTION_NAME);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.CONTACTS_COLLECTION_NAME);
        MongoCollection<Document> outputCollection = integratedDB.getCollection(COLLECTION_NAME);

        // gobal map contact -> {week -> {label -> {responses}}}
        Map<String, HashMap<String, HashMap<String, ArrayList<String>>>> qMap = new HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>>();

        int countryCode = 0;
        int countryHour = TEXT_TIME;
        if(COUNTRY.equals("zambia")) {
            countryCode = Calendar.MONDAY;
        } else if (COUNTRY.equals("kenya")) {
            countryCode = Calendar.SATURDAY;
        } else {
            logger.error("invalid country");
            System.exit(-1);
        }


        System.out.println("Analyze database from " + fromDate + " to " + toDate + " in time " + TEXT_TIME + " for questions " + qType.toString());
        Date first = null;
        Date last = null;
        try {
            if(fromDate != null) {
                first = df_Z.parse(fromDate);
            } else {
                FindIterable<Document> iter = flowsCollection.find().sort(new Document("created_on",1)).limit(1);;
                iter.projection(new Document("created_on", 1).append("_id", 0));
                first = df_Z.parse((String) iter.first().get("created_on"));
            }
            if(toDate != null) {
                last = df_Z.parse(toDate);
            } else {
                last = new Date();
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar c = Calendar.getInstance();
        c.setTime(last);
        Date currEnd = c.getTime();
        Date currBeg = null;

        ArrayList<String> weekLabels = new ArrayList<>();
        //Map<String, ArrayList<String>> weekQuestionLabels = new HashMap<String, ArrayList<String>>();

        while(currEnd.getTime() > first.getTime()) {

            Calendar cTemp = Calendar.getInstance();
            cTemp.setTime(currEnd);
            cTemp.set(Calendar.DAY_OF_WEEK, countryCode);
            cTemp.set(Calendar.HOUR_OF_DAY, countryHour);
            cTemp.set(Calendar.MINUTE, 0);
            cTemp.set(Calendar.SECOND, 0);
            cTemp.set(Calendar.MILLISECOND, 0);
            if (currEnd.equals(cTemp.getTime())) {
                cTemp.add(Calendar.DATE, -7);
                currBeg = cTemp.getTime();
            } else
                currBeg = cTemp.getTime();

            if(first.after(cTemp.getTime())) {
                currBeg = first;
            }

            currEnd = new Date(currEnd.getTime() -1);

            Calendar cWeek = Calendar.getInstance();
            cWeek.setMinimalDaysInFirstWeek(7);
            cWeek.setTime(currBeg);
            String label = cWeek.get(Calendar.YEAR) + " W" + String.format("%02d", cWeek.get(Calendar.WEEK_OF_YEAR))
                    + " : " + df_dm.format(currBeg) + " - " + df_dm.format(currEnd);
            cWeek.setTime(currEnd);
            //String nextLabel = cWeek.get(Calendar.YEAR) + " W" + String.format("%02d", cWeek.get(Calendar.WEEK_OF_YEAR))
            //+ " : " + df_dm.format(currBeg) + " - " + df_dm.format(currEnd);
            weekLabels.add(label);
            //weekQuestionLabels.put(label, new ArrayList<String>());

            Bson flowsFilter = null;
//            if(qType != null)
//                flowsFilter = Filters.in("rulesets.label", qType);

            // Newly Added
            /*Bson runsFilter = null;
            ArrayList<Bson> qtypeArray = new ArrayList<>();
            for(String qtype : qType) {
                Bson ques_filter = Filters.exists("values" + qtype);
                qtypeArray.add(ques_filter);
            }
            runsFilter = Filters.or(qtypeArray);*/

            ArrayList<Document> flowsIter = null;
            try {
                //flowsIter = TextItUtils.getFlowsByDeploymentDate(flowsCollection, runsCollection, flowsFilter, df_Z.format(currBeg), df_Z.format(currEnd), runsFilter);
                flowsIter = TextItUtils.getFlowsByDeploymentDate(flowsCollection, runsCollection, flowsFilter, df_Z.format(currBeg), df_Z.format(currEnd));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            for (Document flowsDocument : flowsIter) {
                String flow_uuid = (String) flowsDocument.get("uuid");

                if (flowsDocument.containsKey("name") && flowsDocument.get("name") != null && flowsDocument.get("name") instanceof String) {
                    String flowNameLowerCase = flowsDocument.getString("name").trim().toLowerCase();
                    System.out.print("Analyze Flow : " + flowNameLowerCase);

                    if (excludeFlows.contains(flowNameLowerCase)) {
                        System.out.println(" - SKIPPED");
                        continue;
                    } else {
                        boolean skip = false;
                        for (String match : excludeFlowsMatch) {
                            if (flowNameLowerCase.contains(match)) {
                                skip = true;
                                break;
                            }
                        }
                        if (skip) {
                            System.out.println(" - SKIPPED");
                            continue;
                        }
                    }
                } else {
                    System.out.println("No Flow Name");
                    continue;
                }

                System.out.println("--");

//                List<String> qUuid = new ArrayList<String>();
//                Object rulesets = flowsDocument.get("rulesets");
//                if (rulesets instanceof ArrayList) {
//                    ArrayList<Document> rulesetsArray = (ArrayList<Document>) rulesets;
//                    for (Document rule : rulesetsArray) {
//                        if (qType.contains(rule.getString("label"))) {
//                           // if(!weekQuestionLabels.get(label).contains(rule.getString("label"))) {
//                           //     weekQuestionLabels.get(label).add(rule.getString("label"));
//                           // }
//                            qUuid.add(rule.getString("node"));
//                        }
//                    }
//                }


                ArrayList<Bson> typeExists = new ArrayList<Bson>();
                List<String> qTypeStr = new ArrayList<>();
                for(String type : qType) {
                    qTypeStr.add(type.replace(' ', '_'));
                    typeExists.add(Filters.exists("values." + type.replace(' ', '_'), true));
                }

                BasicDBObject runsQuery = new BasicDBObject();
                runsQuery.put("flow_uuid", flow_uuid);
                //FindIterable<Document> runsIter = runsCollection.find(runsQuery);
                //FindIterable<Document> runsIter = runsCollection.find(Filters.and(runsQuery, Filters.in("values.node", qUuid)));

                //FindIterable<Document> runsIter = runsCollection.find(Filters.and(runsQuery, runsFilter));
                FindIterable<Document> runsIter = runsCollection.find(Filters.and(runsQuery, Filters.or(typeExists)));
                runsIter.projection(new Document("_id", 0));
                MongoCursor<Document> runsCursor = runsIter.iterator();

                while (runsCursor.hasNext()) {
                    Document runsDocument = runsCursor.next();
                    //Object values = runsDocument.get("values");
                    Document values = (Document) runsDocument.get("values");
                    //String contact = (String) runsDocument.get("contact");
                    String contact = (String)((Document) runsDocument.get("contact")).get("uuid");
                    //if (values instanceof ArrayList) {
                        //ArrayList<Document> valuesArray = (ArrayList<Document>) values;
                        //for (Document value : valuesArray) {
                    for (String qStr : qTypeStr) {
                            //Document category = (Document) value.get("category");
                            //String date = null;

                            if(!values.containsKey(qStr))
                                continue;
                            Document value = (Document) values.get(qStr);

                            //String qLabel = (String) value.get("label");
                            //String qNode = (String) value.get("node");
                            //if(qUuid.size() != 0 && !qUuid.contains(qNode))
                                //continue;

                            String qLabel = qStr.replace("_", " ");

                            Date dateZ = null;
                            try {
                                //date = df_dd.format(df_SSS.parse(value.getString("time").substring(0, 23)));
                                dateZ = df_SSS.parse(value.getString("time").substring(0, 23));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            if (dateZ.after(currEnd)) { // change if needed to put delayed responses to the next week
                                //week = nextLabel;
                                continue;
                            }


                            String category = (String) value.get("category");
                            //String qVal = category.get("base") != null ? category.getString("base") : category.getString("eng");
                            String qVal = category;
//                            if (qVal.equals("numeric") && value.containsKey("value")) {
//                                qVal = "" + value.get("value");
//                            }
//                            if (qVal.equalsIgnoreCase("Other") && value.containsKey("value")) {
//                                qVal += "-" + ("" + value.get("value"));
//                            }
                            if (category.equals("numeric") && value.containsKey("value")) {
                                qVal = "" + value.get("value");
                            } else if (textLables.contains(qLabel) && value.containsKey("value")) {
                                qVal = "" + value.get("value");
                            }
                            else if (category.equalsIgnoreCase("Other") && value.containsKey("value")) {
                                qVal = "Other-" + ("" + value.get("value"));
                            }
                            String week = label;

                            if (qMap.get(contact) != null) {
                                Map<String, HashMap<String, ArrayList<String>>> qObject = qMap.get(contact);
                                if (qObject.get(week) != null) {
                                    Map<String, ArrayList<String>> dateObject = qObject.get(week);
                                    if (dateObject.get(qLabel) != null) {
                                        dateObject.get(qLabel).add(qVal);
                                    } else {
                                        ArrayList<String> answerArray = new ArrayList<String>();
                                        answerArray.add(qVal);
                                        dateObject.put(qLabel, answerArray);
                                    }
                                } else {
                                    HashMap<String, ArrayList<String>> dateObject = new HashMap<String, ArrayList<String>>();
                                    ArrayList<String> answerArray = new ArrayList<String>();
                                    answerArray.add(qVal);
                                    dateObject.put(qLabel, answerArray);
                                    qObject.put(week, dateObject);
                                }
                            } else {
                                HashMap<String, HashMap<String, ArrayList<String>>> qObject = new HashMap<String, HashMap<String, ArrayList<String>>>();
                                HashMap<String, ArrayList<String>> dateObject = new HashMap<String, ArrayList<String>>();
                                ArrayList<String> answerArray = new ArrayList<String>();
                                answerArray.add(qVal);
                                dateObject.put(qLabel, answerArray);
                                qObject.put(week, dateObject);
                                qMap.put(contact, qObject);
                            }
                        }
                    //}
                }
            }

            currEnd = currBeg;
        }

        Collections.sort(weekLabels);
        for (String contact : qMap.keySet()) {
            ArrayList<Document> docArray = new ArrayList<Document>();
            for (String weekLabel : weekLabels) {  // if qType is provided
                String answer = "-";
                if (qMap.get(contact).containsKey(weekLabel)) {
                    HashMap<String, ArrayList<String>> answers = qMap.get(contact).get(weekLabel);
                    int i = 0;
                    Document contactObj = new Document();
                    contactObj.put("uuid", contact);
                    contactObj.put("week", weekLabel.split(":")[0].trim());
                    for (String qLabel : answers.keySet()) {
                        ArrayList<String> answerArray = answers.get(qLabel);
                        if (i == 0) {
                            answer = StringUtils.join(answerArray, " | ");
                        } else {
                            answer += " | " + StringUtils.join(answerArray, " | ");
                        }
                        i++;

                        contactObj.put(qLabel, answer);
                    }
                    docArray.add(contactObj);
                }
            }
            outputCollection.insertMany(docArray);
        }

    }
}
