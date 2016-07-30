package edu.indiana.d2i.textit.api.service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import edu.indiana.d2i.textit.api.utils.MongoDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.*;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by kunarath on 2/15/16.
 */

@Path("/")
public class TextItRest {

    private CacheControl control = new CacheControl();
    private SimpleDateFormat df_Z = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private SimpleDateFormat df_SSS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private SimpleDateFormat df_dd = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat df_dmy = new SimpleDateFormat("dd MMMMM yyyy");

    private static Logger logger = Logger.getLogger(TextItRest.class);
    private static int daysBeforeFlowDeployment = 14;

    static {
        PropertyConfigurator.configure(TextItRest.class.getResource("./../log4j.properties"));
    }

    @GET
    @Path("/{country}/flows")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllFlows(@PathParam("country") String country,
                                @QueryParam("flowId") String flowId,
                                @QueryParam("from") String fromDate,
                                @QueryParam("to") String toDate) {
        control.setNoCache(true);

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDate = fromDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$gte", fromDate)));
        }
        if (toDate != null) {
            toDate = toDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$lte", toDate)));
        }
        if (flowId != null) {
            obj.add(new BasicDBObject("uuid", flowId));
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> iter = flowsCollection.find(andQuery).sort(new Document("created_on",-1));;
        iter.projection(new Document("flows", 1)
                .append("uuid", 1).append("name", 1)
                .append("archived", 1).append("labels", 1)
                .append("created_on", 1).append("expires", 1)
                .append("runs", 1).append("completed_runs", 1)
                .append("rulesets", 1).append("_id", 0));
        MongoCursor<Document> cursor = iter.iterator();
        JSONArray array = new JSONArray();
        while (cursor.hasNext()) {
            array.put(new JSONObject(cursor.next().toJson()));
        }
        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/runs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllRuns(@PathParam("country") String country,
                               @QueryParam("contact") String contactId,
                               @QueryParam("flowId") String flowId,
                               @QueryParam("from") String fromDate,
                               @QueryParam("to") String toDate) {
        control.setNoCache(true);

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDate = fromDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$gte", fromDate)));
        }
        if (toDate != null) {
            toDate = toDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$lte", toDate)));
        }
        if (flowId != null) {
            obj.add(new BasicDBObject("flow_uuid", flowId));
        }
        if (contactId != null) {
            obj.add(new BasicDBObject("contact", contactId));
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> iter = runsCollection.find(andQuery);
        iter.projection(new Document("runs", 1)
                .append("flow_uuid", 1).append("flow", 1)
                .append("contact", 1).append("run", 1)
                .append("created_on", 1).append("modified_on", 1)
                .append("completed", 1).append("expires_on", 1)
                .append("steps", 1).append("values", 1)
                .append("_id", 0));
        MongoCursor<Document> cursor = iter.iterator();
        JSONArray array = new JSONArray();
        while (cursor.hasNext()) {
            array.put(new JSONObject(cursor.next().toJson()));
        }
        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/contacts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllContacts(@PathParam("country") String country,
                                   @QueryParam("uuid") String contact,
                                   @QueryParam("from") String fromDate,
                                   @QueryParam("to") String toDate) {
        control.setNoCache(true);

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollectionName);

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDate = fromDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("modified_on", new BasicDBObject("$gte", fromDate)));
        }
        if (toDate != null) {
            toDate = toDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("modified_on", new BasicDBObject("$lte", toDate)));
        }
        if (contact != null) {
            obj.add(new BasicDBObject("uuid", contact));
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> iter = contactsCollection.find(andQuery);
        iter.projection(new Document("contacts", 1)
                .append("uuid", 1).append("name", 1)
                .append("language", 1).append("phone", 1)
                .append("group_uuids", 1).append("latitude", 1)
                .append("longitude", 1).append("camp", 1)
                .append("modified_on", 1).append("_id", 0));
        MongoCursor<Document> cursor = iter.iterator();
        JSONArray array = new JSONArray();
        while (cursor.hasNext()) {
            array.put(new JSONObject(cursor.next().toJson()));
        }
        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllData(@PathParam("country") String country,
                               @QueryParam("flowId") String flowId,
                               @QueryParam("contact") String contact,
                               @QueryParam("from") String fromDate,
                               @QueryParam("to") String toDate) {

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollectionName);

        control.setNoCache(true);

        JSONArray array = new JSONArray();
        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDate = fromDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$gte", fromDate)));
        }
        if (toDate != null) {
            toDate = toDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$lte", toDate)));
        }
        if (flowId != null) {
            obj.add(new BasicDBObject("uuid", flowId));
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> flowIter = flowsCollection.find(andQuery);
        MongoCursor<Document> flowCursor = flowIter.iterator();

        while (flowCursor.hasNext()) {
            Document flowDocument = flowCursor.next();
            String flow_uuid = (String) flowDocument.get("uuid");

            BasicDBObject runsQuery = new BasicDBObject();
            runsQuery.put("flow_uuid", flow_uuid);
            if (contact != null) {
                runsQuery.put("contact", contact);
            }

            FindIterable<Document> runsIter = runsCollection.find(runsQuery);
            MongoCursor<Document> runsCursor = runsIter.iterator();

            while (runsCursor.hasNext()) {
                Document runsDocument = runsCursor.next();

                JSONObject new_list = new JSONObject();
                Object ques = runsDocument.get("steps");
                Object values = runsDocument.get("values");
                if (ques instanceof ArrayList) {
                    ArrayList<Document> quesIds = (ArrayList<Document>) ques;
                    ArrayList<Document> quesNames = (ArrayList<Document>) values;
                    for (Document node : quesIds) {
                        node.getString("node");
                    }
                    new_list.put("steps", quesIds);
                    new_list.put("values", quesNames);
                }

                String contactId = runsDocument.getString("contact");
                new_list.put("flow_name", flowDocument.getString("name"));
                new_list.put("flow", runsDocument.getInteger("flow"));
                new_list.put("run", runsDocument.getInteger("run"));
                new_list.put("created_on", runsDocument.getString("created_on"));

                FindIterable<Document> contactIter = contactsCollection.find(new BasicDBObject("uuid", contactId));
                MongoCursor<Document> contactCursor = contactIter.iterator();

                while (contactCursor.hasNext()) {
                    Document contactDocument = contactCursor.next();
                    new_list.put("contact_name", contactDocument.getString("name"));
                    new_list.put("contact_phone", contactDocument.getString("phone"));
                    break;
                }

                array.put(new_list);
            }
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/runsofflow")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRunsOfFlowData(@PathParam("country") String country,
                                      @QueryParam("flowId") String flowId,
                                      @QueryParam("contact") String contact,
                                      @QueryParam("from") String fromDate,
                                      @QueryParam("to") String toDate) {

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);

        control.setNoCache(true);

        JSONArray array = new JSONArray();

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDate = fromDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$gte", fromDate)));
        }
        if (toDate != null) {
            toDate = toDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$lte", toDate)));
        }
        if (flowId != null) {
            obj.add(new BasicDBObject("uuid", flowId));
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> flowsIter = flowsCollection.find(andQuery);
        MongoCursor<Document> flowsCursor = flowsIter.iterator();

        while (flowsCursor.hasNext()) {
            Document flowsDocument = flowsCursor.next();
            String flow_uuid = (String) flowsDocument.get("uuid");

            BasicDBObject runsQuery = new BasicDBObject();
            runsQuery.put("flow_uuid", flow_uuid);
            if (contact != null) {
                runsQuery.put("contact", contact);
            }

            FindIterable<Document> runsIter = runsCollection.find(runsQuery);
            MongoCursor<Document> runsCursor = runsIter.iterator();

            while (runsCursor.hasNext()) {
                Document runsDocument = runsCursor.next();

                JSONObject new_list = new JSONObject();

                Object ques = runsDocument.get("steps");
                Object values = runsDocument.get("values");
                if (ques instanceof ArrayList) {
                    ArrayList<Document> quesIds = (ArrayList<Document>) ques;
                    ArrayList<Document> quesNames = (ArrayList<Document>) values;
                    for (Document node : quesIds) {
                        node.getString("node");
                    }
                    new_list.put("steps", quesIds);
                    new_list.put("values", quesNames);
                }
                new_list.put("flow_name", flowsDocument.getString("name"));
                new_list.put("flow", runsDocument.getInteger("flow"));
                new_list.put("run", runsDocument.getInteger("run"));
                new_list.put("created_on", runsDocument.getString("created_on"));
                new_list.put("contact", runsDocument.getString("contact"));

                array.put(new_list);
            }
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/runsandcontacts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRunsAndContactsData(@PathParam("country") String country,
                                           @QueryParam("contact") String contactId,
                                           @QueryParam("from") String fromDate,
                                           @QueryParam("to") String toDate) {

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);

        control.setNoCache(true);

        JSONArray array = new JSONArray();

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDate = fromDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$gte", fromDate)));
        }
        if (toDate != null) {
            toDate = toDate.replace("+00:00", "Z");
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$lte", toDate)));
        }
        if (contactId != null) {
            obj.add(new BasicDBObject("contact", contactId));
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> runsIter = runsCollection.find(andQuery);
        MongoCursor<Document> runsCursor = runsIter.iterator();

        while (runsCursor.hasNext()) {
            Document document = runsCursor.next();
            String contact_id = (String) document.get("contact");

            FindIterable<Document> contactsIter = contactsCollection.find(new BasicDBObject("uuid", contact_id));
            MongoCursor<Document> contactsCursor = contactsIter.iterator();

            while (contactsCursor.hasNext()) {
                Document document1 = contactsCursor.next();

                JSONObject new_list = new JSONObject();

                new_list.put("contact_id", document.getString("contact"));
                new_list.put("run_id", document.getInteger("run"));
                new_list.put("status", document.getBoolean("completed"));
                new_list.put("created_on", document.getString("created_on"));
                new_list.put("contact_name", document1.getString("name"));
                new_list.put("contact_phone", document1.getString("phone"));
                new_list.put("modified_on", document1.getString("modified_on"));

                array.put(new_list);
            }
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/flowcompletion")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFlowCompletionAnalysis(@PathParam("country") String country,
                                      @QueryParam("flowId") String flowId,
                                      @QueryParam("from") String fromDate,
                                      @QueryParam("to") String toDate) {

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        control.setNoCache(true);
        JSONArray array = new JSONArray();

        Bson filter = flowId != null ? Filters.eq("uuid", flowId) : null;
        ArrayList<Document> flowsIter = null;
        try {
            flowsIter = getFlowsByDeploymentDate(flowsCollection, runsCollection, filter, fromDate, toDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        for (Document flowsDocument : flowsIter) {
            String flow_uuid = (String) flowsDocument.get("uuid");

            BasicDBObject runsQuery = new BasicDBObject();
            runsQuery.put("flow_uuid", flow_uuid);

            FindIterable<Document> runsIter = runsCollection.find(runsQuery);
            MongoCursor<Document> runsCursor = runsIter.iterator();

            Map<Integer, Integer> completedCount = new HashMap<Integer, Integer>();
            JSONObject new_flow = new JSONObject();

            while (runsCursor.hasNext()) {
                Document runsDocument = runsCursor.next();

                boolean completed = runsDocument.getBoolean("completed");

                if(!completed)
                    continue;

                Object ques = runsDocument.get("steps");
                ArrayList<Date> dates = new ArrayList<Date>();
                if (ques instanceof ArrayList) {
                    ArrayList<Document> steps = (ArrayList<Document>) ques;
                    for (Document step : steps) {
                        try {
                            dates.add(df_SSS.parse(step.getString("left_on").substring(0, 23)));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                }
                Date created = null;
                try {
                    created = df_Z.parse(runsDocument.getString("created_on"));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                Collections.sort(dates);
                Date last = dates.get(dates.size() - 1);
                long diff = Math.abs(last.getTime() - created.getTime());
                double diffHours = diff * 1.0 / ( 60 * 60 * 1000);
                int hoursToComplete = (int)(Math.ceil(diffHours * 1.0 / 12))*12;
                if(completedCount.get(hoursToComplete) != null) {
                    completedCount.put(hoursToComplete, completedCount.get(hoursToComplete) + 1);
                } else {
                    completedCount.put(hoursToComplete, 1);
                }
            }

            int total_runs = flowsDocument.getInteger("runs");

            new_flow.put("flow_name", flowsDocument.getString("name"));
            new_flow.put("created_on", flowsDocument.getString("created_on"));
            try {
                if(runsIter.first() != null)
                    new_flow.put("deployed_on", df_dd.format(df_Z.parse(runsIter.first().getString("created_on"))));
                else
                    new_flow.putOnce("deployed_on", JSONObject.NULL);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            new_flow.put("uuid", flow_uuid);
            new_flow.put("total_runs", total_runs);
            new_flow.put("completed_runs", flowsDocument.getInteger("completed_runs"));
            new_flow.put("expires", flowsDocument.getInteger("expires"));

            if(total_runs > 0) {
                JSONArray perc_matrix = new JSONArray();
                int completed = 0;

                ArrayList<Integer> keyList = new ArrayList<Integer>();
                keyList.addAll(completedCount.keySet());
                Collections.sort(keyList);

                for(int i = 12 ; i <= keyList.get(keyList.size() -1) ; i = i + 12) {
                    if(completedCount.get(i) != null) {
                        completed += completedCount.get(i);
                    }
                    JSONObject matrix_object = new JSONObject();
                    matrix_object.put("perc", Math.round(completed*100.0/total_runs));
                    matrix_object.put("hour", i);
                    perc_matrix.put(matrix_object);
                }


                new_flow.put("matrix", perc_matrix);
            }

            array.put(new_flow);
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/questionanalysis")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQuestionAnalysis(@PathParam("country") String country,
                                              @QueryParam("type") String qType,
                                              @QueryParam("from") String fromDate,
                                              @QueryParam("to") String toDate) {

        if(qType == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new JSONObject().put("error", "'type' is a mandatory query parameter").toString())
                    .cacheControl(control).build();
        }

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        control.setNoCache(true);
        JSONArray array = new JSONArray();

        ArrayList<Document> flowsIter = null;
        try {
            flowsIter = getFlowsByDeploymentDate(flowsCollection, runsCollection, Filters.in("rulesets.label", qType), fromDate, toDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        long total_runs = 0;
        long answered = 0;
        long notAnswered = 0;
        long notAsked = 0;
        Map<String, Map<String, Integer>> qObject = new HashMap<String, Map<String, Integer>>();

        for (Document flowsDocument : flowsIter) {
            String flow_uuid = (String) flowsDocument.get("uuid");

            String qUuid = "";
            Object rulesets = flowsDocument.get("rulesets");
            if (rulesets instanceof ArrayList) {
                ArrayList<Document> rulesetsArray = (ArrayList<Document>) rulesets;
                for (Document rule : rulesetsArray) {
                    if(rule.getString("label").equals(qType)) {
                        qUuid = rule.getString("node");
                        break;
                    }
                }
            }

            BasicDBObject runsQuery = new BasicDBObject();
            runsQuery.put("flow_uuid", flow_uuid);
            FindIterable<Document> runsIter = runsCollection.find(Filters.and(runsQuery, Filters.in("values.node", qUuid) ));
            MongoCursor<Document> runsCursor = runsIter.iterator();
            //Map<String, Map<String, Integer>> qObject = new HashMap<String, Map<String, Integer>>();

            answered += runsCollection.count(Filters.and(runsQuery, Filters.in("values.node", qUuid)));
            notAnswered += runsCollection.count(Filters.and(runsQuery, Filters.nin("values.node", qUuid), Filters.in("steps.node", qUuid) ));
            notAsked += runsCollection.count(Filters.and(runsQuery, Filters.nin("values.node", qUuid), Filters.nin("steps.node", qUuid) ));
            total_runs += flowsDocument.getInteger("runs");

            while (runsCursor.hasNext()) {
                Document runsDocument = runsCursor.next();
                Object values = runsDocument.get("values");
                if (values instanceof ArrayList) {
                    ArrayList<Document> valuesArray = (ArrayList<Document>) values;
                    for (Document value : valuesArray) {
                        if(value.getString("node").equals(qUuid)) {
                            Document category = (Document) value.get("category");
                            String date = null;
                            try {
                                date = df_dd.format(df_SSS.parse(value.getString("time").substring(0, 23)));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            String qVal = category.get("base") != null ? category.getString("base") : category.getString("eng");
                            if(qObject.get(date) != null) {
                                Map<String, Integer> dateObject = qObject.get(date);
                                if(dateObject.get(qVal) != null) {
                                    dateObject.put(qVal, dateObject.get(qVal) + 1);
                                } else {
                                    dateObject.put(qVal, 1);
                                }
                            } else {
                                Map<String, Integer> newObject = new HashMap<String, Integer>();
                                newObject.put(qVal, 1);
                                qObject.put(date, newObject);
                            }
                        }
                    }
                }
            }
        }

        for (String key : qObject.keySet()) {
            JSONObject arrayObject = new JSONObject();
            arrayObject.put("day", key);
            JSONArray matrix = new JSONArray();
            for(String valKey : qObject.get(key).keySet()) {
                matrix.put(new JSONObject().put("anw", valKey).put("count", qObject.get(key).get(valKey)));
            }
            arrayObject.put("matrix", matrix);
            array.put(arrayObject);
        }
        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/filesize")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFileSizes(@PathParam("country") String country,
                                        @QueryParam("flowId") String flowId,
                                        @QueryParam("from") String fromDate,
                                        @QueryParam("to") String toDate,
                                        @QueryParam("count") int count) {

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollectionName);

        control.setNoCache(true);
        JSONArray array = new JSONArray();

        Bson filter = flowId != null ? Filters.eq("uuid", flowId) : null;

        Calendar c = Calendar.getInstance();
        if(country.equals("zambia")) {
            c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            c.set(Calendar.HOUR_OF_DAY, 12);
        } else if (country.equals("kenya")) {
            //TODO
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new JSONObject().put("error", "invalid country").toString())
                    .cacheControl(control).build();
        }
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        for(int i = 0 ; i < count ; i ++) {

            c.add(Calendar.MILLISECOND, -1);
            Date currentDate = c.getTime();
            String currentDateStr = df_Z.format(c.getTime());
            c.add(Calendar.DATE, -7);
            c.add(Calendar.MILLISECOND, 1);
            Date prevDate = c.getTime();
            String prevDateStr = df_Z.format(c.getTime());

            ArrayList<Document> flowsIter = null;
            try {
                flowsIter = getFlowsByDeploymentDate(flowsCollection, runsCollection, filter, prevDateStr, currentDateStr);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            double flowSize = 0;
            double runsSize = 0;
            double contactSize = 0;

            for (Document flowsDocument : flowsIter) {
                String flow_uuid = (String) flowsDocument.get("uuid");
                flowSize += flowsDocument.toJson().length();

                BasicDBObject runsQuery = new BasicDBObject();
                runsQuery.put("flow_uuid", flow_uuid);
                FindIterable<Document> runsIter = runsCollection.find(runsQuery);
                runsIter.projection(new Document("_id", 0));
                MongoCursor<Document> runsCursor = runsIter.iterator();

                while (runsCursor.hasNext()) {
                    Document runsDocument = runsCursor.next();
                    runsSize += runsDocument.toJson().length();
                }
            }

            FindIterable<Document> contactsIter = contactsCollection.find(Filters.and(
                    Filters.gte("modified_on", prevDateStr),
                    Filters.lte("modified_on",currentDateStr)) );
            contactsIter.projection(new Document("_id", 0));
            MongoCursor<Document> contactsCursor = contactsIter.iterator();
            while (contactsCursor.hasNext()) {
                Document contactDocument = contactsCursor.next();
                contactSize += contactDocument.toJson().length();
            }

            array.put(new JSONObject()
                    .put("week", df_dd.format(prevDate) + "-" + df_dd.format(currentDate))
                    .put("matrix",
                            new JSONArray()
                                    .put(new JSONObject().put("type", "flows").put("count", flowSize))
                                    .put(new JSONObject().put("type", "runs").put("count", runsSize))
                                    .put(new JSONObject().put("type", "contacts").put("count", contactSize))));
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }

    public ArrayList<Document> getFlowsByDeploymentDate(MongoCollection<Document> flowsCollection,
                                                        MongoCollection<Document> runsCollection,
                                                        Bson filter,  String fromDate, String toDate) throws ParseException {

        ArrayList<Document> flows = new ArrayList<Document>();
        Date fromDay = null;
        Date toDay = null;
        String toDateStr = "";

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDay = df_Z.parse(fromDate);
            Calendar cal = Calendar.getInstance();
            cal.setTime(df_Z.parse(fromDate));
            cal.add(Calendar.DATE, -daysBeforeFlowDeployment);
            String dateBefore = df_Z.format(cal.getTime());
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$gte", dateBefore)));
        }
        if (toDate != null) {
            toDay = df_Z.parse(toDate);
            toDateStr = df_dmy.format(toDay);
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$lte", toDate)));
        }
        if (obj.size() != 0)
            andQuery.put("$and", obj);

        FindIterable<Document> flowsIter;
        if(filter != null)
            flowsIter = flowsCollection.find(Filters.and(andQuery, filter));
        else
            flowsIter = flowsCollection.find(andQuery);

        flowsIter.projection(new Document("_id", 0));
        MongoCursor<Document> flowsCursor = flowsIter.iterator();

        while (flowsCursor.hasNext()) {
            boolean include = true;
            Document flowsDocument = flowsCursor.next();

            if(fromDate != null || toDate != null) {
                include = false;
                String flow_uuid = (String) flowsDocument.get("uuid");

                BasicDBObject runsQuery = new BasicDBObject();
                runsQuery.put("flow_uuid", flow_uuid);
                FindIterable<Document> runsIter = runsCollection.find(runsQuery);
                flowsIter.projection(new Document("created_on", 1).append("_id", 0));
                MongoCursor<Document> runsCursor = runsIter.iterator();
                int count = 0;
                while (runsCursor.hasNext() && include == false && count < 10) {
                    count++;
                    Document runsDocument = runsCursor.next();
                    Date runDay = df_Z.parse(runsDocument.getString("created_on"));
                    if ((fromDate == null || (fromDate != null && runDay.after(fromDay)))
                            && (toDate == null || (toDate != null && runDay.before(toDay)))) {
                        include = true;
                    }
                }
            }

            if(toDate != null && include == true && flowsDocument.getString("name").contains(toDateStr))
                include = false;

            if(include)
                flows.add(flowsDocument);
        }

        return flows;
    }

    class stringToIntComp implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            int number1 = Integer.parseInt(o1);
            int number2 = Integer.parseInt(o2);
            if (number1 > number2) {
                return 1;
            } else {
                return -1;
            }
        }
    }
}