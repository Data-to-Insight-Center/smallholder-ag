package edu.indiana.d2i.textit.api.service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.sun.jersey.api.client.ClientResponse;
import edu.indiana.d2i.textit.api.utils.Constants;
import edu.indiana.d2i.textit.api.utils.MongoDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
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
    private SimpleDateFormat df_dmmy = new SimpleDateFormat("d MMMMM yyyy");
    private SimpleDateFormat df_dmy = new SimpleDateFormat("d MMM yyyy");
    private SimpleDateFormat df_dm = new SimpleDateFormat("d MMM");

    private static Logger logger = Logger.getLogger(TextItRest.class);
    private static int daysBeforeFlowDeployment = 16;

    private static ArrayList<String> excludeFlowsMatch = new ArrayList<>();
    private static ArrayList<String> excludeFlows = new ArrayList<>();

    static {
        PropertyConfigurator.configure(TextItRest.class.getResource("./../log4j.properties"));
        excludeFlows.add("inter-season flow 1"); //TODO remove hardcoded values for test flows
        excludeFlows.add("inter-season flow 2");
        excludeFlowsMatch.add("test");
        excludeFlowsMatch.add("join");
        excludeFlowsMatch.add("copy");
        excludeFlowsMatch.add("15-16");
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
    @Path("/{country}/contactstats")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getContactStats(@PathParam("country") String country,
                                   @QueryParam("from") String fromDate,
                                   @QueryParam("to") String toDate) {
        control.setNoCache(true);

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> contactsStatCollection = db.getCollection(MongoDB.contactsStatCollectionName);

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        try {
            if (fromDate != null) {
                fromDate = fromDate.replace("+00:00", "Z");
                obj.add(new BasicDBObject("fromDate", new BasicDBObject("$gte", df_dd.format(df_Z.parse(fromDate)))));
            }
            if (toDate != null) {
                toDate = toDate.replace("+00:00", "Z");
                obj.add(new BasicDBObject("toDate", new BasicDBObject("$lte", df_dd.format(df_Z.parse(toDate)))));
            }
        } catch (ParseException e) {
            return Response.status(ClientResponse.Status.BAD_REQUEST)
                    .entity(new JSONObject().put("error", "Couldn't parse the 'from'/'to' date(s)").toString())
                    .cacheControl(control).build();
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> iter = contactsStatCollection.find(andQuery);
        iter.projection(new Document("_id", 0));
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
                    matrix_object.put("abs", completed);
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
    @Path("/{country}/flowresponse")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFlowRespondAnalysis(@PathParam("country") String country,
                                              @QueryParam("flowId") String flowId,
                                              @QueryParam("qCount") String noOfQsStr,
                                              @QueryParam("from") String fromDate,
                                              @QueryParam("to") String toDate) {

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        control.setNoCache(true);


        int noOfQs = 0;
        if (noOfQsStr != null) {
            try {
                noOfQs = Integer.parseInt(noOfQsStr);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new JSONObject().put("error", "'qCount' should be an integer").toString())
                        .cacheControl(control).build();
            }
        }
        if (noOfQs < 1) {
            noOfQs = 1;
        }

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

            Map<Integer, Integer> respondedCount = new HashMap<Integer, Integer>();
            JSONObject new_flow = new JSONObject();

            while (runsCursor.hasNext()) {
                Document runsDocument = runsCursor.next();

                Object values = runsDocument.get("values");
                ArrayList<Date> dates = new ArrayList<Date>();
                if (values instanceof ArrayList) {
                    ArrayList<Document> steps = (ArrayList<Document>) values;
                    for (Document step : steps) {
                        try {
                            dates.add(df_SSS.parse(step.getString("time").substring(0, 23)));
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                }

                if(dates.size() < noOfQs)
                    continue;

                Date created = null;
                try {
                    created = df_Z.parse(runsDocument.getString("created_on"));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                Collections.sort(dates);
                Date last = dates.get(noOfQs - 1);
                long diff = Math.abs(last.getTime() - created.getTime());
                double diffHours = diff * 1.0 / ( 60 * 60 * 1000);
                int hoursToRespond = (int)(Math.ceil(diffHours * 1.0 / 12))*12;
                if(respondedCount.get(hoursToRespond) != null) {
                    respondedCount.put(hoursToRespond, respondedCount.get(hoursToRespond) + 1);
                } else {
                    respondedCount.put(hoursToRespond, 1);
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

            int responded = 0;
            if(total_runs > 0 && respondedCount.size() > 0) {
                JSONArray perc_matrix = new JSONArray();
                ArrayList<Integer> keyList = new ArrayList<Integer>();
                keyList.addAll(respondedCount.keySet());
                Collections.sort(keyList);

                for(int i = 12 ; i <= keyList.get(keyList.size() -1) ; i = i + 12) {
                    if(respondedCount.get(i) != null) {
                        responded += respondedCount.get(i);
                    }
                    JSONObject matrix_object = new JSONObject();
                    matrix_object.put("abs", responded);
                    matrix_object.put("perc", Math.round(responded*100.0/total_runs));
                    matrix_object.put("hour", i);
                    perc_matrix.put(matrix_object);
                }


                new_flow.put("matrix", perc_matrix);
            }
            new_flow.put("responded", responded);


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

        Date fromDay = null;
        Date toDay = null;
        if (fromDate != null) {
            try {
                fromDay = df_Z.parse(fromDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        if (toDate != null) {
            try {
                toDay = df_Z.parse(toDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        if(fromDate != null) {
            Calendar c = Calendar.getInstance();
            try {
                c.setTime(df_Z.parse(fromDate));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            c.add(Calendar.DATE, -8); // people will answer questions 7/8 days after the flow deployment
            fromDate = df_Z.format(c.getTime());

        }

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
            //TODO add time filter to this place - to and from dates
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
                            Date dateZ = null;
                            try {
                                date = df_dd.format(df_SSS.parse(value.getString("time").substring(0, 23)));
                                dateZ = df_SSS.parse(value.getString("time").substring(0, 23));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            String qVal = category.get("base") != null ? category.getString("base") : category.getString("eng");

                            //TODO remove time filter from this place - to and from dates
                            if ( (fromDate != null && dateZ.before(fromDay)) || (toDate != null && dateZ.after(toDay)) ) {
                                continue;
                            }

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
                                        //@QueryParam("from") String fromDate,
                                        //@QueryParam("to") String toDate,
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
            c.set(Calendar.HOUR_OF_DAY, Constants.zambiaTime);
        } else if (country.equals("kenya")) {
            c.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
            c.set(Calendar.HOUR_OF_DAY, Constants.kenyaTime);
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
                    .put("week", df_dmy.format(prevDate) + " - " + df_dmy.format(currentDate))
                    .put("matrix",
                            new JSONArray()
                                    .put(new JSONObject().put("type", "flows").put("count", flowSize))
                                    .put(new JSONObject().put("type", "runs").put("count", runsSize))
                                    .put(new JSONObject().put("type", "contacts").put("count", contactSize))));
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }


    @GET
    @Path("/{country}/questioninfo")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQeutstionInfo(@PathParam("country") String country,
                                 @QueryParam("from") String fromDate,
                                 @QueryParam("to") String toDate) {

        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollectionName);

        control.setNoCache(true);
        JSONArray array = new JSONArray();
        Map<String, HashMap<String, HashMap<String, ArrayList<String>>>> qMap = new HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>>();

        int countryCode = 0;
        int countryHour = 0;
        if(country.equals("zambia")) {
            countryCode = Calendar.MONDAY;
            countryHour = Constants.zambiaTime;
        } else if (country.equals("kenya")) {
            countryCode = Calendar.SATURDAY;
            countryHour = Constants.kenyaTime;
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new JSONObject().put("error", "invalid country").toString())
                    .cacheControl(control).build();
        }

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
            String nextLabel = cWeek.get(Calendar.YEAR) + " W" + String.format("%02d", cWeek.get(Calendar.WEEK_OF_YEAR))
                    + " : " + df_dm.format(currBeg) + " - " + df_dm.format(currEnd);

            ArrayList<Document> flowsIter = null;
            try {
                flowsIter = getFlowsByDeploymentDate(flowsCollection, runsCollection, null, df_Z.format(currBeg),  df_Z.format(currEnd));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            for (Document flowsDocument : flowsIter) {
                String flow_uuid = (String) flowsDocument.get("uuid");

                BasicDBObject runsQuery = new BasicDBObject();
                runsQuery.put("flow_uuid", flow_uuid);
                FindIterable<Document> runsIter = runsCollection.find(runsQuery);
                runsIter.projection(new Document("_id", 0));
                MongoCursor<Document> runsCursor = runsIter.iterator();

                while (runsCursor.hasNext()) {
                    Document runsDocument = runsCursor.next();
                    Object values = runsDocument.get("values");
                    String contact = (String) runsDocument.get("contact");
                    if (values instanceof ArrayList) {
                        ArrayList<Document> valuesArray = (ArrayList<Document>) values;
                        for (Document value : valuesArray) {
                            Document category = (Document) value.get("category");
                            String date = null;
                            Date dateZ = null;
                            try {
                                date = df_dd.format(df_SSS.parse(value.getString("time").substring(0, 23)));
                                dateZ = df_SSS.parse(value.getString("time").substring(0, 23));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            String qVal = category.get("base") != null ? category.getString("base") : category.getString("eng");
                            if(qVal.equals("numeric") && value.containsKey("value")) {
                                qVal = "" + value.get("value");
                            }
                            String qLabel = (String) value.get("label");


                            String week = label;
                            if (dateZ.after(currEnd)) {
                                week = nextLabel;
                            }

                            if(qMap.get(contact) != null) {
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
                    }
                }
            }
            currEnd = currBeg;
        }


        for(String contact : qMap.keySet()) {
            FindIterable<Document> contactsIter = contactsCollection.find(Filters.eq("uuid", contact));
            Document contactDoc = contactsIter.first();
            JSONObject contactObj = new JSONObject();
            if(contactDoc!= null && contactDoc.containsKey("name")) {
                contactObj.put("name", contactDoc.get("name"));
            }
            if(contactDoc!= null && contactDoc.containsKey("phone")) {
                contactObj.put("phone", contactDoc.get("phone"));
            }
            contactObj.put("uuid", contact);
            contactObj.put("results", new JSONObject(qMap.get(contact)));
            array.put(contactObj);
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }

    @GET
    @Path("/{country}/delayedruns")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRunsDelayedResponses(@PathParam("country") String country,
                                     @QueryParam("from") String fromDate,
                                     @QueryParam("to") String toDate) {

        // TODO from=2016-08-08T11:00:00.000Z&to=2016-08-22T11:00:00.000Z
        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollectionName);

        control.setNoCache(true);
        JSONArray array = new JSONArray();
        Map<String, HashMap<String, HashMap<String, ArrayList<String>>>> qMap = new HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>>();

        int countryCode = 0;
        int countryHour = 0;
        if(country.equals("zambia")) {
            countryCode = Calendar.MONDAY;
            countryHour = Constants.zambiaTime;
        } else if (country.equals("kenya")) {
            countryCode = Calendar.SATURDAY;
            countryHour = Constants.kenyaTime;
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new JSONObject().put("error", "invalid country").toString())
                    .cacheControl(control).build();
        }

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
            String nextLabel = cWeek.get(Calendar.YEAR) + " W" + String.format("%02d", cWeek.get(Calendar.WEEK_OF_YEAR))
                    + " : " + df_dm.format(currBeg) + " - " + df_dm.format(currEnd);

            ArrayList<Document> flowsIter = null;
            try {
                flowsIter = getFlowsByDeploymentDate(flowsCollection, runsCollection, null, df_Z.format(currBeg),  df_Z.format(currEnd));
            } catch (ParseException e) {
                e.printStackTrace();
            }

            for (Document flowsDocument : flowsIter) {
                String flow_uuid = (String) flowsDocument.get("uuid");

                BasicDBObject runsQuery = new BasicDBObject();
                runsQuery.put("flow_uuid", flow_uuid);
                FindIterable<Document> runsIter = runsCollection.find(runsQuery);
                runsIter.projection(new Document("_id", 0));
                MongoCursor<Document> runsCursor = runsIter.iterator();

                int totalRuns = (int) flowsDocument.get("runs");
                if(totalRuns <= 0)
                    continue;

                int delayedRuns = 0;

                while (runsCursor.hasNext()) {
                    Document runsDocument = runsCursor.next();
                    ArrayList<Document> values = (ArrayList<Document>)runsDocument.get("steps");
                    Date modified_on = null;
                    try {
                        modified_on = df_Z.parse(values.get(values.size() -1).getString("arrived_on"));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if(modified_on.after(currEnd)) {
                        delayedRuns++;
                    }
                }

                double delayedPerc = (double) Math.round((delayedRuns*1.0/totalRuns)*100 * 100) / 100  ;

                JSONObject flow = new JSONObject();
                flow.put("name", flowsDocument.get("name"));
                flow.put("uuid", flowsDocument.get("uuid"));
                flow.put("week", label);
                flow.put("start_date", df_Z.format(currBeg));
                flow.put("end_date", df_Z.format(currEnd));
                flow.put("total_runs", totalRuns);
                flow.put("late_runs", delayedRuns);
                flow.put("late_runs_perc", delayedPerc);
                array.put(flow);
            }

            currEnd = currBeg;
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }


    @GET
    @Path("/{country}/contactresponses")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getResponsesByContacts(@PathParam("country") String country,
                                            @QueryParam("qtype") List<String> qType,
                                            @QueryParam("from") String fromDate,
                                            @QueryParam("to") String toDate) {


        if(qType == null || qType.size() == 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new JSONObject().put("error", "'qtype' is a mandatory query parameter").toString())
                    .cacheControl(control).build();
        }

        // TODO from=2016-08-08T11:00:00.000Z&to=2016-08-22T11:00:00.000Z
        MongoDatabase db = MongoDB.getMongoClientInstance().getDatabase(country);
        MongoCollection<Document> flowsCollection = db.getCollection(MongoDB.flowsCollectionName);
        MongoCollection<Document> runsCollection = db.getCollection(MongoDB.runsCollectionName);
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollectionName);

        control.setNoCache(true);
        JSONArray array = new JSONArray();
        Map<String, HashMap<String, HashMap<String, ArrayList<String>>>> qMap = new HashMap<String, HashMap<String, HashMap<String, ArrayList<String>>>>();

        int countryCode = 0;
        int countryHour = 0;
        if(country.equals("zambia")) {
            countryCode = Calendar.MONDAY;
            countryHour = Constants.zambiaTime;
        } else if (country.equals("kenya")) {
            countryCode = Calendar.SATURDAY;
            countryHour = Constants.kenyaTime;
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new JSONObject().put("error", "invalid country").toString())
                    .cacheControl(control).build();
        }

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

            Bson flowsFilter = null;
            if(qType != null)
                flowsFilter = Filters.in("rulesets.label", qType);

            ArrayList<Document> flowsIter = null;
            try {
                flowsIter = getFlowsByDeploymentDate(flowsCollection, runsCollection, flowsFilter, df_Z.format(currBeg),  df_Z.format(currEnd));
            } catch (ParseException e) {
                e.printStackTrace();
            }

           /* for (Document flowsDocument : flowsIter) {
                JSONObject flow = new JSONObject();
                flow.put("name", flowsDocument.get("name"));
                flow.put("week", label);
                flow.put("start_date", df_Z.format(currBeg));
                flow.put("end_date", df_Z.format(currEnd));
                array.put(flow);
            }*/

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

                List<String> qUuid = new ArrayList<String>();
                Object rulesets = flowsDocument.get("rulesets");
                if (rulesets instanceof ArrayList) {
                    ArrayList<Document> rulesetsArray = (ArrayList<Document>) rulesets;
                    for (Document rule : rulesetsArray) {
                        if (qType.contains(rule.getString("label"))) {
                            qUuid.add(rule.getString("node"));
                        }
                    }
                }

                BasicDBObject runsQuery = new BasicDBObject();
                runsQuery.put("flow_uuid", flow_uuid);
                //FindIterable<Document> runsIter = runsCollection.find(runsQuery);
                FindIterable<Document> runsIter = runsCollection.find(Filters.and(runsQuery, Filters.in("values.node", qUuid)));
                runsIter.projection(new Document("_id", 0));
                MongoCursor<Document> runsCursor = runsIter.iterator();

                while (runsCursor.hasNext()) {
                    Document runsDocument = runsCursor.next();
                    Object values = runsDocument.get("values");
                    String contact = (String) runsDocument.get("contact");
                    if (values instanceof ArrayList) {
                        ArrayList<Document> valuesArray = (ArrayList<Document>) values;
                        for (Document value : valuesArray) {
                            Document category = (Document) value.get("category");
                            //String date = null;

                            String qLabel = (String) value.get("label");
                            String qNode = (String) value.get("node");
                            if(qUuid.size() != 0 && !qUuid.contains(qNode))
                                continue;

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

                            String qVal = category.get("base") != null ? category.getString("base") : category.getString("eng");
                            if (qVal.equals("numeric") && value.containsKey("value")) {
                                qVal = "" + value.get("value");
                            }
                            if (qVal.equalsIgnoreCase("Other") && value.containsKey("value")) {
                                qVal += "-" + ("" + value.get("value"));
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
                    }
                }
            }

            currEnd = currBeg;
        }

        Collections.sort(weekLabels);
        for (String contact : qMap.keySet()) {
            FindIterable<Document> contactsIter = contactsCollection.find(Filters.eq("uuid", contact));
            Document contactDoc = contactsIter.first();
            JSONObject contactObj = new JSONObject();
            if (contactDoc != null && contactDoc.containsKey("name")) {
                contactObj.put("name", contactDoc.get("name"));
            }
            if (contactDoc != null && contactDoc.containsKey("phone")) {
                contactObj.put("phone", contactDoc.get("phone"));
            }
            contactObj.put("uuid", contact);
            if(qType != null && qType.size() != 0) {
                for (String weekLabel : weekLabels) {  // if qType is provided
                    String answer = "-";
                    if (qMap.get(contact).containsKey(weekLabel)) {
                        HashMap<String, ArrayList<String>> answers = qMap.get(contact).get(weekLabel);
                        int i = 0;
                        for(ArrayList<String> answerArray : answers.values()){
                            if(i == 0) {
                                answer = StringUtils.join(answerArray, " | ");
                            } else {
                                answer += " | " + StringUtils.join(answerArray, " | ");
                            }
                            i++;
                        }
                    }
                    contactObj.put(weekLabel.split(":")[0].trim(), answer);
                }
            } else {
                contactObj.put("results", new JSONObject(qMap.get(contact)));
            }
            array.put(contactObj);
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }

    public ArrayList<Document> getFlowsByDeploymentDate(MongoCollection<Document> flowsCollection,
                                                        MongoCollection<Document> runsCollection,
                                                        Bson filter,  String fromDate, String toDate) throws ParseException {

        ArrayList<Document> flows = new ArrayList<Document>();
        Date fromDay = null;
        Date toDay = null;
        String toDateStrMm = "";
        String toDateStrM = "";
        String fromDateStrMm = "";
        String fromDateStrM = "";

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
        if (fromDate != null) {
            fromDay = df_Z.parse(fromDate);
            fromDateStrMm = df_dmmy.format(fromDay);
            fromDateStrM = df_dmy.format(fromDay);
            Calendar cal = Calendar.getInstance();
            cal.setTime(df_Z.parse(fromDate));
            cal.add(Calendar.DATE, -daysBeforeFlowDeployment);
            String dateBefore = df_Z.format(cal.getTime());
            obj.add(new BasicDBObject("created_on", new BasicDBObject("$gte", dateBefore)));
        }
        if (toDate != null) {
            toDay = df_Z.parse(toDate);
            toDateStrMm = df_dmmy.format(toDay);
            toDateStrM = df_dmy.format(toDay);
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

            // this filter will exclude flows that have been created for the next week
            // if queried toDate is exactly on the flow deployment date
            if(toDate != null && include == true
                    && (flowsDocument.getString("name").contains(toDateStrMm) || flowsDocument.getString("name").contains(toDateStrM)))
                include = false;

            // this filter will include flows that have been created for this week
            // if queried fromDate is exactly on the flow deployment date
            if(fromDate != null && include == false
                    && (flowsDocument.getString("name").contains(fromDateStrM) || flowsDocument.getString("name").contains(fromDateStrMm)))
                include = true;

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