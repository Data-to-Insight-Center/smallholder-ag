package edu.indiana.d2i.textit.service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import edu.indiana.d2i.textit.utils.MongoDB;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.*;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kunarath on 2/15/16.
 */

@Path("/")
public class TextItRest {
    private MongoCollection<Document> flowsCollection = null;
    private MongoCollection<Document> runsCollection = null;
    private MongoCollection<Document> contactsCollection = null;
    private CacheControl control = new CacheControl();
    private SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @GET
    @Path("/{country}/flows")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAllFlows(@PathParam("country") String country,
                                @QueryParam("flowId") String flowId,
                                @QueryParam("from") String fromDate,
                                @QueryParam("to") String toDate) {

        if (country.equals("zambia")){
            MongoDatabase db1 = MongoDB.getServicesDB1();
            flowsCollection = db1.getCollection(MongoDB.flowsObjects);
        }else if (country.equals("kenya")){
            MongoDatabase db2 = MongoDB.getServicesDB2();
            flowsCollection = db2.getCollection(MongoDB.flowsObjects);
        }
        control.setNoCache(true);

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

        FindIterable<Document> iter = flowsCollection.find(andQuery);
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
    public Response getAllRuns(@PathParam("country") String country) {
        if (country.equals("zambia")){
            MongoDatabase db1 = MongoDB.getServicesDB1();
            runsCollection = db1.getCollection(MongoDB.runsObjects);
        }else if (country.equals("kenya")){
            MongoDatabase db2 = MongoDB.getServicesDB2();
            runsCollection = db2.getCollection(MongoDB.runsObjects);
        }
        control.setNoCache(true);

        FindIterable<Document> iter = runsCollection.find();
        iter.projection(new Document("runs", 1)
                .append("flow_uuid", 1).append("flow", 1)
                .append("contact", 1)
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
    public Response getAllContacts(@PathParam("country") String country) {
        if (country.equals("zambia")){
            MongoDatabase db1 = MongoDB.getServicesDB1();
            contactsCollection = db1.getCollection(MongoDB.contactsObjects);
        }else if (country.equals("kenya")){
            MongoDatabase db2 = MongoDB.getServicesDB2();
            contactsCollection = db2.getCollection(MongoDB.contactsObjects);
        }
        control.setNoCache(true);

        FindIterable<Document> iter = contactsCollection.find();
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
    public Response getAllData(@PathParam("country") String country) {
        if (country.equals("zambia")){
            MongoDatabase db1 = MongoDB.getServicesDB1();
            flowsCollection = db1.getCollection(MongoDB.flowsObjects);
            runsCollection = db1.getCollection(MongoDB.runsObjects);
            contactsCollection = db1.getCollection(MongoDB.contactsObjects);
        }else if (country.equals("kenya")){
            MongoDatabase db2 = MongoDB.getServicesDB2();
            flowsCollection = db2.getCollection(MongoDB.flowsObjects);
            runsCollection = db2.getCollection(MongoDB.runsObjects);
            contactsCollection = db2.getCollection(MongoDB.contactsObjects);
        }
        control.setNoCache(true);

        JSONArray array = new JSONArray();

        FindIterable<Document> iter = flowsCollection.find();
        MongoCursor<Document> cursor = iter.iterator();

        while (cursor.hasNext()) {
            Document document = cursor.next();
            String flow_uuid = (String) document.get("uuid");

            FindIterable<Document> iter1 = runsCollection.find(new BasicDBObject("flow_uuid", flow_uuid));
            MongoCursor<Document> cursor1 = iter1.iterator();

            while (cursor1.hasNext()) {
                Document document1 = cursor1.next();

                JSONObject new_list = new JSONObject();
                Object ques = document1.get("steps");
                Object values = document1.get("values");
                if (ques instanceof ArrayList) {
                    ArrayList<Document> quesIds = (ArrayList<Document>) ques;
                    ArrayList<Document> quesNames = (ArrayList<Document>) values;
                    for (Document node : quesIds) {
                        node.getString("node");
                    }
                    new_list.put("steps", quesIds);
                    new_list.put("values", quesNames);
                }

                String contactId = document1.getString("contact");
                new_list.put("flow_name", document.getString("name"));
                new_list.put("flow", document1.getInteger("flow"));
                new_list.put("run", document1.getInteger("run"));
                new_list.put("created_on", document1.getString("created_on"));

                FindIterable<Document> iter2 = contactsCollection.find(new BasicDBObject("uuid", contactId));
                MongoCursor<Document> cursor2 = iter2.iterator();

                while (cursor2.hasNext()) {
                    Document document2 = cursor2.next();
                    new_list.put("contact_name", document2.getString("name"));
                    new_list.put("contact_phone", document2.getString("phone"));
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
                                      @QueryParam("from") String fromDate,
                                      @QueryParam("to") String toDate) {
        if (country.equals("zambia")){
            MongoDatabase db1 = MongoDB.getServicesDB1();
            flowsCollection = db1.getCollection(MongoDB.flowsObjects);
            runsCollection = db1.getCollection(MongoDB.runsObjects);
        }else if (country.equals("kenya")){
            MongoDatabase db2 = MongoDB.getServicesDB2();
            flowsCollection = db2.getCollection(MongoDB.flowsObjects);
            runsCollection = db2.getCollection(MongoDB.runsObjects);
        }
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

        FindIterable<Document> iter = flowsCollection.find(andQuery);
        MongoCursor<Document> cursor = iter.iterator();

        while (cursor.hasNext()) {
            Document document = cursor.next();
            String flow_uuid = (String) document.get("uuid");

            FindIterable<Document> iter1 = runsCollection.find(new BasicDBObject("flow_uuid", flow_uuid));
            MongoCursor<Document> cursor1 = iter1.iterator();

            while (cursor1.hasNext()) {
                Document document1 = cursor1.next();

                JSONObject new_list = new JSONObject();

                Object ques = document1.get("steps");
                Object values = document1.get("values");
                if (ques instanceof ArrayList) {
                    ArrayList<Document> quesIds = (ArrayList<Document>) ques;
                    ArrayList<Document> quesNames = (ArrayList<Document>) values;
                    for (Document node : quesIds) {
                        node.getString("node");
                    }
                    new_list.put("steps", quesIds);
                    new_list.put("values", quesNames);
                }
                new_list.put("flow_name", document.getString("name"));
                new_list.put("flow", document1.getInteger("flow"));
                new_list.put("run", document1.getInteger("run"));
                new_list.put("created_on", document1.getString("created_on"));

                array.put(new_list);
            }
        }

        return Response.ok(array.toString()).cacheControl(control).build();
    }
}
