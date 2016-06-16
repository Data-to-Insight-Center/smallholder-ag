package edu.indiana.d2i.textit.api.service;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import edu.indiana.d2i.textit.api.utils.MongoDB;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
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

    private CacheControl control = new CacheControl();
    private SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static Logger logger = Logger.getLogger(TextItRest.class);

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
    public Response getAllRuns(@PathParam("country") String country,
                               @QueryParam("contact") String contactId,
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
        if (contactId != null) {
            obj.add(new BasicDBObject("contact", contactId));
        }
        if (obj.size() != 0) {
            andQuery.put("$and", obj);
        }

        FindIterable<Document> iter = runsCollection.find(andQuery);
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
        MongoCollection<Document> contactsCollection = db.getCollection(MongoDB.contactsCollection);
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

        FindIterable<Document> iter = runsCollection.find(andQuery);
        MongoCursor<Document> cursor = iter.iterator();

        while (cursor.hasNext()) {
            Document document = cursor.next();
            String contact_id = (String) document.get("contact");

            FindIterable<Document> iter1 = contactsCollection.find(new BasicDBObject("uuid", contact_id));
            MongoCursor<Document> cursor1 = iter1.iterator();

            while (cursor1.hasNext()) {
                Document document1 = cursor1.next();

                JSONObject new_list = new JSONObject();

                new_list.put("contact_id", document.getString("contact"));
                new_list.put("run_id", document.getString("flow_uuid"));
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
}
