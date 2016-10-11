package edu.indiana.d2i.textit.analyze;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by charmadu on 10/11/16.
 */
public class AnalyzerUtils {

    private SimpleDateFormat df_Z = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private SimpleDateFormat df_dmmy = new SimpleDateFormat("d MMMMM yyyy");
    private SimpleDateFormat df_dmy = new SimpleDateFormat("d MMM yyyy");

    private static int daysBeforeFlowDeployment = 16;

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
}
