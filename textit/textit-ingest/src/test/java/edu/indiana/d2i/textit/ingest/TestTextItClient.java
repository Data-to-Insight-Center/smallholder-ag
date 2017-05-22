package edu.indiana.d2i.textit.ingest;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.FileEntity;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestTextItClient {
    private LocalTestServer server = null;
    private Properties properties = new Properties();

    class FlowRequestHandler implements HttpRequestHandler {

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                throws HttpException, IOException {
            String path = TestTextItClient.class.getClassLoader().
                    getResource("flows.json").getPath().toString();
            response.setEntity(new FileEntity(new File(path)));
        }
    }

    class RunRequestHandler implements HttpRequestHandler {

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                throws HttpException, IOException {
            try {
                URIBuilder builder = new URIBuilder(request.getRequestLine().getUri());
                List<NameValuePair> params = builder.getQueryParams();
                String flowId = null;
                for (NameValuePair param : params) {
                    if (param.getName().equals("flow_uuid")) {
                        flowId = param.getValue();
                        break;
                    }
                }

                String name = (flowId.equals("XXX-XXX-01")) ? "runs-1-1.json" : "runs-2-1.json";
                String path = TestTextItClient.class.getClassLoader().
                        getResource(name).getPath().toString();
                response.setEntity(new FileEntity(new File(path)));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
    }

    @Before
    public void setup() throws Exception {
        server = new LocalTestServer(null, null);
        server.register("/flows.json", new FlowRequestHandler());
        server.register("/runs.json", new RunRequestHandler());
        server.start();

        String serverUrl = "http:/" + server.getServiceAddress();
        System.out.println("Local test server is at: " + serverUrl);

        // because LocalTestServer uses a random port, we have to set the endpoint on the fly
        properties.setProperty("textit.epr", serverUrl);
        properties.setProperty("textit.token", "XYZ");
        properties.setProperty("textit.downloader.outputdir", "./tmp");
    }

    @After
    public void clearup() throws Exception {
        server.stop();
    }

    @Test
    public void testGetFlowIDs() throws IOException, ParseException {
        TextItClient client = TextItClient.createClient(properties);
        List<String> flowIDs = client.getCreatedFlowIDs();

        Assert.assertEquals(2, flowIDs.size());
        for (int i = 0; i < flowIDs.size(); i++) {
            Assert.assertEquals("XXX-XXX-0" + (i + 1), flowIDs.get(i));
        }
    }

    @Test
    public void testGetRuns() throws IOException, ParseException {
        TextItClient client = TextItClient.createClient(properties);
        List<String> flowIDs = client.getCreatedFlowIDs();
        client.downloadData(null, flowIDs);

        // TODO: check the directory
        FileUtils.deleteQuietly(Paths.get("./tmp").toFile());
    }

/*	@Test
	public void testWebHook() throws Exception {
		properties.setProperty("textit.downloader.outputdir", "./out");

		TextItWebHook hook = TextItWebHook.getTestInstance(properties, 9000);
		hook.start();

		// make a post
		HttpClient client = HttpClientBuilder.create().build();
		HttpPost request = new HttpPost("http://127.0.0.1:9000/notify");
		List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
		urlParameters.add(new BasicNameValuePair("event", "flow"));
		urlParameters.add(new BasicNameValuePair("relayer", "254"));
		urlParameters.add(new BasicNameValuePair("relayer_phone", "250788111111"));
		urlParameters.add(new BasicNameValuePair("flow", "XXX-XXX-0"));
		urlParameters.add(new BasicNameValuePair("phone", "12345"));
		urlParameters.add(new BasicNameValuePair("step", "12341234-1234-1234-1234-1234-12341234"));
		request.setEntity(new UrlEncodedFormEntity(urlParameters));

		client.execute(request);

		hook.stop();

		// TODO: check the directory
		FileUtils.deleteQuietly(Paths.get("./out").toFile());
	}*/

    @Test
    public void testQuestionLabels() throws IOException {

        //final String timestamp_prev = "2013-12-15"; //Zambia start date
        final String timestamp_prev = "2015-03-01"; //Kenya start date
        final String timestamp_now = "2016-09-01";

        String TEXT_TIME = "00:00:00.000";
        String FLOWS_URL = "https://textit.in/api/v1/flows.json";
        String RUNS_URL = "https://textit.in/api/v1/runs.json";
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        final String OUTPUT_FILE = "./output.txt";
        final String FLOWS = "flows";

        //TextItUtils utils = TextItUtils.createUtils("apiKey", "Africa/Zambia");
        TextItUtils utils = TextItUtils.createUtils("apiKey", "Africa/Kenya");

        final List<String> res = new ArrayList<String>();
        final Map<String, String> types = new HashMap<String, String>();

        URL target = null;
        target = new URL(FLOWS_URL.toString() + "?after=" + timestamp_prev
                + "T" + TEXT_TIME + "&&" + "before=" + timestamp_now
                + "T" + TEXT_TIME);

        final String timestamp_final = timestamp_prev.replace("-", "_") + "-"
                + timestamp_now.replace("-", "_");

        utils.processData(target, new TextItUtils.IJsonProcessor() {
            @Override
            public void process(Map<String, Object> data, int pageNum)
                    throws IOException {
                List<Object> results = (List<Object>) data.get("results");
                for (Object result : results) {
                    Map<Object, Object> map = (Map<Object, Object>) result;
                    if (map.get("uuid") != null) {
                        res.add((String) map.get("uuid"));
                    }

                    List<Object> steps = (List<Object>) map.get("rulesets");
                    for (Object step : steps) {
                        Map<Object, Object> stepObject = (Map<Object, Object>) step;
                        types.put((String) stepObject.get("node"), (String) stepObject.get("label"));
                    }
                }
                //ObjectMapper objectMapper = new ObjectMapper();
                //objectMapper.writeValue(Paths.get(OUTPUT_FILE).toFile(), data);
            }
        });

        Writer output = new BufferedWriter(new FileWriter(OUTPUT_FILE, true));
        output.write("Flow ID|node ID|Question Text|Type|Left On\n");
        output.flush();
        output.close();

        for (final String flowId : res) {
            URL runUrl = new URL(RUNS_URL + "?flow_uuid=" + flowId);
            utils.processData(runUrl, new TextItUtils.IJsonProcessor() {
                @Override
                public void process(Map<String, Object> data, int pageNum)
                        throws IOException {
                    List<Object> results = (List<Object>) data.get("results");
                    String csv = "";
                    for (Object result : results) {
                        Map<Object, Object> map = (Map<Object, Object>) result;

                        List<Object> steps = (List<Object>) map.get("steps");
                        String qText = "";
                        String leftOn = "";
                        int count = 1;
                        for (Object step : steps) {
                            Map<Object, Object> stepObject = (Map<Object, Object>) step;
                            if (((String) stepObject.get("type")).equals("A")) {
                                if (steps.size() == count)
                                    break;
                                qText = (String) stepObject.get("text");
                                leftOn = (String) stepObject.get("left_on");
                            } else {
                                String node = (String) stepObject.get("node");
                                csv += flowId + "|" + node + "|" + qText + "|" + types.get(node) + "|" + leftOn + "\n";
                            }
                            count++;
                        }
                    }
                    Writer output = new BufferedWriter(new FileWriter(OUTPUT_FILE, true));
                    output.write(csv);
                    output.flush();
                    output.close();
                }
            });
        }

        utils.close();
    }


    @Test
    public void testFlowNames() throws IOException, ParseException {

        //final String timestamp_prev = "2015-09-28T11:00:00.000Z"; //Zambia start date
        final String timestamp_prev = "2015-02-28T06:00:00.000Z"; //Kenya start date
        //final String timestamp_now = "2016-09-26T11:00:00.000Z";
        final String timestamp_now = "2016-09-24T06:00:00.000Z";// Kenya

        String FLOWS_URL = "https://textit.in/api/v1/flows.json";
        DateFormat df_Z = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        final String OUTPUT_FILE = "./output.txt";
        final String FLOWS = "flows";

        //TextItUtils utils = TextItUtils.createUtils("apiKey", "Africa/Zambia");
        TextItUtils utils = TextItUtils.createUtils("apiKey", "Africa/Kenya");

        final List<String> res = new ArrayList<String>();
        final Map<String, String> types = new HashMap<String, String>();

        URL target = null;
        target = new URL(FLOWS_URL.toString() + "?after=" + timestamp_prev + "&" + "before=" + timestamp_now);


        utils.processData(target, new TextItUtils.IJsonProcessor() {
            @Override
            public void process(Map<String, Object> data, int pageNum)
                    throws IOException {
                List<Object> results = (List<Object>) data.get("results");
                for (Object result : results) {
                    Map<Object, Object> map = (Map<Object, Object>) result;
                    List<Object> steps = (List<Object>) map.get("rulesets");
                    String labels = "";
                    for (Object step : steps) {
                        Map<Object, Object> stepObject = (Map<Object, Object>) step;
                        labels += " ## " + stepObject.get("label");
                    }
                    res.add(map.get("uuid") + "|" + map.get("name") + "|" + map.get("created_on") + "|" + map.get("runs") + "|" + map.get("completed_runs") + "|" + labels);
                }
                //ObjectMapper objectMapper = new ObjectMapper();
                //objectMapper.writeValue(Paths.get(OUTPUT_FILE).toFile(), data);
            }
        });

        System.out.println(StringUtils.join(res, "\n"));

        utils.close();
    }

    @Test
    public void abc() throws IOException, ParseException, JSONException {
        String csvFile = "/Users/charmadu/Downloads/pw.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        List<String> ids = Arrays.asList("uuid","2015 W09","2015 W10","2015 W11","2015 W12","2015 W13","2015 W14",
                "2015 W15","2015 W16","2015 W17","2015 W18","2015 W19","2015 W20","2015 W21","2015 W22","2015 W23",
                "2015 W24","2015 W25","2015 W26","2015 W27","2015 W28","2015 W29","2015 W30","2015 W31","2015 W32",
                "2015 W33","2015 W34","2015 W35","2015 W36","2015 W37","2015 W38","2015 W39","2015 W40","2015 W41",
                "2015 W42","2015 W43","2015 W44","2015 W45","2015 W46","2015 W47","2015 W48","2015 W49","2015 W50",
                "2015 W51","2015 W52","2016 W01","2016 W02","2016 W03","2016 W04","2016 W05","2016 W06","2016 W07",
                "2016 W08","2016 W09","2016 W10","2016 W11","2016 W12","2016 W13","2016 W14","2016 W15","2016 W16",
                "2016 W17","2016 W18","2016 W19","2016 W20","2016 W21","2016 W22","2016 W23","2016 W24","2016 W25",
                "2016 W26","2016 W27","2016 W28","2016 W29","2016 W30","2016 W31","2016 W32","2016 W33","2016 W34",
                "2016 W35","2016 W36","2016 W37","2016 W38","2016 W39","2016 W40","2016 W41","2016 W42","2016 W43",
                "2016 W44","2016 W45","2016 W46","2016 W47","2016 W48","2016 W49","2016 W50","2016 W51","2016 W52",
                "2017 W01","2017 W02","2017 W03","2017 W04","2017 W05","2017 W06","2017 W07","2017 W08","2017 W09",
                "2017 W10","2017 W11","2017 W12","2017 W13","2017 W14","2017 W15","2017 W16"

        );
        Map<String, JSONObject> map = new HashMap<>();
        String temp = null;

        try {

            br = new BufferedReader(new FileReader(csvFile));
            line = br.readLine();
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] country = line.split(cvsSplitBy);
                for(int i = 1 ; i < country.length ; i++) {
                    map.put(country[0] + "-" + ids.get(i), new JSONObject().put("piecework",country[i]));
                    temp = "piecework-" + country[0] + "-" + ids.get(i);
                }
            }

            br = new BufferedReader(new FileReader("/Users/charmadu/Downloads/str.csv"));
            line = br.readLine();
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] country = line.split(cvsSplitBy);
                for(int i = 1 ; i < country.length ; i++) {
                    if(map.containsKey(country[0] + "-" + ids.get(i))){
                        map.get(country[0] + "-" + ids.get(i)).put("storage", country[i]);
                    }
                    temp = "storage-" + country[0] + "-" + ids.get(i);
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println(temp);
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        final String OUTPUT_FILE = "/Users/charmadu/Downloads/output.csv";


        System.out.println("piecework,storage");
        Writer output = new BufferedWriter(new FileWriter(OUTPUT_FILE, true));
        output.write("piecework,storage\n");
        output.flush();
        output.close();


        for(String key : map.keySet()) {
            JSONObject val = map.get(key);
            if(val.has("piecework") && val.has("storage")  && !val.getString("piecework").equals("-") && !val.getString("storage").equals("-")){
                //System.out.println(key + " - " + val);
                //System.out.println(val.get("piecework") + "," + val.get("storage"));

                String pw = val.getString("piecework").replace("Other-","");
                String storage = val.getString("storage").replace("Other-","");

                if(pw.trim().equals("0")
                        || pw.trim().equalsIgnoreCase("no")
                        || pw.trim().equalsIgnoreCase("0 day")
                        || pw.trim().equalsIgnoreCase("0 days")
                        || pw.trim().matches("(?i:.*non.*)")
                        || pw.trim().matches("(?i:.*nothing.*)")
                        || pw.trim().matches("(?i:.*nil.*)")
                        || pw.trim().matches("(?i:.*not.*)")
                        || pw.trim().matches("(?i:.*zero.*)")
                        || pw.trim().matches("(?i:.*zelo.*)")
                        || pw.trim().matches("(?i:.*not.*)")
                        || pw.trim().equalsIgnoreCase("o"))
                    pw = "No";
                 else if(pw.trim().matches("\\d+(\\.\\d+)?")
                        || pw.trim().equalsIgnoreCase("yes")
                        || pw.trim().equalsIgnoreCase("1 day")
                        || pw.trim().equalsIgnoreCase("1 days")
                        || pw.trim().matches("(?i:.*one.*)")
                        || pw.trim().matches("(?i:.*two.*)")
                        || pw.trim().matches("(?i:.*three.*)")
                        || pw.trim().matches("(?i:.*four.*)")
                        || pw.trim().matches("(?i:.*five.*)")
                        || pw.trim().matches("(?i:.*six.*)"))
                    pw = "Yes";

                String csv = pw +
                        "," +
                        storage +
                        "\n";

                output = new BufferedWriter(new FileWriter(OUTPUT_FILE, true));
                output.write(csv);
                output.flush();
                output.close();
            }
        }
        System.out.println("done");

    }


}
