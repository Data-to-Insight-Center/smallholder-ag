package edu.indiana.d2i.textit.ingest;

import edu.indiana.d2i.textit.ingest.utils.TextItUtils;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.localserver.LocalTestServer;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.text.DateFormat;
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
				
				String name = (flowId.equals("XXX-XXX-01")) ? "runs-1-1.json": "runs-2-1.json";
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
	public void testGetFlowIDs() throws IOException {
		TextItClient client = TextItClient.createClient(properties);
		List<String> flowIDs = client.getFlowIDs();
		
		Assert.assertEquals(2, flowIDs.size());
		for (int i = 0; i < flowIDs.size(); i++) {
			Assert.assertEquals("XXX-XXX-0" + (i+1), flowIDs.get(i));
		}
	}
	
	@Test
	public void testGetRuns() throws IOException {
		TextItClient client = TextItClient.createClient(properties);
		List<String> flowIDs = client.getFlowIDs();
		client.downloadData(null, flowIDs);
		
		// TODO: check the directory
		FileUtils.deleteQuietly(Paths.get("./tmp").toFile());
	}
	
	@Test
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
	}

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
                        types.put((String)stepObject.get("node"), (String)stepObject.get("label"));
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

        for(final String flowId : res) {
            URL runUrl = new URL(RUNS_URL + "?flow_uuid=" + flowId);
            utils.processData( runUrl , new TextItUtils.IJsonProcessor() {
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
                            if(((String)stepObject.get("type")).equals("A")) {
                                if(steps.size() == count)
                                    break;
                                qText = (String)stepObject.get("text");
                                leftOn = (String)stepObject.get("left_on");
                            } else {
                                String node = (String)stepObject.get("node");
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
}
