package edu.indiana.d2i.textit;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

import junit.framework.Assert;

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
}
