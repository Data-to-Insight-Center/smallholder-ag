package edu.indiana.d2i.textit;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TextItWebHook {
	private static Logger logger = Logger.getLogger(TextItWebHook.class);
	private static TextItWebHook instance = null;
	
	private final java.util.Properties properties;
	private Server server = null;
	private final int PORT;
	
	private Properties getProperties() throws IOException {
		// load the property file
		final Properties properties = new Properties();
		InputStream stream = TextItClient.class.getClassLoader().
				getResourceAsStream("config.properties");
		if (stream == null) {
			throw new RuntimeException("config.properties is not found!");
		}
		properties.load(stream);
		stream.close();
		
		return properties;
	}
	
	private TextItWebHook(int port) throws IOException {
		this.PORT = port;
		this.properties = getProperties();
	}
	
	/** unit test purpose */
	private TextItWebHook(java.util.Properties properties, int port) {
		this.PORT = port;
		this.properties = properties;		
	}
	protected synchronized static TextItWebHook getTestInstance(
			java.util.Properties properties, int port) {
		return new TextItWebHook(properties, port);
	}
	/** end of unit test features 
	 * @throws IOException */
	
	public synchronized static TextItWebHook getSingleton(Properties properties, int port) throws IOException {
		if (instance == null) {
			instance = new TextItWebHook(properties, port);
		}
		return instance;
	}
	
	@SuppressWarnings("serial")
	class WebHookServlet extends HttpServlet {
		
		@Override
		public void init(ServletConfig config) throws ServletException {
			
		}
		
//		@Override
//		protected void doGet(HttpServletRequest request, HttpServletResponse response) 
//				throws ServletException, IOException {
//			// Set response content type
//      response.setContentType("text/html");
//
//      // Actual logic goes here.
//      java.io.PrintWriter out = response.getWriter();
//      out.println("<h1>" + "Hello World!!!" + "</h1>");
//		}
		
		@Override
		protected void doPost(HttpServletRequest request, HttpServletResponse response) 
				throws ServletException, IOException {		
			// parse the request from TextIt
			BufferedReader reader = request.getReader();
			StringBuilder buffer = new StringBuilder();
			String line = null;
			while ((line = reader.readLine()) != null) {
				buffer.append(line);
			}
			logger.debug("TextIt sent: " + buffer.toString());
			
			// split by & to get flow id and values
			// sample request: event=flow&relayer=254&relayer_phone=%2B250788111111&phone=%2B250788123123&flow=1524&step=12341234-1234-1234-1234-1234-12341234&values=[]
			String runInfo = null;
			String flowID = null;
			String[] items = buffer.toString().split("&");
			for (String item : items) {
				String[] kv = item.split("=");
				if (kv.length == 2 && kv[0].equals("values")) {
					runInfo = kv[1];
				} else if (kv.length == 2 && kv[0].equals("flow")) {
					flowID = kv[1];
				}
			}
			
			if (flowID == null || runInfo == null) {
				logger.warn("Does not receive flow id or json value.");
				return ;
			}
			
			// download a new flow data or stop
			TextItClient client = (properties != null) ? 
					TextItClient.custom().setProperties(properties).build(): // unit test purpose
			    TextItClient.custom().setWorkerNum(1).build();
			client.downloadRunsByFlowID(flowID);
			client.close();
			
			
//			// decode the json string
//			String result = java.net.URLDecoder.decode(runInfo, "UTF-8");
//			
//			// save the json string in a json file
//			final String timestamp = new SimpleDateFormat("yyyyMMdd").format(new Date());	
//			String outputDir = properties.getProperty("textit.downloader.outputdir", "./output");
//			BufferedWriter writer = new BufferedWriter(new FileWriter(Paths.get(outputDir, 
//					String.format("%s-%s-%d.json", timestamp, flowID, 1)).toFile()));
//			writer.write(result);
//			writer.close();
		}
	}
	
	public void start() throws Exception {
		ServletHolder sh = new ServletHolder(new WebHookServlet());

		server = new Server(this.PORT);
		ServletContextHandler context = new ServletContextHandler(
				server, "/", ServletContextHandler.SESSIONS);
		context.addServlet(sh, "/notify");
		server.start();
		
		logger.info("The call back service is running.");
	}
	
	public void stop() throws Exception {
		if (server != null) {
			server.stop();
		}
	}
}

