package edu.indiana.d2i.textit.ingest.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

public final class TextItUtils {
	private static Logger logger = Logger.getLogger(TextItUtils.class);

	public static interface IJsonProcessor {
		public void process(Map<String, Object> data, int pageNum)
				throws IOException;
	}

	/** properties loaded from the properties file */
	private final String TOKEN, TIMEZONE;
	private final CloseableHttpClient httpclient;

	private TextItUtils(String token, String timezone) throws IOException {
		TOKEN = token;
		TIMEZONE = timezone;

		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setDefaultMaxPerRoute(20);
		httpclient = HttpClients.custom().setConnectionManager(cm).build();
	}

	public static TextItUtils createUtils(String token, String timezone)
			throws IOException {
		return new TextItUtils(token, timezone);
	}

	@SuppressWarnings("unchecked")
	public void processData(URL startPointer, IJsonProcessor processor) throws IOException {
		String next = null;
		int pageNum = 1;
		do {
            HttpGet request = (next != null) ? new HttpGet(
                    new URL(next).toString()) : new HttpGet(
                    startPointer.toString());
            request.addHeader("Authorization", "Token " + TOKEN);
            logger.info("Request URL : "
                    + request.getURI().toString() + " for " + TIMEZONE);

            CloseableHttpResponse response = httpclient.execute(request);
            try {
                StatusLine statusLine = response.getStatusLine();
                HttpEntity entity = response.getEntity();
                if (statusLine.getStatusCode() >= 300) {
                    logger.error("Error is " + new HttpResponseException(statusLine.getStatusCode(),
                            statusLine.getReasonPhrase()));
                    throw new HttpResponseException(statusLine.getStatusCode(),
                            statusLine.getReasonPhrase());
                }
                if (entity == null) {
                    logger.error(new ClientProtocolException(
                            "Response contains no content"));
                    throw new ClientProtocolException(
                            "Response contains no content");
                }

                InputStream stream = entity.getContent();
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS,
                        true);
                Map<String, Object> data = mapper.readValue(stream, Map.class);
                next = (data.get("next") != null) ? next = (String) data
                        .get("next") : null;

                if (data.get("results") != null) {
                    processor.process(data, pageNum++);
                }
                stream.close();
            } finally {
                response.close();
			}
		} while (next != null);
	}

	public void close() throws IOException {
		httpclient.close();
	}
}

