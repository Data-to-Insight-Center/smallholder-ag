package edu.indiana.d2i.textit.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.indiana.d2i.textit.ingest.utils.MongoDB;
import edu.indiana.d2i.textit.ingest.utils.TextItUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class TextItClient {
	private static Logger logger = Logger.getLogger(TextItClient.class);

	/** properties loaded from the properties file */
	private final String TOKEN;
	private final String TIMEZONE;
	private final int WORKER_NUM, NO_OF_DAYS;
	private final String OUTPUT_DIRECTORY;
	private final URL TEXTIT_BASE_URL;
	private final URL GET_FLOWS_URL;
	private final URL GET_RUNS_URL;
	private final URL GET_CONTACTS_URL;
	private final String START_DATE;
	private final String END_DATE;
	private final String TEXT_TIME;
	private final String INTERVAL;

    public static final String FLOWS = "flows";
    public static final String RUNS = "runs";
    public static final String CONTACTS = "contacts";
    public static final String STATS = "stats";

	/** utilities for parsing responses from TextIt */
	private final TextItUtils utils;

    private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

	/** multi-threaded downloader */
	class ThreadedDownloader {
		private final BlockingQueue<String> queue;
		private final Thread[] workers;
		private final int expectedCount;
		private AtomicInteger finishedCount = new AtomicInteger(0);

		private String flowParam = "?flow_uuid=";

		class Worker implements Runnable {
			@Override
			public void run() {
				String flowid = null;
				while ((flowid = queue.poll()) != null) {
					try {

						URL target = new URL(GET_RUNS_URL.toString()
								+ flowParam + flowid);
						final String id = flowid;
						logger.info("Downloading runs for flow : " + flowid);

						final String timestamp = df.format(new DateTime(END_DATE).toDate()).replace("-", "_");
						utils.processData(target,
								new TextItUtils.IJsonProcessor() {
									@Override
									public void process(
											Map<String, Object> data,
											int pageNum) throws IOException {
										ObjectMapper objectMapper = new ObjectMapper();
										objectMapper
												.writeValue(
														Paths.get(
																OUTPUT_DIRECTORY + "/" + RUNS,
																String.format(
																		"%s-%s-%d-" + RUNS + ".json",
																		timestamp,
																		id,
																		pageNum))
																.toFile(), data);
									}
								});

						finishedCount.incrementAndGet();
					} catch (IOException e) {
						logger.warn("There are errors while downloading runs for flow "
								+ flowid, e);
					}
				}
			}
		}

		public ThreadedDownloader(List<String> flowIDs, int workerNum,
				String param) {
			queue = new LinkedBlockingQueue<>(flowIDs);
			workers = new Thread[workerNum];
			expectedCount = flowIDs.size();
			if (param != null) {
				flowParam = param;
			}
		}

		public void execute() throws InterruptedException {
			long startT = System.currentTimeMillis();
			for (int i = 0; i < workers.length; i++) {
				workers[i] = new Thread(new Worker());
				workers[i].start();
			}

			for (Thread worker : workers) {
				worker.join();
			}

			long duration = System.currentTimeMillis() - startT;
			logger.info("Expected runs for " + expectedCount + " flows, actually downloaded runs for " + finishedCount.get() + " flows");
            logger.info("Time to download total runs : " + duration / 1000.0);
		}
	}

	private TextItClient(String token, String outputDir, String epr, int workerNum, String timezone, int no_of_days,
                         String start_date, String end_date, String textTime, String interval) throws IOException {
        df.setTimeZone(TimeZone.getTimeZone("timezone"));

        TOKEN = token;
		TIMEZONE = timezone;
		NO_OF_DAYS = no_of_days;
        START_DATE = start_date;
        END_DATE = end_date;
        TEXT_TIME = textTime;
		OUTPUT_DIRECTORY = outputDir;
        INTERVAL = interval;

		String textitEpr = epr;
		WORKER_NUM = workerNum;
		if (TOKEN == null) {
			throw new IllegalArgumentException("Textit token is missing");
		}
		if (TIMEZONE == null) {
			throw new IllegalArgumentException("Textit timezone is missing");
		}

		TEXTIT_BASE_URL = new URL(textitEpr.replaceAll("/$", ""));
		GET_FLOWS_URL = new URL(TEXTIT_BASE_URL.getProtocol(),
				TEXTIT_BASE_URL.getHost(), TEXTIT_BASE_URL.getPort(),
				TEXTIT_BASE_URL.getFile() + "/flows.json", null);
		GET_RUNS_URL = new URL(TEXTIT_BASE_URL.getProtocol(),
				TEXTIT_BASE_URL.getHost(), TEXTIT_BASE_URL.getPort(),
				TEXTIT_BASE_URL.getFile() + "/runs.json", null);
		GET_CONTACTS_URL = new URL(TEXTIT_BASE_URL.getProtocol(),
				TEXTIT_BASE_URL.getHost(), TEXTIT_BASE_URL.getPort(),
				TEXTIT_BASE_URL.getFile() + "/contacts.json", null);

		Files.createDirectories(Paths.get(OUTPUT_DIRECTORY + "/" + FLOWS));
		Files.createDirectories(Paths.get(OUTPUT_DIRECTORY + "/" + RUNS));
		Files.createDirectories(Paths.get(OUTPUT_DIRECTORY + "/" + CONTACTS));

		utils = TextItUtils.createUtils(TOKEN, TIMEZONE);
	}

	@SuppressWarnings("unchecked")
	protected List<String> getFlowIDs() throws IOException {
		final List<String> res = new ArrayList<String>();

        int no_of_days_before = 7;
		final String timestamp_prev = df.format(new DateTime(START_DATE)
				.minusDays(no_of_days_before).toDate());
		final String timestamp_now = df.format(new DateTime(END_DATE)
				.toDate());

		logger.info("No of Days : " + timestamp_prev + " to " + timestamp_now);
        URL target = null;
        target = new URL(GET_FLOWS_URL.toString() + "?after=" + timestamp_prev
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
                }
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.writeValue(
                        Paths.get(
                                OUTPUT_DIRECTORY + "/" + FLOWS,
                                String.format("%s-%d-" + FLOWS + ".json",
                                        timestamp_final, pageNum)).toFile(),
                        data);

            }
        });

        return res;
	}

	@SuppressWarnings("unchecked")
	protected List<String> getContactInfo() throws IOException {
		final List<String> res = new ArrayList<String>();

		final String timestamp_prev = df.format(new DateTime(START_DATE).toDate());
		final String timestamp_now = df.format(new DateTime(END_DATE).toDate());

		URL target = new URL(GET_CONTACTS_URL.toString() + "?after="
				+ timestamp_prev + "T" + TEXT_TIME + "&&" + "before=" + timestamp_now
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
				}
				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.writeValue(
						Paths.get(
								OUTPUT_DIRECTORY + "/" + CONTACTS,
								String.format("%s-%d-" + CONTACTS + ".json",
										timestamp_final, pageNum)).toFile(),
						data);

                if (pageNum == 1) {
                    ObjectMapper objectMapper2 = new ObjectMapper();
                    File statFile = Paths.get(OUTPUT_DIRECTORY + "/" + CONTACTS,
                            String.format("%s-" + STATS + ".json", timestamp_final)).toFile();
                    Map<String, Object> exsistingVals = new HashMap<String, Object>();
                    if(statFile.exists()) {
                        exsistingVals = objectMapper2.readValue(statFile, Map.class);
                    }
                    exsistingVals.put(MongoDB.UPDATED, data.get("count"));
                    exsistingVals.put(MongoDB.FROM_DATE, timestamp_prev);
                    exsistingVals.put(MongoDB.TO_DATE, timestamp_now);
                    exsistingVals.put(MongoDB.DATE, df.format(new Date()));
                    exsistingVals.put(MongoDB.INTERVAL, INTERVAL);
                    objectMapper2.writeValue(statFile, exsistingVals);
                }
			}
		});


        utils.processDataOnce(new URL(GET_CONTACTS_URL.toString()), new TextItUtils.IJsonProcessor() {
            @Override
            public void process(Map<String, Object> data, int pageNum)
                    throws IOException {
                ObjectMapper objectMapper = new ObjectMapper();
                File statFile = Paths.get(OUTPUT_DIRECTORY + "/" + CONTACTS,
                        String.format("%s-" + STATS + ".json", timestamp_final)).toFile();
                Map<String, Object> exsistingVals = new HashMap<String, Object>();
                if(statFile.exists()) {
                    exsistingVals = objectMapper.readValue(statFile, Map.class);
                }
                exsistingVals.put(MongoDB.TOTAL, data.get("count"));
                objectMapper.writeValue(statFile, exsistingVals);
            }
        });

		return res;
	}

    @SuppressWarnings("unchecked")
    protected List<String> getUpdatedRuns() throws IOException {
        final List<String> res = new ArrayList<String>();

        final String timestamp_prev = df.format(new DateTime(START_DATE).toDate());
        final String timestamp_now = df.format(new DateTime(END_DATE).toDate());

        URL target = new URL(GET_RUNS_URL.toString() + "?after="
                + timestamp_prev + "T" + TEXT_TIME + "&&" + "before=" + timestamp_now
                + "T" + TEXT_TIME);
        final String timestamp_final = timestamp_prev.replace("-", "_") + "-"
                + timestamp_now.replace("-", "_");
        logger.info("Downloading updated runs for : " + timestamp_final);

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
                }
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.writeValue(
                        Paths.get(
                                OUTPUT_DIRECTORY + "/" + RUNS,
                                String.format("%s-%d-" + RUNS + ".json",
                                        timestamp_final, pageNum)).toFile(),
                        data);

            }
        });

        return res;
    }

	protected void downloadData(String param, List<String> flowIDs)
			throws IOException {
		ThreadedDownloader downloader = new ThreadedDownloader(flowIDs,
				WORKER_NUM, param);
		try {
			downloader.execute();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	/** unit test purpose */
	protected static TextItClient createClient(Properties properties)
			throws IOException {
		String token = properties.getProperty("token");
		String outputDir = properties.getProperty(
				"outputdir", "./output");
		String textitEpr = properties.getProperty("textit.epr",
				"https://textit.in/api/v1");
		String timezone = properties.getProperty("timezone");
		int no_of_days = Integer.valueOf(properties
				.getProperty("download_no_of_days"));

		int workerNum = Integer.valueOf(properties.getProperty(
				"workernum", "1"));

        String start_date = properties.getProperty("start_date");
        String end_date = properties.getProperty("end_date");
        String interval = properties.getProperty("interval");

        String textTime = "00:00:00.000";
        if (properties.getProperty("text.time") != null) {
            textTime = properties.getProperty("text.time");
        }
		TextItClient instance = new TextItClient(token, outputDir, textitEpr,
				workerNum, timezone, no_of_days, start_date, end_date, textTime, interval);
		return instance;
	}

	public boolean downloadRuns(){

        boolean status = false;

        JSONObject statusObject = new JSONObject();
        statusObject.put(MongoDB.DATE, df.format(new Date()));
        statusObject.put(MongoDB.END_DATE, df.format(new DateTime(END_DATE).toDate()));
        if(INTERVAL.equals(MongoDB.DURATION)) {
            statusObject.put(MongoDB.START_DATE, df.format(new DateTime(START_DATE).toDate()));
        }
        statusObject.put(MongoDB.INTERVAL, INTERVAL);
        statusObject.put(MongoDB.ACTION, MongoDB.DOWNLOAD);

        statusObject.put(MongoDB.TYPE, FLOWS);
        List<String> flowIDs = null;
        try {
            flowIDs = getFlowIDs();
        } catch (Exception e) {
            logger.error(e.getMessage());
            statusObject.put(MongoDB.STATUS, MongoDB.FAILURE);
            statusObject.put(MongoDB.MESSAGE, e.getMessage());
            MongoDB.addStatus(statusObject.toString());
            // update contact status
            statusObject.put(MongoDB.TYPE, CONTACTS);
            statusObject.put(MongoDB.MESSAGE, "Failed to download Flows hence halted");
            MongoDB.addStatus(statusObject.toString());
            // update runs status
            statusObject.put(MongoDB.TYPE, RUNS);
            MongoDB.addStatus(statusObject.toString());
            return status;
        }
        statusObject.put(MongoDB.STATUS, MongoDB.SUCCESS);
        MongoDB.addStatus(statusObject.toString());

        statusObject.put(MongoDB.TYPE, CONTACTS);
        try {
            List<String> contactInfo = getContactInfo();
        } catch (Exception e) {
            logger.error(e.getMessage());
            statusObject.put(MongoDB.STATUS, MongoDB.FAILURE);
            statusObject.put(MongoDB.MESSAGE, e.getMessage());
            MongoDB.addStatus(statusObject.toString());
            // update runs status
            statusObject.put(MongoDB.TYPE, RUNS);
            statusObject.put(MongoDB.MESSAGE, "Failed to download Contacts hence halted");
            MongoDB.addStatus(statusObject.toString());
            return status;
        }
        MongoDB.addStatus(statusObject.toString());

        statusObject.put(MongoDB.TYPE, RUNS);
        try {
            getUpdatedRuns();
            downloadData(null, flowIDs);
        } catch (Exception e) {
            logger.error(e.getMessage());
            statusObject.put(MongoDB.STATUS, MongoDB.FAILURE);
            statusObject.put(MongoDB.MESSAGE, e.getMessage());
            MongoDB.addStatus(statusObject.toString());
            return status;
        }
        MongoDB.addStatus(statusObject.toString());

        return true;
    }

	public void close() throws IOException {
		utils.close();
	}
}
