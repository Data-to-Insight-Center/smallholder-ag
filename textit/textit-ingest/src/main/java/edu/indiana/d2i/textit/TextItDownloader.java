package edu.indiana.d2i.textit;

public class TextItDownloader {
	static TextItWebHook hook = null;
	
	public static void main(String[] args) throws Exception {
		if (args.length != 0 && args.length != 1) {
			System.out.println("Usage: [port]");
			System.exit(-1);
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
	   @Override
	   public void run() {
           System.out.println("Sleeping....");
           try {
               Thread.sleep(3000);
           } catch (InterruptedException e) {
               e.printStackTrace();
           }
           System.out.println("Clean up resources.");
	  	 if (hook != null)
				try {
					hook.stop();
				} catch (Exception e) {
					System.err.println(e.getStackTrace());
				}
	   }
	  });
		
		if (args.length == 0) {
			System.out.println("Just download the runs.");
			TextItClient client = TextItClient.createClient();
			client.downloadRuns();
			client.close();
		} else {
			// TODO: check if there is any run before, try to resume first
			System.out.println("Download the runs first and then runs as a callback service.");
			
			// start downloading from scratch
			TextItClient client = TextItClient.createClient();
			client.downloadRuns();
			client.close();
			
			// run the web hook
			int port = Integer.valueOf(args[0]);
			hook = TextItWebHook.getSingleton(port);
			hook.start();
		}
	}
}
