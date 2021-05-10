import java.io.File;
import java.io.IOException;

public class ClientMain {

	public static void main(String[] args) {

//		int cport = -1;
//		int timeout = -1;
//		try {
//			// parse arguments
//			cport = Integer.parseInt(args[0]);
//			timeout = Integer.parseInt(args[1]);
//		} catch (NumberFormatException e) {
//			System.err.println("Error parsing arguments: " + e.getMessage());
//			System.err.println("Expected: java ClientMain cport timeout");
//			System.exit(-1);
//		}

		File downloadFolder = new File("downloads");
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");

//		testClient(cport, timeout, downloadFolder);

		// example to launch a number of concurrent clients, each doing the same operations
		for (int i = 0; i < 1000; i++) {
			new Thread() {
				public void run() {
					testClient(4444, 1000, downloadFolder);
				}
			}.start();
		}
	}

	public static void testClient(int cport, int timeout, File downloadFolder) {
		Client client = null;

		try {

			client = new Client(cport, timeout, Logger.LoggingType.ON_TERMINAL_ONLY);

			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

			try { list(client); } catch(IOException e) { e.printStackTrace(); }

			try { client.store(new File("test.txt")); } catch(IOException e) { e.printStackTrace(); }

			try { client.store(new File("doggo.jpg")); } catch(IOException e) { e.printStackTrace(); }

			try { client.store(new File("catto.jpg")); } catch(IOException e) { e.printStackTrace(); }

			try { client.store(new File("gudfile.txt")); } catch(IOException e) { e.printStackTrace(); }

			try { client.store(new File("a.txt")); } catch(IOException e) { e.printStackTrace(); }

			try { client.store(new File("hemlo.txt")); } catch(IOException e) { e.printStackTrace(); }


			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }

			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }

			try { list(client); } catch(IOException e) { e.printStackTrace(); }

		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

	public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
		System.out.println("Retrieving list of files...");
		String list[] = client.list();

		System.out.println("Ok, " + list.length + " files:");
		int i = 0;
		for (String filename : list)
			System.out.println("[" + i++ + "] " + filename);

		return list;
	}

}