import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ClientMain {

	public static void main(String[] args) throws Exception{

		final int cport = 4444;
		int timeout = 500;

		File downloadFolder = new File("downloads");
		if (!downloadFolder.exists())
			if (!downloadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");

		File uploadFolder = new File("to_store");
		if (!uploadFolder.exists())
			throw new RuntimeException("to_store folder does not exist");

		// testClient(cport, timeout, downloadFolder);

		// example to launch a number of concurrent clients, each doing the same operations
		for (int i = 0; i < 30; i++) {
			new Thread() {
				public void run() {
//					test2Client(cport, timeout, downloadFolder, uploadFolder);
					testClient(cport, timeout, downloadFolder);
				}
			}.start();
		}
	}

	public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
		Client client = null;

		try {
			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
			client.connect();
			Random random = new Random(System.currentTimeMillis() * System.nanoTime());

			File fileList[] = uploadFolder.listFiles();
			for (int i=0; i<fileList.length/2; i++) {
				File fileToStore = fileList[random.nextInt(fileList.length)];
				try {
					client.store(fileToStore);
				} catch (Exception e) {
					System.out.println("Error storing file " + fileToStore);
					e.printStackTrace();
				}
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

			for (int i = 0; i < list.length/4; i++) {
				String fileToRemove = list[random.nextInt(list.length)];
				try {
					client.remove(fileToRemove);
				} catch (Exception e) {
					System.out.println("Error remove file " + fileToRemove);
					e.printStackTrace();
				}
			}

			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if (client != null)
				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
		}
	}

//	public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
//		Client client = null;
//
//		try {
//			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
//			client.connect();
//
//			File fileList[] = uploadFolder.listFiles();
//			for (int i=0; i<fileList.length; i++) {
//				File fileToStore = fileList[i];
//				try {
//					client.store(fileToStore);
//				} catch (Exception e) {
//					System.out.println("Error storing file " + fileToStore);
//					e.printStackTrace();
//				}
//			}
//
//			// String list[] = null;
//			// try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
//			//
//			// for (int i = 0; i < list.length/4; i++) {
//			//     String fileToRemove = list[random.nextInt(list.length)];
//			//     try {
//			//         client.remove(fileToRemove);
//			//     } catch (Exception e) {
//			//         System.out.println("Error remove file " + fileToRemove);
//			//         e.printStackTrace();
//			//     }
//			// }
//			//
//			// try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
//
//		} catch(IOException e) {
//			e.printStackTrace();
//		} finally {
//			if (client != null)
//				try { client.disconnect(); } catch(Exception e) { e.printStackTrace(); }
//		}
//	}

	public static void testClient(int cport, int timeout, File downloadFolder) {
		Client client = null;

		try {

			client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

			try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }

			try { list(client); } catch(IOException e) { e.printStackTrace(); }

			try { client.store(new File("1.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("2.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("3.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("4.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("5.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("6.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("7.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("7.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("8.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("9.cql")); } catch(IOException e) { e.printStackTrace(); }
			try { client.store(new File("10.cql")); } catch(IOException e) { e.printStackTrace(); }

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }

//			if (list != null)
//				for (String filename : list)
//					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
//			try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }

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