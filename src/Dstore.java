import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {

    final int PORT;
    final int CPORT;
    final int TIMEOUT;
    final String FILE_FOLDER;
    final Socket CSOCKET;
    final ServerSocket DSOCKET;
    PrintWriter controllerMessages;
    final String FOLDER_NAME;

    public Dstore(int port, int cport, int timeout, String file_folder) throws Exception {
        PORT = port;
        CPORT = cport;
        TIMEOUT = timeout;
        FILE_FOLDER = file_folder;
        CSOCKET = new Socket("localhost" ,cport);
        DSOCKET = new ServerSocket(port);
        FOLDER_NAME = file_folder;
        controllerMessages = new PrintWriter(CSOCKET.getOutputStream());
        controllerMessages.println("JOIN " + port); controllerMessages.flush();
        File theDir = new File(file_folder);
        if (!theDir.exists()){
            theDir.mkdirs();
        }
        run();
    }

    private void run() throws IOException {
        while (true){
            System.out.println("Waiting for connections...");
            Socket client = DSOCKET.accept();
            System.out.println("Connection established");
            new Thread( () -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while ((line = in.readLine()) != null){
                        String[] lineSplit = line.split(" ");
                        switch (lineSplit[0]){
                            case "STORE":
                                store(lineSplit, client);
                                break;
                            default:
                                System.out.println("Unrecognised command");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }


    private void store(String[] lineSplit, Socket client) throws IOException {
        String line;
        String filename = FILE_FOLDER + "/" + lineSplit[1];
        int filesize = Integer.parseInt(lineSplit[2]);

        PrintWriter out = new PrintWriter(client.getOutputStream());
        InputStream fileIn = client.getInputStream();
        OutputStream fileOut = new FileOutputStream(new File(filename));

        out.println("ACK");
        byte[] bytes = new byte[8*1024]; // could be filesize
        int count;
        while ((count = fileIn.read(bytes)) > 0) {
            fileOut.write(bytes, 0, count);
        }
        fileOut.close();
        controllerMessages.println("STORE_ACK "+ lineSplit[1]);
        controllerMessages.flush();
    }

    public void storeFile(String fileContent){
        System.out.println(fileContent);
    }

    //java Controller cport R timeout rebalance_period
    public static void main(String[] args) {
        int port, cport, timeout;
        String file_folder = args[3];
        try { port = Integer.parseInt(args[0]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of cport!"); return;}
        try { cport = Integer.parseInt(args[1]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of R!"); return;}
        try { timeout = Integer.parseInt(args[2]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of timeout!"); return;}

        try {
            new Dstore(port, cport, timeout, file_folder);

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
