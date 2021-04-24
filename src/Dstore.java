import java.io.*;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class Dstore {

    final int PORT;
    final int CPORT;
    final int TIMEOUT;
    final String FILE_FOLDER;
    final Socket CSOCKET;
    final ServerSocket DSOCKET;
    final PrintWriter controllerMessages;
    final String FOLDER_NAME;
    final CopyOnWriteArrayList<File> fileList;

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
        fileList = new CopyOnWriteArrayList<>();
        setup();
        run();
    }

    private void setup() throws IOException {
        File dir = new File(FOLDER_NAME);
        if (!dir.exists()){
            System.out.println("made a dir");
            dir.mkdirs();
        } else {
            fileList.removeAll(Arrays.asList(dir.listFiles()));
        }
    }

    private void run() throws IOException {
        while (true){
            Socket client = DSOCKET.accept();
            System.out.println("Dstore connection established by: " + client.getRemoteSocketAddress());
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
        String filename = FILE_FOLDER + "/" + lineSplit[1];
        int filesize = Integer.parseInt(lineSplit[2]);

        PrintWriter out = new PrintWriter(client.getOutputStream());
        InputStream fileIn = client.getInputStream();
        File file = new File(filename);

        boolean exists = false;
        for (File f : fileList){
            if (f.getName().equals(lineSplit[1])){
                exists = true;
                break;
            }
        }
        if(!exists){
            out.println("ACK"); out.flush();
            OutputStream fileOut = new FileOutputStream(file);
            byte[] bytes = fileIn.readNBytes(filesize);
            fileOut.write(bytes);
            fileOut.close();
            fileList.add(file);
        } else {
            System.out.println("File already exists");
        }
        System.out.println(fileList.size());
        controllerMessages.println("STORE_ACK " + lineSplit[1]);
        controllerMessages.flush();
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
