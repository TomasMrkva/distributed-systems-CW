import java.io.*;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Dstore {

    class FileRecord extends File {

        private final int filesize;

        public FileRecord(String filename, int filesize) {
            super(FILE_FOLDER + "/" + filename);
            this.filesize = filesize;
        }

        public int getFilesize() {
            return filesize;
        }
    }

    final public int TIMEOUT;
    final public String FILE_FOLDER;
    final private Socket CSOCKET;
    final private ServerSocket DSOCKET;
    final public Map<String, FileRecord> files;
    final private PrintWriter out;

    public Dstore(int dPort, int cPort, int timeout, String file_folder) throws IOException {
        TIMEOUT = timeout;
        FILE_FOLDER = file_folder;
        CSOCKET = new Socket("localhost", cPort);
        out = new PrintWriter(CSOCKET.getOutputStream());
        DSOCKET = new ServerSocket(dPort);
        files = Collections.synchronizedMap(new HashMap<>());
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, dPort);
        setup(new File(file_folder));
        controllerCommunication();
        sendToController("JOIN " + dPort);
        run();
    }

    private void setup(File dir) {
        if (!dir.exists()){
            dir.mkdirs();
        } else {
            String[] entries = dir.list();
//            System.out.println("Removing files: " + Arrays.toString(entries));
            for(String s: entries){
                File currentFile = new File(dir.getPath(),s);
                currentFile.delete();
            }
        }
    }

    public void send(String message, Socket socket, PrintWriter out) throws IOException {
        out.println(message);
        out.flush();
        DstoreLogger.getInstance().messageSent(socket, message);
    }

    public void sendToController(String message) {
        out.println(message);
        out.flush();
        DstoreLogger.getInstance().messageSent(CSOCKET, message);
    }

// Controller DSTORE messages

    private void controllerCommunication() {
        new Thread( () -> {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(CSOCKET.getInputStream()));
                String line;
                while ((line = in.readLine()) != null){
                    DstoreLogger.getInstance().messageReceived(CSOCKET, line);
                    String[] lineSplit = line.split(" ");
                    switch (lineSplit[0]) {
                        case "REMOVE" -> {
                            if (lineSplit.length == 2) {
                                removeFile(lineSplit);
                            } else {
                                System.out.println("Malformed REMOVE message: " + line);
                            }
                        }
                        case "LIST" -> {
                            if (lineSplit.length == 1) {
                                list();
                            } else {
                                System.out.println("Malformed LIST message: " + line);
                            }
                        }
                        case "REBALANCE" -> new DstoreRebalance(this).rebalance(line.split(" "));
                        default -> System.out.println("Malformed message: " + line);
                    }
                }
            } catch (IOException e) { e.printStackTrace();}
        }).start();
    }

    private void list() throws IOException {
        StringBuilder fileList = new StringBuilder(Protocol.LIST_TOKEN);
        synchronized (files) {
            if (!files.isEmpty()) {
                files.forEach((filename, fileRecord) -> fileList.append(" ").append(filename));
            }
        }
        sendToController(fileList.toString());
    }

// Client DSTORE messages

    private void run() throws IOException {
        while (true){
            Socket client = DSOCKET.accept();
//            System.out.println("Dstore connection established by: " + client.getRemoteSocketAddress());
            new Thread( () -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream());
                    String line;
                    while ((line = in.readLine()) != null){
                        DstoreLogger.getInstance().messageReceived(client, line);
                        String[] lineSplit = line.split(" ");
                        switch (lineSplit[0]) {
                            case "STORE" -> store(lineSplit, client, false, out);
                            case "LOAD_DATA" -> loadData(lineSplit, client);
                            case "REBALANCE_STORE" -> store(lineSplit, client, true, out);
                            default -> System.out.println("Malformed message: " + line);
                        }
                    }
                } catch (IOException e) {
                    System.out.println( "(X) DSTORE EXCEPTION -> " + e.getMessage().toUpperCase());
                }
            }).start();
        }
    }

    // Operations

    private boolean isStoreMessageCorrect(String[] lineSplit){
        if (lineSplit.length != 3 || !Common.isNumeric(lineSplit[2])) {
            System.out.println("Malformed STORE/REBALANCE_STORE message: " + String.join(" ", lineSplit));
            return false;
        }
        return true;
    }

    private void store(String[] lineSplit, Socket client, boolean rebalance, PrintWriter out) throws IOException {
        if (!isStoreMessageCorrect(lineSplit))
            return;
        FileRecord f = files.get(lineSplit[1]);
        if (f == null) {        //file does not exist yet, should always be true
            send(Protocol.ACK_TOKEN, client, out);
            if (!storeAction(client, lineSplit[1], Integer.parseInt(lineSplit[2]))) {
                return;
            }
        } else if (rebalance) {
            System.out.println("(X) DSTORE ERROR: File already exists");
        }
        if (!rebalance)
            sendToController(Protocol.STORE_ACK_TOKEN + " " + lineSplit[1]);
        else {
            client.close();
        }
    }

    private boolean storeAction(Socket client, String filename, int filesize) {
        Callable<byte[]> task = () -> {
            InputStream in = client.getInputStream();
            return in.readNBytes(filesize);
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        FileRecord file = new FileRecord(filename, filesize);
        try (OutputStream outf = new FileOutputStream(file)) {
            Future<byte[]> future = executor.submit(task);
            byte[] bytes = future.get(TIMEOUT, TimeUnit.MILLISECONDS);
            if (bytes.length == 0 && filesize != 0) {
//                System.out.println("(X) DSTORE ERROR, COULD NOT READ FILE DATA FROM STREAM " + filename);
                file.delete();
                return false;
            }
            outf.write(bytes);
            files.put(filename,file);
//            System.out.println("(i)" + file.getName() + " WAS ADDED" );
            return true;
        } catch (ExecutionException | InterruptedException | IOException e){
            e.printStackTrace();
            return false;
        } catch (TimeoutException e) {
            System.out.println("(X) TIMEOUT EXPIRED WHILE TRYING TO STORE");
            file.delete();
            return false;
        }
    }

    private boolean isLoadMessageCorrect(String[] lineSplit){
        if (lineSplit.length != 2) {
            System.out.println("Malformed LOAD_DATA message: " + String.join(" ", lineSplit));
            return false;
        }
        return true;
    }

    private void loadData(String[] loadSplit, Socket client) throws IOException {
        if (!isLoadMessageCorrect(loadSplit))
            return;
        String filename = loadSplit[1];
        FileRecord f = files.get(filename);
        if(f == null) {
            System.out.println("(X) LOAD ERROR - FILE: [" + filename + "] DOES NOT EXIST");
            client.close();
        } else {
            File file = new File(FILE_FOLDER + "/" + filename);
            InputStream inf = new FileInputStream(file);
            OutputStream outf = client.getOutputStream();
            byte[] bytes = inf.readNBytes(f.getFilesize());
            outf.write(bytes);
            inf.close();
            outf.close();
        }
    }

    private boolean isRemoveMessageCorrect(String[] lineSplit){
        if (lineSplit.length != 2) {
            System.out.println("Malformed REMOVE message: " + String.join(" ", lineSplit));
            return false;
        }
        return true;
    }

    private void removeFile(String[] lineSplit) throws IOException {
        if (!isRemoveMessageCorrect(lineSplit))
            return;
        String filename = lineSplit[1];
        FileRecord removed = files.remove(filename);
        if (removed == null) {
            sendToController(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
            return;
        }
        File file = new File(FILE_FOLDER + "/" + filename);
        file.delete();
//        if (file.delete())
//            System.out.println("(i) REMOVE CONFIRMATION: File removed successfully");
//        else
//            System.out.println("(X) REMOVE ERROR: Failed to remove a file");
        sendToController(Protocol.REMOVE_ACK_TOKEN + " " + filename);
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
        }
    }
}
