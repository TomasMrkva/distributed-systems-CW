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

    final int TIMEOUT;
    final String FILE_FOLDER;
    final Socket CSOCKET;
    final ServerSocket DSOCKET;
    final String FOLDER_NAME;
    final Set<FileRecord> files;
    final File dir;


    public Dstore(int port, int cport, int timeout, String file_folder) throws Exception {
        TIMEOUT = timeout;
        FILE_FOLDER = file_folder;
        CSOCKET = new Socket("localhost" ,cport);
        DSOCKET = new ServerSocket(port);
        FOLDER_NAME = file_folder;
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);
        controllerCommunication();
        send("JOIN " + port, CSOCKET);
        files = ConcurrentHashMap.newKeySet();
        dir = new File(FOLDER_NAME);
        setup();
        run();
    }

    private void setup() {
        if (!dir.exists()){
            dir.mkdirs();
        } else {
            String[] entries = dir.list();
            System.out.println("Removing files: " + Arrays.toString(entries));
            for(String s: entries){
                File currentFile = new File(dir.getPath(),s);
                currentFile.delete();
            }
            files.clear();
        }
    }

    private void controllerCommunication() {
        new Thread( () -> {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(CSOCKET.getInputStream()));
                String line;
                while ((line = in.readLine()) != null){
                    DstoreLogger.getInstance().messageReceived(CSOCKET, line);
                    String[] lineSplit = line.split(" ");
                    switch (lineSplit[0]) {
                        case "REMOVE" -> removeFile(lineSplit);
                        case "LIST" -> list();
                        default -> System.out.println("Unrecognised command " + line);
                    }
                }
            } catch (IOException e) {e.printStackTrace();}
        }).start();
    }

    private void list() throws IOException {
        StringBuilder fileList = new StringBuilder(Protocol.LIST_TOKEN);
        files.forEach(fileRecord -> fileList.append(" ").append(fileRecord.getName()));
        send(fileList.toString(), CSOCKET);
    }

    public void send(String message, Socket socket) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream());
        out.println(message);
        out.flush();
        DstoreLogger.getInstance().messageSent(socket, message);
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
                        DstoreLogger.getInstance().messageReceived(client, line);

                        String[] lineSplit = line.split(" ");
                        switch (lineSplit[0]) {
                            case "STORE" -> store(lineSplit, client);
                            case "LOAD_DATA" -> loadData(lineSplit, client);
                            default -> System.out.println("Unrecognised command " + line);
                        }
                    }
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            }).start();
        }
    }

    // Operations

    private void store(String[] lineSplit, Socket client) throws IOException {
        if (!isStoreMessageCorrect(lineSplit, client)) return;
        boolean exists = false;
        for (FileRecord f : files) {
            if (f.getName().equals(lineSplit[1])){
                exists = true;
                break;
            }
        }
        if (!exists) {
            send(Protocol.ACK_TOKEN, client);
            if(!storeAction(client, lineSplit[1], Integer.parseInt(lineSplit[2])))
                return;
        } else {
            System.out.println("File already exists, closing socket");
            client.close();
        }
        files.forEach( file -> System.out.println(file.getName() + " : " + file.getFilesize()));
        send(Protocol.STORE_ACK_TOKEN + " " + lineSplit[1], CSOCKET);
    }

    private boolean isStoreMessageCorrect(String[] lineSplit, Socket client){
        if (lineSplit.length != 3 || !Common.isNumeric(lineSplit[2])) {
            System.out.println("Malformed STORE message: " + String.join(" ", lineSplit));
            return false;
        }
        return true;
    }

    private boolean storeAction(Socket client, String filename, int filesize) {
        Callable<byte[]> task = () -> {
            InputStream in = client.getInputStream();
            return in.readNBytes(filesize);
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        FileRecord file = new FileRecord(filename, filesize);
        try (OutputStream outf = new FileOutputStream(file)){
            Future<byte[]> future = executor.submit(task);
            byte[] bytes = future.get(TIMEOUT, TimeUnit.MILLISECONDS);
            if(bytes.length != filesize)
                return false;
            outf.write(bytes);
            files.add(file);
            return true;
        } catch (ExecutionException | InterruptedException | IOException e){
            e.printStackTrace();
            System.out.println("Unexpected stuff happened");
            return false;
        } catch (TimeoutException e) {
            System.out.println("Timeout expired");
            return false;
        }
    }

    private void loadData(String[] loadSplit, Socket client) throws IOException {
        if (!isLoadMessageCorrect(loadSplit, client))
            return;
        String filename = loadSplit[1];
        boolean exists = false;
        int filesize = 0;
        for (FileRecord f : files){
            if (f.getName().equals(filename)){
                exists = true;
                filesize = f.getFilesize();
                break;
            }
        }
        if(!exists){
            System.out.println("Dstore - File does not exist: " + filename);
            client.close();
        } else {
            File file = new FileRecord(filename, filesize);
            InputStream inf = new FileInputStream(file);
            OutputStream outf = client.getOutputStream();
            byte[] bytes = inf.readNBytes(filesize);
            outf.write(bytes);
            inf.close();
            outf.close();
        }
    }

    private boolean isLoadMessageCorrect(String[] lineSplit, Socket client){
        if (lineSplit.length != 2) {
            System.out.println("Malformed LOAD_DATA message: " + String.join(" ", lineSplit));
            return false;
        }
        return true;
    }

    private void removeFile(String[] lineSplit) throws IOException {
        if (!isRemoveMessageCorrect(lineSplit))
            return;
        String filename = lineSplit[1];
        System.out.println("Before removing the file: " + files.size());
        boolean removed = files.removeIf( fileRecord -> fileRecord.getName().equals(filename));
        if (!removed) {
            send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename, CSOCKET);
            return;
        }
        File file = new File(FILE_FOLDER + "/" + filename);
        if (file.delete())
            System.out.println("File removed successfully");
        else
            System.out.println("Failed to remove a file");
        System.out.println("After removing the file: " + files.size());
        send(Protocol.REMOVE_ACK_TOKEN + " " + filename, CSOCKET);
    }

    private boolean isRemoveMessageCorrect(String[] lineSplit){
        if (lineSplit.length != 2) {
            System.out.println("Malformed REMOVE message: " + String.join(" ", lineSplit));
            return false;
        }
        return true;
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
