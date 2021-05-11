import java.io.*;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    final private int TIMEOUT;
    final private String FILE_FOLDER;
    final private Socket CSOCKET;
    final private ServerSocket DSOCKET;
    final private ConcurrentHashMap<String, FileRecord> files;
//    final private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    Map<String, List<Integer>> filesToSend;


    public Dstore(int dPort, int cPort, int timeout, String file_folder) throws IOException {
        TIMEOUT = timeout;
        FILE_FOLDER = file_folder;
        CSOCKET = new Socket("localhost", cPort);
        DSOCKET = new ServerSocket(dPort);
        files = new ConcurrentHashMap<>();
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, dPort);
        setup(new File(file_folder));
        controllerCommunication();
        send("JOIN " + dPort, CSOCKET);
        run();
    }

    private void setup(File dir) {
        if (!dir.exists()){
            dir.mkdirs();
        } else {
            String[] entries = dir.list();
            System.out.println("Removing files: " + Arrays.toString(entries));
            for(String s: entries){
                File currentFile = new File(dir.getPath(),s);
                currentFile.delete();
            }
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
                        case "REBALANCE" -> rebalance(line.split("\\s+"));
                        default -> System.out.println("Unrecognised command for DSTORE: [" + line + "] ");
                    }
                }
            } catch (IOException e) {e.printStackTrace();}
        }).start();
    }

    private void rebalance(String[] lineSplit) throws IOException {
        parseRemove( parseAdd(lineSplit) );
        System.out.println("(i) REBALANCE: FILES TO SEND");
        filesToSend.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });
        System.out.println("(i) REBALANCE: FILES TO REMOVE");
        System.out.println(Arrays.toString(filesToRemove.toArray()));

        if (!filesToSend.isEmpty()){
            CountDownLatch latch = new CountDownLatch(filesToSend.values().size());
            rebalanceSend(latch);
            try { latch.await(); }      //TODO: ADD TIMEOUT
            catch (InterruptedException e) { e.printStackTrace(); };
        }
        if (!filesToRemove.isEmpty()) {
            for (String filename : filesToRemove) {
                FileRecord f = files.remove(filename);
                boolean deleted = f.delete();
                System.out.println("DELETING: " + f.getName() + " " + deleted);
            }
        }
        send(Protocol.REBALANCE_COMPLETE_TOKEN, CSOCKET);
    }

    private void rebalanceSend(CountDownLatch latch) {
        filesToSend.forEach( (filename, dstores) -> {
//            System.out.println(filename + " " + Arrays.toString(dstores.toArray()));
            for (int port : dstores) {
                try {
                    Socket socket = new Socket("localhost", port);
                    System.out.println("CREATED A SOCKET PORT: " + port);
                    new Thread( () -> {
                        try { sendRebalanceFile(socket, filename, latch); }
                        catch (IOException e) { e.printStackTrace();}
                    }).start();
                    send(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + getFile(filename).getFilesize(), socket);
//                    new Thread( () -> {
//                        try {
//                            send(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + getFile(filename).getFilesize(), socket);
//                        } catch (IOException e) { e.printStackTrace(); }
//                    }).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void sendRebalanceFile(Socket socket, String filename, CountDownLatch latch) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        socket.setSoTimeout(TIMEOUT);
        String line;
        try {
            line = in.readLine();
            System.out.println(line);
        } catch (Exception e) {
            e.printStackTrace();
            latch.countDown();
            return;
        } if (line == null) {
            System.out.println("LINE IN DSTORE:155 WAS NULL");
            return;
        }
//        socket.shutdownInput();
        if(!line.equals(Protocol.ACK_TOKEN)) {
            latch.countDown();
            System.out.println("WRONG ACK VALUE");
            return;
        } else {
            InputStream inf;
            OutputStream outf = socket.getOutputStream();
            try {
                System.out.println("HERE");
                File file = new File(FILE_FOLDER + "/" + filename);
                System.out.println(file.getName());
                inf = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                latch.countDown();
                return;
            }
            byte[] bytes = inf.readNBytes(getFile(filename).getFilesize());
            outf.write(bytes);
            inf.close();
            outf.close();
            System.out.println("(i) SUCCESSFULY SENT FILE: " + filename + " TO PORT: " + socket.getPort() );
        }
        latch.countDown();
    }

    private FileRecord getFile(String filename) {
//        readWriteLock.readLock().lock();
//        for (FileRecord f : files) {
//            if (f.getName().equals(filename)){
//                System.out.println("FILE FOUND");
//                return f;
//            }
//        }
        FileRecord f = files.get(filename);
        if (f != null) {
            return f;
        }
//        readWriteLock.readLock().unlock();
        return null;
    }

    List<String> filesToRemove;
    private void parseRemove(String[] msg) {
        filesToRemove = new ArrayList<>();
        int numberToRemove;
        try { numberToRemove = Integer.parseInt(msg[0]); }
        catch (NumberFormatException e) { return; }
        for (int i = 1; i <= numberToRemove; i++) {
            filesToRemove.add(msg[i]);
        }
//        System.out.println(Arrays.toString(filesToRemove.toArray()));
    }

    private String[] parseAdd(String[] lineSplit) {
        String[] msg = Arrays.copyOfRange(lineSplit,1,lineSplit.length);
        filesToSend = new HashMap<>();
        try {
            int numberTosend = Integer.parseInt(msg[0]);
            int j = 1;
            for (int i = 0; i < numberTosend; i++) {
                String filename = msg[j];
//                System.out.println("FILE " + filename);
                j++;
                int dstoresAmount = Integer.parseInt(msg[j]);
                j++;
//                System.out.println("AMOUNT " + dstoresAmount);
                for (int x = 0; x < dstoresAmount; x++) {
                    int dstorePort = Integer.parseInt(msg[j]);
                    j++;
//                    System.out.println("PORT " + dstorePort);
                    List<Integer> list = filesToSend.getOrDefault(filename, new ArrayList<>());
                    list.add(dstorePort);
                    filesToSend.put(filename, list);
                }
            }
//            System.out.println("***PARSED RESULT***");
//            sendMap.forEach((key, value) -> System.out.println(key + " " + value));
            return Arrays.copyOfRange(lineSplit,j+1,lineSplit.length);
        } catch (NumberFormatException e) {
            filesToSend.clear();
            return Arrays.copyOfRange(lineSplit,2,lineSplit.length);
        }
    }

    private void list() throws IOException {
        StringBuilder fileList = new StringBuilder(Protocol.LIST_TOKEN);
//        readWriteLock.readLock().lock();
        synchronized (files) {
            if (!files.isEmpty()) {
                files.forEach((filename, fileRecord) -> fileList.append(" ").append(filename));
            }
        }
//        readWriteLock.readLock().unlock();
        send(fileList.toString(), CSOCKET);
    }


    public void send(String message, Socket socket) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream());
        out.println(message);
        out.flush();
        DstoreLogger.getInstance().messageSent(socket, message);
    }

// Client DSTORE messages

    private void run() throws IOException {
        while (true){
            Socket client = DSOCKET.accept();
//            System.out.println("Dstore connection established by: " + client.getRemoteSocketAddress());
            new Thread( () -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while ((line = in.readLine()) != null){
                        DstoreLogger.getInstance().messageReceived(client, line);
                        String[] lineSplit = line.split(" ");
                        switch (lineSplit[0]) {
                            case "STORE" -> store(lineSplit, client, false);
                            case "LOAD_DATA" -> loadData(lineSplit, client);
                            case "REBALANCE_STORE" -> store(lineSplit, client, true);
                            default -> System.out.println("Unrecognised command for DSTORE: " + line);
                        }
                    }
                } catch (IOException e) {
                    System.out.println( "(X) DSTORE EXCEPTION -> " + e.getMessage().toUpperCase());
                }
            }).start();
        }
    }

    // Operations

    private void store(String[] lineSplit, Socket client, boolean rebalance) throws IOException {
        if (!isStoreMessageCorrect(lineSplit)) return;
        FileRecord f = files.get(lineSplit[1]);
        if (f == null) {
            send(Protocol.ACK_TOKEN, client);
            if(!storeAction(client, lineSplit[1], Integer.parseInt(lineSplit[2])))
                return;
        } else {
            System.out.println("(X) File already exists, closing socket...");
            client.close();
        }
        if (!rebalance)
            send(Protocol.STORE_ACK_TOKEN + " " + lineSplit[1], CSOCKET);
        else {
            client.close();
        }
    }

    private boolean isStoreMessageCorrect(String[] lineSplit){
        if (lineSplit.length != 3 || !Common.isNumeric(lineSplit[2])) {
            System.out.println("Malformed STORE/REBALANCE_STORE message: " + String.join(" ", lineSplit));
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
//            if(bytes.length != filesize) {
//                System.out.println("WRONG BYTE VALUES " + bytes.length);
//                return false;
//            }
            outf.write(bytes);
//            readWriteLock.writeLock().lock();
            files.put(filename,file);
            System.out.println("(i)" + file.getName() + " WAS ADDED" );
//            readWriteLock.writeLock().unlock();
            return true;
        } catch (ExecutionException | InterruptedException | IOException e){
            e.printStackTrace();
//            System.out.println("Unexpected stuff happened");
            return false;
        } catch (TimeoutException e) {
            System.out.println("(X) TIMEOUT EXPIRED WHILE TRYING TO STORE");
            return false;
        }
    }

    private void loadData(String[] loadSplit, Socket client) throws IOException {
        if (!isLoadMessageCorrect(loadSplit, client))
            return;
        String filename = loadSplit[1];
        boolean exists = false;
        int filesize = 0;
//        readWriteLock.readLock().lock();
//        for (FileRecord f : files){
//            if (f.getName().equals(filename)){
//                exists = true;
//                filesize = f.getFilesize();
//                break;
//            }
//        }
        FileRecord f = files.get(filename);
//        readWriteLock.readLock().unlock();
        if(f == null){
            System.out.println("(X) LOAD ERROR - FILE: [" + filename + "] DOES NOT EXIST");
            client.close();
        } else {
            File file = new File(FILE_FOLDER + "/" + filename);
            System.out.println(file.exists());
            System.out.println(file.length());
            InputStream inf = new FileInputStream(file);
            OutputStream outf = client.getOutputStream();
            byte[] bytes = inf.readNBytes(f.getFilesize());
//            System.out.println(Arrays.toString(bytes));
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
//        System.out.println("Before removing the file: " + files.size());
//        readWriteLock.writeLock().lock();
        FileRecord removed = files.remove(filename);
//        readWriteLock.writeLock().unlock();
        if (removed == null) {
            send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename, CSOCKET);
            return;
        }
        File file = new File(FILE_FOLDER + "/" + filename);
        if (file.delete())
            System.out.println("(i) REMOVE CONFIRMATION: File removed successfully");
        else
            System.out.println("(X) REMOVE ERROR: Failed to remove a file");
//        System.out.println("After removing the file: " + files.size());
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
