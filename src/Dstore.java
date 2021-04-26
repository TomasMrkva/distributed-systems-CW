import java.io.*;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Dstore {

    class FileRecord extends File {

        private final int filesize;

        public FileRecord(String pathname, int filesize) {
            super(pathname);
            this.filesize = filesize;
        }

        public FileRecord(String parent, String child, int filesize) {
            super(parent, child);
            this.filesize = filesize;
        }

        public int getFilesize() {
            return filesize;
        }

    }

    final int PORT;
    final int CPORT;
    final int TIMEOUT;
    final String FILE_FOLDER;
    final Socket CSOCKET;
    final ServerSocket DSOCKET;
    final PrintWriter controllerMessages;
    final String FOLDER_NAME;
    final Set<FileRecord> files;
    final File dir;

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

    private void run() throws IOException {
        while (true){
            Socket client = DSOCKET.accept();
            System.out.println("Dstore connection established by: " + client.getRemoteSocketAddress());
            new Thread( () -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream());
                    String line;
                    while ((line = in.readLine()) != null){
                        String[] lineSplit = line.split(" ");
                        switch (lineSplit[0]) {
                            case "STORE" -> store(lineSplit, client, out);
                            case "LOAD_DATA" -> loadData(lineSplit[1], client);
                            case "REMOVE" -> removeFile(lineSplit[1],out);
                            default -> System.out.println("Unrecognised command " + line);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    private void removeFile(String filename, PrintWriter out){
        System.out.println("Before removing the file");
        files.removeIf( fileRecord -> fileRecord.getName().equals(filename));
        System.out.println("After removing the file");
    }

    private void loadData(String filename, Socket client) throws IOException {
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
            File file = new FileRecord(FILE_FOLDER + "/" , filename , filesize);
            InputStream inf = new FileInputStream(file);
            OutputStream outf = client.getOutputStream();
            byte[] bytes = inf.readNBytes(filesize);
            outf.write(bytes);
            inf.close();
//            outf.close();
        }
    }

    private void store(String[] lineSplit, Socket client, PrintWriter out) throws IOException {
        boolean exists = false;
        for (FileRecord f : files){
            if (f.getName().equals(lineSplit[1])){
                exists = true;
                break;
            }
        }
        if(!exists){
            out.println("ACK"); out.flush();
            if(!storeAction(client, lineSplit[1], Integer.parseInt(lineSplit[2])))
                return;
        } else {
            System.out.println("File already exists, closing socket");
            client.close();
        }
        files.forEach( file -> System.out.println(file.getName() + " : " + file.getFilesize()));
        controllerMessages.println("STORE_ACK " + lineSplit[1]); controllerMessages.flush();
    }

    private boolean storeAction(Socket client, String filename, int filesize) {
        Callable<byte[]> task = () -> {
            InputStream in = client.getInputStream();
            byte[] bytes = in.readNBytes(filesize);
            return bytes;
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        FileRecord file = new FileRecord(FILE_FOLDER + "/" + filename, filename, filesize);
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
