import java.io.*;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Dstore {

    final int PORT;
    final int CPORT;
    final int TIMEOUT;
    final String FILE_FOLDER;
    final Socket CSOCKET;
    final ServerSocket DSOCKET;
    final PrintWriter controllerMessages;
    final String FOLDER_NAME;
    final ConcurrentHashMap<File,Integer> files;

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
        files = new ConcurrentHashMap<>();
        setup();
        run();
    }

    private void setup() {
        File dir = new File(FOLDER_NAME);
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
                            case "LOAD_DATA" -> loadData(lineSplit[1], client, out);
                            default -> System.out.println("Unrecognised command " + line);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    private void loadData(String filename, Socket client, PrintWriter out) throws IOException {
        boolean exists = false;
        int filesize = 0;
        for (File f : Collections.list(files.keys())){
            if (f.getName().equals(filename)){
                exists = true;
                filesize = files.get(f);
                break;
            }
        }
        if(!exists){
            out.println("ERROR_FILE_DOES_NOT_EXIST"); out.flush();
        } else {
            File file = new File(FILE_FOLDER + "/" + filename);
            InputStream inf = new FileInputStream(file);
            OutputStream outf = client.getOutputStream();
            byte[] bytes = inf.readNBytes(filesize);
            outf.write(bytes);
            inf.close();
            outf.close();
        }
    }

    private void store(String[] lineSplit, Socket client, PrintWriter out) throws IOException {
        boolean exists = false;
        for (File f : Collections.list(files.keys())){
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
            System.out.println("File already exists");
        }
        files.forEach((k,v) -> System.out.println(k.getName() + ", " + k));
        controllerMessages.println("STORE_ACK " + lineSplit[1]); controllerMessages.flush();
    }

    private boolean storeAction(Socket client, String filename, int filesize) {
        Callable<byte[]> task = () -> {
            InputStream in = client.getInputStream();
            return in.readNBytes(filesize);
        };
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try{
            byte[] bytes = executor.submit(task).get(TIMEOUT, TimeUnit.MILLISECONDS);
            if(bytes.length != filesize)
                return false;
            File file = new File(FILE_FOLDER + "/" + filename);
            OutputStream outf = new FileOutputStream(file);
            outf.write(bytes);
            outf.close();
            files.put(file,filesize);
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
