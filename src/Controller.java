import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {

    final public int R;
    final public int CPORT;
    final public int TIMEOUT;
    final public int REBALANCE_PERIOD;
    final private ServerSocket ss;
    final public Index index;
    public ConcurrentHashMap<Integer,ControllerDstoreSession> dstoreSessions;
    public ConcurrentHashMap<String, CountDownLatch> waitingStoreAcks;
    public ConcurrentHashMap<String, CountDownLatch> waitingRemoveAcks;

    public Controller(int cport, int r, int timeout, int rebalance_period) throws Exception {
        CPORT = cport;
        R = r;
        TIMEOUT = timeout;
        REBALANCE_PERIOD = rebalance_period;
        ss = new ServerSocket(cport);
        dstoreSessions = new ConcurrentHashMap<>();
        waitingStoreAcks = new ConcurrentHashMap<>();
        waitingRemoveAcks = new ConcurrentHashMap<>();
        index = new Index(this);
        run();
    }

    private void run() throws IOException {
        while (true){
            Socket client = ss.accept();
            System.out.println("Controller connection established by: " + client.getRemoteSocketAddress());
            new Thread(() -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line = in.readLine(); //TODO: line can be null if user doesnt write anything
                    String[] lineSplit = line.split(" ");
                    if (lineSplit[0].equals("JOIN")) {
                        int dstorePort = Integer.parseInt(lineSplit[1]);
                        ControllerDstoreSession cd = new ControllerDstoreSession(dstorePort,client,this, line);
                        dstoreSessions.put(dstorePort,cd);
                        System.out.println("Dstores: " + dstoreSessions.size());
                        new Thread(cd).start();
                    } else {
//                        System.out.println(line);
                        ControllerClientSession cc = new ControllerClientSession(client, this, line);
                        new Thread(cc).start();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    public void dstoreClosedNotify(int dstorePort) {
//        synchronized (Lock.DSTORE){
            dstoreSessions.remove(dstorePort);
            index.removeDstore(dstorePort);
            System.out.println("Dstores: " + dstoreSessions.size());
//        }
    }

    public void addStoreAck(String filename, ControllerDstoreSession dstore) {
        waitingStoreAcks.compute(filename,(key, value) -> {
            index.addDstore(filename, dstore);
           if(value.getCount() == 1) {
               value.countDown();
               return null;
           } else {
               value.countDown();
               return value;
           }
        });
    }

    public void addRemoveAck(String filename, ControllerDstoreSession dstore) {
        waitingRemoveAcks.compute(filename,(key,value) -> {
            index.removeDstore(filename, dstore);
            if(value.getCount() == 1) {
                value.countDown();
                return null;
            } else {
                value.countDown();
                return value;
            }
        });
    }

    //----Operations----

    public void controllerRemoveOperation(String filename, PrintWriter out) throws InterruptedException {
        if(dstoreSessions.size() < R) {
            out.println("ERROR_NOT_ENOUGH_DSTORES"); out.flush();
        } else if (!index.setRemoveInProgress(filename)) {
            out.println("ERROR_FILE_DOES_NOT_EXIST"); out.flush();
        } else {
            //TODO: not sure if this is safe
            List<ControllerDstoreSession> dstores = new ArrayList<>(index.getDstores(filename));
            dstores.forEach( v -> v.sendMessageToDstore("REMOVE " + filename));

            CountDownLatch latch = new CountDownLatch(R);
            waitingRemoveAcks.put(filename, latch);
            if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                //TODO: log an error here -> timeout reached
                System.out.println("timeout reached");
                index.removeFile(filename);
                waitingStoreAcks.remove(filename);
            } else {
                index.removeFile(filename);
                out.println("REMOVE_COMPLETE");
                waitingStoreAcks.remove(filename);
                out.flush();
            }
        }
    }

    public void controllerRemoveError(){

    }

    public void controllerListOperation(PrintWriter out){
        if(dstoreSessions.size() < R){
            out.println("ERROR_NOT_ENOUGH_DSTORES");
        } else {
            StringBuilder output = new StringBuilder("LIST");
            List<MyFile> files = new ArrayList<>(index.getFiles());
            for (MyFile f : files){
                if(f.exists())
                output.append(" ").append(f.getName());
            }
            out.println(output);
        }
        out.flush();
    }

    public void controllerStoreOperation(String filename, int filesize, PrintWriter out) throws InterruptedException {
        List<Integer> list = new ArrayList<>(index.getNDstores(R));
//        synchronized (Lock.DSTORE){
//            list = index.getNDstores(R);
//            System.out.println(Arrays.toString(list.toArray()));
//        }
        if(list.size() < R) {
            out.println("ERROR_NOT_ENOUGH_DSTORES"); out.flush();
        } else if (!index.setStoreInProgress(filename, filesize)) {
            out.println("ERROR_FILE_ALREADY_EXISTS"); out.flush();
        } else {
//            Collections.shuffle(list.subList(0,R));
//            System.out.println(Arrays.toString(list.toArray()));
            StringBuilder output = new StringBuilder("STORE_TO");
            list.forEach( port ->  output.append(" ").append(port));
            System.out.println("Selected dstores: " + output);
            CountDownLatch latch = new CountDownLatch(R);
            waitingStoreAcks.put(filename, latch);
            out.println(output); out.flush();
            if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                //TODO: log an error here -> timeout reached
                System.out.println("timeout reached");
                index.removeFile(filename);
                waitingStoreAcks.remove(filename);
            } else {
                index.setStoreComplete(filename);
                out.println("STORE_COMPLETE");
                waitingStoreAcks.remove(filename);
                out.flush();
            }
        }
    }

    public void controllerLoadOperation(String filename, PrintWriter out, ControllerClientSession session){
        if (!index.fileExists(filename)){
            out.println("ERROR_FILE_DOES_NOT_EXIST"); out.flush();
            return;
        }
        List<ControllerDstoreSession> dstoreSessions = new ArrayList<>(index.getDstores(filename));
        if(dstoreSessions.size() < R){
            out.println("ERROR_NOT_ENOUGH_DSTORES");
        } else {
            int dstorePort = dstoreSessions.get(0).getDstorePort();
            session.loadCounter.add(dstorePort);
            out.println("LOAD_FROM " + dstorePort + " " + index.getFileSize(filename));
        }
        out.flush();
    }

    public void controllerReloadOperation(String filename, PrintWriter out, ControllerClientSession session) {
        List<ControllerDstoreSession> dstoreSessions = new ArrayList<>(index.getDstores(filename));
        if(dstoreSessions.size() < R){
            out.println("ERROR_NOT_ENOUGH_DSTORES"); out.flush();
            return;
        }
        List<Integer> dstorePorts = new ArrayList<>();
        dstoreSessions.forEach(dstore -> dstorePorts.add(dstore.getDstorePort()));
        dstorePorts.removeAll(session.loadCounter);
        if(dstorePorts.isEmpty()) {
            out.println("ERROR_LOAD");
        } else {
            int dstorePort = dstorePorts.get(0);
            session.loadCounter.add(dstorePort);
            out.println("LOAD_FROM " + dstorePort + " " + index.getFileSize(filename));
        }
        out.flush();
    }

    //java Controller cport R timeout rebalance_period
    public static void main(String[] args) {
        int cport, R, timeout, rebalance_period;
        if(args.length < 4) {
            System.out.println("Not enough parameters provided");
            return;
        }
        try { cport = Integer.parseInt(args[0]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of cport!"); return;}
        try { R = Integer.parseInt(args[1]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of R!"); return;}
        try { timeout = Integer.parseInt(args[2]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of timeout!"); return;}
        try { rebalance_period = Integer.parseInt(args[3]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of rebalance_period!"); return;}

        try { new Controller(cport, R, timeout, rebalance_period); }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
