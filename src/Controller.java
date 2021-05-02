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
    public final ConcurrentHashMap<String, CountDownLatch> waitingStoreAcks;
    public final ConcurrentHashMap<String, CountDownLatch> waitingRemoveAcks;
    public ConcurrentHashMap<String, Object> filenameKeys;

    public Controller(int cport, int r, int timeout, int rebalance_period) throws Exception {
        CPORT = cport;
        R = r;
        TIMEOUT = timeout;
        REBALANCE_PERIOD = rebalance_period;
        ss = new ServerSocket(cport);
        dstoreSessions = new ConcurrentHashMap<>();
        waitingStoreAcks = new ConcurrentHashMap<>();
        waitingRemoveAcks = new ConcurrentHashMap<>();
        filenameKeys = new ConcurrentHashMap<>();
        index = new Index(this);
        ControllerLogger.init(Logger.LoggingType.ON_TERMINAL_ONLY);
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
                    if (lineSplit[0].equals(Protocol.JOIN_TOKEN)) {
                        int dstorePort = Integer.parseInt(lineSplit[1]);
                        ControllerDstoreSession cd = new ControllerDstoreSession(dstorePort,client,this, line);
//                        dstoreSessions.put(dstorePort,cd);
//                        System.out.println("Dstores: " + dstoreSessions.size());
                        new Thread(cd).start();
                    } else {
//                        System.out.println(line);
                        ControllerClientSession cc = new ControllerClientSession(client, this, line);
                        new Thread(cc).start();
                    }
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            }).start();
        }
    }

    public void dstoreClosedNotify(int dstorePort) {
//        synchronized (Lock.DSTORE){
            dstoreSessions.remove(dstorePort);
            index.removeDstore(dstorePort);
            System.out.println("Dstores: " + dstoreSessions.mappingCount());
//        }
    }

    public void addStoreAck(String filename, ControllerDstoreSession dstore) {
        synchronized (waitingStoreAcks){
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
    }

    public void addRemoveAck(String filename, ControllerDstoreSession dstore) {
        synchronized (waitingRemoveAcks){
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
    }

    //----Operations----

    public void controllerRemoveOperation(String filename, ControllerClientSession session) throws InterruptedException {
        if(dstoreSessions.mappingCount() < R) {
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else if (!index.setRemoveInProgress(filename)) {
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        } else {
            synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())){
                List<ControllerDstoreSession> dstores;
                try{ dstores = new ArrayList<>(index.getDstores(filename)); }
                catch (NullPointerException e) { session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN); return;}

                dstores.forEach( dstoreSession -> dstoreSession.sendMessageToDstore("REMOVE " + filename));

                CountDownLatch latch = new CountDownLatch(R);
                waitingRemoveAcks.put(filename, latch);
                if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                    //TODO: log an error here -> timeout reached
                    System.out.println("timeout reached");
                    index.setRemoveComplete(filename);  //TODO: remove file in rebalance
//                index.removeFile(filename);
                    waitingStoreAcks.remove(filename);
                } else {
                    index.removeFile(filename);
                    waitingStoreAcks.remove(filename);
                    session.send(Protocol.REMOVE_COMPLETE_TOKEN);
                }
            }
        }
    }

    public void controllerRemoveError(){
        //TODO: here
    }

    public void controllerListOperation(ControllerClientSession session){
        if(dstoreSessions.mappingCount() < R){
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            StringBuilder output = new StringBuilder(Protocol.LIST_TOKEN);
            List<MyFile> files = new ArrayList<>(index.getFiles());
            for (MyFile f : files){
                if(f.exists())
                output.append(" ").append(f.getName());
            }
            session.send(output.toString());
        }
    }

    public void controllerStoreOperation(String filename, int filesize, ControllerClientSession session) throws InterruptedException{
        if (!index.setStoreInProgress(filename, filesize)) {
            session.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }
        List<Integer> list = new ArrayList<>(index.getNDstores(R));
        if(list.size() < R) {
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())){
                StringBuilder output = new StringBuilder(Protocol.STORE_TO_TOKEN);
                list.forEach( port ->  output.append(" ").append(port));
                System.out.println("Selected dstores: " + output);
                CountDownLatch latch = new CountDownLatch(R);
                waitingStoreAcks.put(filename, latch);
                session.send(output.toString());
                if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                    //TODO: log an error here -> timeout reached
                    System.out.println("timeout reached");
                    index.removeFile(filename);
                    waitingStoreAcks.remove(filename);
                } else {
                    index.setStoreComplete(filename);
                    waitingStoreAcks.remove(filename);
                    session.send(Protocol.STORE_COMPLETE_TOKEN);
                }
            }
        }
    }

    public void controllerLoadOperation(String filename, ControllerClientSession session){
        if (!index.fileExists(filename)){
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        } else if (dstoreSessions.mappingCount() < R){
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            List<ControllerDstoreSession> dstoreSessions;
            try{dstoreSessions = new ArrayList<>(index.getDstores(filename));}
            catch (NullPointerException e) {session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN); return;}
            if(dstoreSessions.isEmpty()){
                session.send(Protocol.ERROR_LOAD_TOKEN);
                return;
            }
            int dstorePort = dstoreSessions.get(0).getDstorePort();
            session.loadCounter.add(dstorePort);
            session.send(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + index.getFileSize(filename));
        }
    }

    public void controllerReloadOperation(String filename, ControllerClientSession session) {
        List<ControllerDstoreSession> dstoreSessions;
        try { dstoreSessions = new ArrayList<>(index.getDstores(filename)); }
        catch (NullPointerException e) {  session.send(Protocol.ERROR_LOAD_TOKEN); return; }
        if (dstoreSessions.isEmpty()) {
            session.send(Protocol.ERROR_LOAD_TOKEN);
            return;
        }
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
            List<Integer> dstorePorts = new ArrayList<>();
            dstoreSessions.forEach(dstore -> dstorePorts.add(dstore.getDstorePort()));
            dstorePorts.removeAll(session.loadCounter);
            if (dstorePorts.isEmpty()) {
                session.send(Protocol.ERROR_LOAD_TOKEN);
            } else {
                int dstorePort = dstorePorts.get(0);
                session.loadCounter.add(dstorePort);
                session.send(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + index.getFileSize(filename));
            }
//        }
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
