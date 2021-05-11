import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Controller {

    static class QueuedOperation {

        private final int port;
        private final String message;
        private final Socket client;
        private final ControllerClientSession clientSession;
        private final ControllerDstoreSession dstoreSession;

        public QueuedOperation(String message, Socket client, ControllerClientSession session) {
            this.message = message;
            this.client = client;
            this.clientSession = session;
            port = 0;
            dstoreSession = null;
        }

        public QueuedOperation(int port, ControllerDstoreSession session) {
            this.port = port;
            this.dstoreSession = session;
            message = null;
            client = null;
            clientSession = null;
        }

        public Socket getClient() {
            return client;
        }

        public String getMessage() {
            return message;
        }

        public int getPort() {
            return port;
        }

        public ControllerClientSession getClientSession() {
            return clientSession;
        }

        public ControllerDstoreSession getDstoreSession() {
            return dstoreSession;
        }
    }

    final public int R;
    final public int TIMEOUT;
    final public int REBALANCE_PERIOD;
    final private ServerSocket ss;
    final public Index index;

    public AtomicBoolean rebalance = new AtomicBoolean(false);
    public BlockingQueue<QueuedOperation> clientQueue = new LinkedBlockingQueue<>();
    public BlockingQueue<QueuedOperation> dstoreQueue = new LinkedBlockingQueue<>();
    public ConcurrentHashMap<Integer, ArrayList<String>> rebalanceFiles;
    public CountDownLatch rebalanceLatch;
    public CountDownLatch rebalanceCompleteLatch;

    public ConcurrentHashMap<Integer,ControllerDstoreSession> dstoreSessions;
    public ConcurrentHashMap<String, CountDownLatch> waitingStoreAcks, waitingRemoveAcks;
    public ConcurrentHashMap<String, Object> filenameKeys;

    private final Object storeAcksLock = new Object();
    private final Object removeAcksLock = new Object();
    private final Object dstoresLock = new Object();
    private HashMap<Integer,String[]> rebalceResult;
    public final Object rebalanceLock = new Object();
    final Object storeLock = new Object();
//    public final ReentrantLock rebalanceLock = new ReentrantLock();

    public Controller(int cport, int r, int timeout, int rebalance_period) throws Exception {
        R = r;
        TIMEOUT = timeout;
        REBALANCE_PERIOD = rebalance_period;
        ss = new ServerSocket(cport);
        dstoreSessions = new ConcurrentHashMap<>();
        waitingStoreAcks = new ConcurrentHashMap<>();
        waitingRemoveAcks = new ConcurrentHashMap<>();
        filenameKeys = new ConcurrentHashMap<>();
        rebalanceFiles = new ConcurrentHashMap<>();
        rebalceResult = new HashMap<>();
        index = new Index(this);
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        new Thread(this::processClientQueue).start();
//        ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();
//        ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();
//        se.scheduleWithFixedDelay(this::automaticRebalance,rebalance_period,rebalance_period,TimeUnit.MILLISECONDS);
//        new Thread( () -> {
//            try {
//                Thread.sleep(7000);
//                System.out.println("READY");
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            try {
//                dstoreSessions.get(4501).closeSession();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }).start();
        run();
    }

    private void run() throws IOException {
        while (true){
            Socket client = ss.accept();
            new Thread( () -> {
                try {
                    String line = new BufferedReader(new InputStreamReader(client.getInputStream())).readLine();
                    if (line == null) {
                        System.out.println("(X) NULL MESSAGE SENT");
                    } else {
                        String[] lineSplit = line.split(" ");
                        if (lineSplit[0].equals(Protocol.JOIN_TOKEN)) {
                            int dstorePort = Integer.parseInt(lineSplit[1]);
                            new Thread(new ControllerDstoreSession(dstorePort, client,this, line)).start();
                        } else {
                            clientQueue.add(new QueuedOperation(line, client, null));
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    private void processClientQueue() {
        while (true) {
            if (!clientQueue.isEmpty() && !rebalance.get()) {
                QueuedOperation q = clientQueue.poll();
                if (q == null) {
                    continue;
                } else if (q.getClientSession() == null) {
                    System.out.println("MESSAGE: " + q.getMessage());
                    try { new Thread(new ControllerClientSession(q.getClient(), this, q.getMessage())).start(); } // creates a new session
                    catch (IOException e) { e.printStackTrace(); }
                } else {
                    try { q.getClientSession().singleOperation(q.message); }
                    catch (InterruptedException e) { e.printStackTrace(); }
                }
            } else if (rebalance.get()){
                System.out.println("WAITING FOR REBALANCE TO FINISH");
            }
//            try { Thread.sleep(10); }
//            catch (InterruptedException e) { e.printStackTrace(); }
        }
    }

    public void joinOperation(int dstorePort, ControllerDstoreSession session) {
        if (rebalance.compareAndSet(false, false)) {
            dstoreSessions.put(dstorePort, session);
            System.out.println("Dstores: " + dstoreSessions.size());
            rebalance();
        } else {
            dstoreQueue.add(new QueuedOperation(dstorePort, session));
        }
    }

//    private void processDstoreQueue() {
//        while(true) {
//            if (!dstoreQueue.isEmpty() && !rebalance.get()) {
//                QueuedOperation q = dstoreQueue.poll();
//                if (q != null) {
//                    String[] lineSplit = q.getMessage().split(" ");
//                    int dstorePort = Integer.parseInt(lineSplit[1]);
//                    try {
//                        new Thread(new ControllerDstoreSession(dstorePort,q.getClient(),this, q.getMessage())).start();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//            try { Thread.sleep(10); }
//            catch (InterruptedException e) { e.printStackTrace(); }
//        }
//    }

//    private void createSessions(String line, Socket client) throws IOException {
//
//        if (lineSplit[0].equals(Protocol.JOIN_TOKEN)) {
//            int dstorePort = Integer.parseInt(lineSplit[1]);
//
//        } else {
//
//        }
//    }

    public void dstoreClosedNotify(int dstorePort) {
//        synchronized (Lock.DSTORE){
        synchronized (dstoresLock) {
            dstoreSessions.remove(dstorePort);
            index.removeDstore(dstorePort);
//            System.out.println("Dstores: " + dstoreSessions.mappingCount());
        }
    }

    public void addStoreAck(String filename, ControllerDstoreSession dstore) {
        synchronized (storeAcksLock){
//            System.out.println("Store ack for : " + filename + " from " + dstore.getDstorePort());
            System.out.println("Store hashmap contains : " + filename + "? " + waitingStoreAcks.containsKey(filename));
            waitingStoreAcks.computeIfPresent(filename,(key, value) -> {
//                index.addDstore(filename, dstore);
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
        synchronized (removeAcksLock){
//            System.out.println("Remove ack for : " + filename + " from " + dstore.getDstorePort());
            System.out.println("Remove hashmap contains : " + filename + " ? " + waitingRemoveAcks.containsKey(filename));

            waitingRemoveAcks.computeIfPresent(filename,(key,value) -> {
//                index.removeDstore(filename, dstore);
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

    public void setRebalanceResult(HashMap<Integer,String[]> result){
        this.rebalceResult = result;
    }
// synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {}

//--------------------------------------Operations--------------------------------------------

    public void automaticRebalance() {
        if (rebalance.compareAndSet(false, true)) {
            rebalance();
        } else {
            System.out.println("(X) PREVIOUS REBALANCE STILL IN PROGRESS");
        }
    }

    public void rebalance() {
        synchronized (rebalanceLock) {
            System.out.println("(i) REBALANCE STARTING");
            while (waitingStoreAcks.size() != 0 && waitingRemoveAcks.size() != 0 && index.readyToRebalance()) {
                System.out.println("WAITING");
            }
            System.out.println("(i) REBALANCE: FINISHED WAITING FOR ANY STORE/REMOVE");
            rebalanceAction();
            System.out.println("(i) REBALANCE: FINISHED MAIN REBALANCE");
            while (!dstoreQueue.isEmpty()) {
                QueuedOperation q = dstoreQueue.poll();
                System.out.println("QUEUED: " + q.getMessage());
                if (q != null) {
                    dstoreSessions.put(q.getPort(), q.getDstoreSession());
                    System.out.println("Dstores: " + dstoreSessions.size());
                    rebalance();
                }
            }
            System.out.println("(i) REBALANCE: FINISHED");
            rebalance.set(false);
        }
    }

    public void rebalanceAction() {
        HashMap<Integer, ArrayList<String>> copy;
        rebalanceLatch = new CountDownLatch(dstoreSessions.size());
        rebalanceFiles.clear();
        dstoreSessions.values().forEach(session -> session.sendMessageToDstore(Protocol.LIST_TOKEN));
        try {
            if (!rebalanceLatch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                System.out.println("(X) WAITING FOR LIST FROM DSTORES: TIMEOUT");
                copy = new HashMap<>(rebalanceFiles);
                List<Integer> dstoresToRemove = new ArrayList<>();
                dstoreSessions.keySet().forEach( port -> {
                    if (!rebalanceFiles.containsKey(port)) dstoresToRemove.add(port);
                });
                dstoresToRemove.forEach( dstorePort -> {
                    try {
                        System.out.println("WTF CLOSE PORT " + dstorePort);
                        dstoreSessions.get(dstorePort).closeSession(); }
                    catch (Exception e) { e.printStackTrace();}
                });
            } else {
                copy = new HashMap<>(rebalanceFiles);
            }
            if (copy.size() < R) {
                System.out.println("(X) REBALANCE_ERROR: DSTORES THAT SENT REPLIES < R");
                return;
            }
            CountDownLatch waitForRebalance = new CountDownLatch(1);
            new Thread(new Rebalance(copy, this, waitForRebalance)).start();
            if (!waitForRebalance.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                System.out.println("(X) REBALANCE ERROR: TIMEOUT REACHED WHILE REBALANCING");
            } else {
                rebalanceCompleteLatch = new CountDownLatch(rebalceResult.size());
                index.updateIndex();    //TODO: check this
                Thread t = new Thread( () -> {
                    try {
                        if(!rebalanceCompleteLatch.await(TIMEOUT,TimeUnit.MILLISECONDS)) {
                            System.out.println("(X) REBALANCE ERROR: NOT ALL REBALANCE_COMPLETE RECEIVED");
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                t.start();
                rebalceResult.forEach( (port,str) -> {
                    ControllerDstoreSession session = dstoreSessions.get(port);
                    if (session != null) {
                        session.sendMessageToDstore(Protocol.REBALANCE_TOKEN + " " + ((str != null) ? String.join(" ",str) : null));
                    }
                });
                t.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void controllerRemoveOperation(String filename, ControllerClientSession session) throws InterruptedException {
        List<ControllerDstoreSession> dstores;
        synchronized (rebalanceLock) {
            if (!index.setRemoveInProgress(filename)){
                session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
        }
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
//        synchronized (storeLock) {
            if (dstoreSessions.mappingCount() < R) {
                session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
            try {
                dstores = new ArrayList<>(index.getDstores(filename, true));
            } catch (NullPointerException e) {
                session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN); return;
            }
            dstores.forEach( dstoreSession -> dstoreSession.sendMessageToDstore("REMOVE " + filename));
            CountDownLatch latch = new CountDownLatch(R);
//            synchronized (rebalanceLock) {
                waitingRemoveAcks.put(filename, latch);
//            }
            if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                System.out.println("(X) Waiting for REMOVE_ACKS: TIMEOUT REACHED");
                waitingRemoveAcks.remove(filename);
                index.setRemoveComplete(filename);  //TODO: not sure about this
//                index.removeFile(filename);
            } else {
                waitingRemoveAcks.remove(filename);
//                index.removeFile(filename);
                index.setRemoveComplete(filename);
                session.send(Protocol.REMOVE_COMPLETE_TOKEN);
            }
//        }
    }

    public void controllerListOperation(ControllerClientSession session){
        if (dstoreSessions.mappingCount() < R) {
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

    public void controllerStoreOperation(String filename, int filesize, ControllerClientSession session) throws InterruptedException {
        synchronized (rebalanceLock) {
            if (!index.setStoreInProgress(filename, filesize)) {
                session.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                return;
            }
        }
//      synchronized (storeLock) {
//      synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
        List<ControllerDstoreSession> list;
        synchronized (storeLock) {
            list = new ArrayList<>(index.getRDstores(R));
            index.addDstores(filename, list);
        }
//      System.out.println("SELECTED DSTORES: " + Arrays.toString(list.toArray()));
        if (list.size() < R) {
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        StringBuilder output = new StringBuilder(Protocol.STORE_TO_TOKEN);
        list.forEach(dstore -> output.append(" ").append(dstore.getDstorePort()));
//      System.out.println("Selected dstores: " + output);
        CountDownLatch latch = new CountDownLatch(R);
//      synchronized (rebalanceLock) {
        waitingStoreAcks.put(filename, latch);
//      }
        session.send(output.toString());
        if (!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
            System.out.println("(X) Waiting for STORE_ACKS: TIMEOUT REACHED");
            waitingStoreAcks.remove(filename);
//          System.out.println("timeout reached");
            index.removeFile(filename);
//          index.removeDstores(filename,list);
        } else {
            waitingStoreAcks.remove(filename);
            index.setStoreComplete(filename);
            session.send(Protocol.STORE_COMPLETE_TOKEN);
        }
//        }
//        }
    }

    public void controllerLoadOperation(String filename, ControllerClientSession session){
        if (!index.fileExists(filename)){
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
            if (dstoreSessions.mappingCount() < R){
                session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
            List<ControllerDstoreSession> dstoreSessions;
            try{dstoreSessions = new ArrayList<>(index.getDstores(filename, false));}
            catch (NullPointerException e) {session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN); return;}
            if(dstoreSessions.isEmpty()){
                session.send(Protocol.ERROR_LOAD_TOKEN);
                return;
            }
            int dstorePort = dstoreSessions.get(0).getDstorePort();
            session.loadCounter.clear();
            session.loadCounter.add(dstorePort);
            session.send(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + index.getFileSize(filename));
//        }
    }

    public void controllerReloadOperation(String filename, ControllerClientSession session) {
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
            List<ControllerDstoreSession> dstoreSessions;
            try { dstoreSessions = new ArrayList<>(index.getDstores(filename, false)); }
            catch (NullPointerException e) { session.send(Protocol.ERROR_LOAD_TOKEN); return; }
            if (dstoreSessions.isEmpty()) {
                session.send(Protocol.ERROR_LOAD_TOKEN);
                return;
            }
            List<Integer> dstorePorts = new ArrayList<>();
            dstoreSessions.forEach(dstore -> dstorePorts.add(dstore.getDstorePort()));
            dstorePorts.removeAll(session.loadCounter);
            if (dstorePorts.isEmpty()) {
                session.send(Protocol.ERROR_LOAD_TOKEN);
                session.loadCounter.clear();
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
