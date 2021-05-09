import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Controller {

    static class QueuedOperation {

        private final String message;
        private final Socket client;
        private final ControllerClientSession session;

        public QueuedOperation(String message, Socket client, ControllerClientSession session) {
            this.message = message;
            this.client = client;
            this.session = session;
        }

        public Socket getClient() {
            return client;
        }

        public String getMessage() {
            return message;
        }

        public ControllerClientSession getSession() {
            return session;
        }
    }

    final public int R;
    final public int TIMEOUT;
    final public int REBALANCE_PERIOD;
    final private ServerSocket ss;
    final public Index index;

    public AtomicBoolean rebalance = new AtomicBoolean(false);
    public BlockingQueue<QueuedOperation> queue = new LinkedBlockingQueue<>();
    public ConcurrentHashMap<Integer, List<String>> rebalanceFiles;
    public CountDownLatch rebalanceLatch;

    public ConcurrentHashMap<Integer,ControllerDstoreSession> dstoreSessions;
    public ConcurrentHashMap<String, CountDownLatch> waitingStoreAcks, waitingRemoveAcks;
    public ConcurrentHashMap<String, Object> filenameKeys;

    private final Object storeAcksLock = new Object();
    private final Object removeAcksLock = new Object();
    private final Object dstoresLock = new Object();
    private HashMap<Integer,String[]> rebalceResult;
    public final Object rebalanceLock = new Object();
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
        ScheduledExecutorService se = Executors.newSingleThreadScheduledExecutor();
        final ScheduledFuture<?> rebalanceTask = se.scheduleAtFixedRate(this::rebalanceOperation,rebalance_period,rebalance_period,TimeUnit.MILLISECONDS);
//        se.schedule( () -> {
//            rebalanceTask.cancel(true);// This will cancel your rebalanceTask after 5 sec without effecting ShortTask
//        }, 3000, TimeUnit.MILLISECONDS);

        processQueue();
        run();
    }

    private void run() throws IOException {
        while (true){
            Socket client = ss.accept();
            System.out.println("Controller connection established by: " + client.getRemoteSocketAddress());
            new Thread( () -> {
                try {
                    String line = new BufferedReader(new InputStreamReader(client.getInputStream())).readLine();
//                    String line = in.readLine(); //TODO: line can be null if user doesnt write anything
                    queue.add(new QueuedOperation(line, client, null));
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            }).start();
        }
    }

    private void processQueue() {
        new Thread( () -> {
            while (true) {
                if (!queue.isEmpty() && !rebalance.get()) {
                    QueuedOperation q = queue.poll();
                    if (q == null) {
                        continue;
                    } else if (q.session == null){
                        try { this.createSessions(q.getMessage(),q.getClient()); }
                        catch (IOException e) { e.printStackTrace();}
                    } else {
                        try { q.session.singleOperation(q.message); }
                        catch (InterruptedException e) { e.printStackTrace(); }
                    }
                }
                try { Thread.sleep(10); }
                catch (InterruptedException e) { e.printStackTrace(); }
            }
        }).start();
    }

    private void createSessions(String line, Socket client) throws IOException {
        String[] lineSplit = line.split(" ");
        if (lineSplit[0].equals(Protocol.JOIN_TOKEN)) {
            int dstorePort = Integer.parseInt(lineSplit[1]);
            new Thread(new ControllerDstoreSession(dstorePort,client,this, line)).start();
        } else {
            new Thread(new ControllerClientSession(client, this, line)).start();
        }
    }

    public void dstoreClosedNotify(int dstorePort) {
//        synchronized (Lock.DSTORE){
        synchronized (dstoresLock) {
            dstoreSessions.remove(dstorePort);
            index.removeDstore(dstorePort);
            System.out.println("Dstores: " + dstoreSessions.mappingCount());
        }
    }

    public void addStoreAck(String filename, ControllerDstoreSession dstore) {
        synchronized (storeAcksLock){
            System.out.println("Store ack for : " + filename + " from " + dstore.getDstorePort());
            System.out.println("Store hashmap contains : " + filename + "? " + waitingStoreAcks.containsKey(filename));
            waitingStoreAcks.computeIfPresent(filename,(key, value) -> {
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
        synchronized (removeAcksLock){
            System.out.println("Remove ack for : " + filename + " from " + dstore.getDstorePort());
            System.out.println("Remove hashmap contains : " + filename + "? " + waitingRemoveAcks.containsKey(filename));

            waitingRemoveAcks.computeIfPresent(filename,(key,value) -> {
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

    public void setRebalanceResult(HashMap<Integer,String[]> result){
        this.rebalceResult = result;
    }

    //----Operations----
    // synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {}

    public void rebalanceOperation() {
        synchronized (rebalanceLock) {
            long startTime = System.currentTimeMillis(); //fetch starting time
            if (rebalance.compareAndSet(false, true)) {
                while (waitingStoreAcks.mappingCount() != 0 && waitingRemoveAcks.mappingCount() != 0
                        && index.readyToRebalance()) {
                    System.out.println("WAITING");
                }
                ;    //busy waiting for removes/stores
                HashMap<Integer, List<String>> copy;
                rebalanceLatch = new CountDownLatch(dstoreSessions.size());
                dstoreSessions.values().forEach(session -> session.sendMessageToDstore(Protocol.LIST_TOKEN));
                try {
                    if (!rebalanceLatch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                        System.out.println("(!) WAITING FOR LIST FROM DSTORES: TIMEOUT");
                        copy = new HashMap<>(rebalanceFiles);
                        List<Integer> dstoresToRemove = new ArrayList<>();
                        dstoreSessions.keySet().forEach(port -> {
                            if (!rebalanceFiles.containsKey(port)) dstoresToRemove.add(port);
                        });
                        dstoresToRemove.forEach( dstorePort -> {
                            try { dstoreSessions.get(dstorePort).closeSession(); }
                            catch (IOException e) { e.printStackTrace();}
//                            dstoreClosedNotify(dstorePort);
                        });
                    } else {
                        copy = new HashMap<>(rebalanceFiles);
                    }
                    if (copy.size() == 0) {
                        System.out.println("ERROR!");
                        rebalance.set(false);
                        //                    unblockQueue();
                        return;
                    }
                    CountDownLatch waitForRebalance = new CountDownLatch(1);
                    new Thread(new Rebalance(copy, this, R, index, waitForRebalance)).start();
                    if (!waitForRebalance.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                        System.out.println("(!) TIMEOUT REACHED WHILE REBALANCING");
                    } else {
                        System.out.println("REBALANCE COMPLETE");
                        index.updateIndex();
                        System.out.println("HERE");
                        dstoreSessions.forEach( (port,session) -> {
                            String[] str = rebalceResult.get(port);
                            session.sendMessageToDstore(Protocol.REBALANCE_TOKEN + " " + ((str != null) ? String.join(",",str) : "NO_CHANGES"));
                        });
                    }
                } catch (InterruptedException e) {
                    System.out.println("SOMETHING WRONG HAPPENED");
                    e.printStackTrace();
                }
                System.out.println("SET TO FALSE");
                rebalance.set(false);
                //            unblockQueue();
            } else {
                System.out.println("Rebalance still in progress!");
            }
        }
    }

    public void controllerRemoveOperation(String filename, ControllerClientSession session) throws InterruptedException {
        List<ControllerDstoreSession> dstores;
        if (!index.setRemoveInProgress(filename)){
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
            if (dstoreSessions.mappingCount() < R) {
                session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
            try{ dstores = new ArrayList<>(index.getDstores(filename)); }
            catch (NullPointerException e) { session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN); return;}
            dstores.forEach( dstoreSession -> dstoreSession.sendMessageToDstore("REMOVE " + filename));
            CountDownLatch latch = new CountDownLatch(R);
            synchronized (rebalanceLock) {
                waitingRemoveAcks.put(filename, latch);
            }
            if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                System.out.println("(!) Waiting for REMOVE_ACKS: TIMEOUT");
                waitingRemoveAcks.remove(filename);
//                index.setRemoveComplete(filename);  //TODO: remove file in rebalance
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

    public void controllerStoreOperation(String filename, int filesize, ControllerClientSession session) throws InterruptedException {
        if (!index.setStoreInProgress(filename, filesize)) {
            session.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
            List<Integer> list = new ArrayList<>(index.getRDstores(R));
            System.out.println("SELECTED DSTORES: " + Arrays.toString(list.toArray()));
            if(list.size() < R) {
                session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }
            StringBuilder output = new StringBuilder(Protocol.STORE_TO_TOKEN);
            list.forEach(port -> output.append(" ").append(port));
            System.out.println("Selected dstores: " + output);
            CountDownLatch latch = new CountDownLatch(R);
            synchronized (rebalanceLock) {
                waitingStoreAcks.put(filename, latch);
            }
            session.send(output.toString());
            if (!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                System.out.println("(!) Waiting for STORE_ACKS: TIMEOUT");
                waitingStoreAcks.remove(filename);
                System.out.println("timeout reached");
                index.removeFile(filename);
            } else {
                waitingStoreAcks.remove(filename);
                index.setStoreComplete(filename);
                session.send(Protocol.STORE_COMPLETE_TOKEN);
            }
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
            try{dstoreSessions = new ArrayList<>(index.getDstores(filename));}
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
            try { dstoreSessions = new ArrayList<>(index.getDstores(filename)); }
            catch (NullPointerException e) {  session.send(Protocol.ERROR_LOAD_TOKEN); return; }
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
