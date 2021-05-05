import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
    public ConcurrentHashMap<String, CountDownLatch> waitingStoreAcks;
    public ConcurrentHashMap<String, CountDownLatch> waitingRemoveAcks;
    public ConcurrentHashMap<String, Object> filenameKeys;

    private final Object storeAcksLock = new Object();
    private final Object removeAcksLock = new Object();
    private final Object dstoresLock = new Object();
    public final Object storeLock = new Object();
    public final Object removeLock = new Object();

//    public final IdMutexProvider MUTEX_PROVIDER;

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
        index = new Index(this);
//        MUTEX_PROVIDER = new IdMutexProvider();
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
        es.scheduleAtFixedRate(this::rebalanceOperation, rebalance_period,rebalance_period,TimeUnit.MILLISECONDS);
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
                    if (rebalance.get()) {
                        queue.add(new QueuedOperation(line, client, null));
                    } else {
                        createSessions(line, client);
                    }
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            }).start();
        }
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

    //----Operations----
    // synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {}

    public void rebalanceOperation() {
        if(rebalance.compareAndSet(false, true)) {
//            do {Thread.sleep(1000);}
            while (waitingStoreAcks.mappingCount() != 0 && waitingRemoveAcks.mappingCount() != 0
                    && index.readyToRebalance()) {};    //busy waiting for removes/stores
            rebalanceLatch = new CountDownLatch(dstoreSessions.size());
            try {
                if(!rebalanceLatch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                    System.out.println("(!) Waiting for LOAD_ACKS: TIMEOUT");

    //                index.removeFile(filename);
                } else {

                }
            } catch (InterruptedException e) { e.printStackTrace(); return;}
            dstoreSessions.values().forEach(session -> session.sendMessageToDstore(Protocol.LIST_TOKEN));
            for (ControllerDstoreSession session : dstoreSessions.values()) {
                session.sendMessageToDstore(Protocol.LIST_TOKEN);
            }
            //TODO: this might not be too good

        } else {
            System.out.println("Rebalance still in progress!");
        }
    }

//    private List<String> fileDistribution() {
//
//        Map<Integer, List<String>> dstoreContents = new HashMap<>(rebalanceFiles); //dstorePort -> [filenames] map
//        Map<String, List<Integer>> filesAllocation = new HashMap<>();   // filenames -> [Dstore_Ports]
//        Map<Integer, String[]> messages = new HashMap<>();  //[0] -> to send [1] -> to remove
//        Map<Integer, List<String>> filesToMove = new HashMap<>();
////        Map<Integer, Integer> freeSpaces = new HashMap<>();
//        Stack<Integer[]> freeSpaces = new Stack<>();
//
//        dstoreContents.forEach((k, v) -> v.forEach(filename -> {
//            List<Integer> list = filesAllocation.getOrDefault(filename, new ArrayList<>());
//            list.add(k);
//            filesAllocation.put(filename, list);
//        }));
//
//        dstoreContents.clear();
//
//        filesAllocation.forEach((filename, dstores) -> {
//            if (dstores.size() > R) {
////                List<Integer> head = dstores.subList(0, R);
//                List<Integer> tail = dstores.subList(R, dstores.size());
//                tail.forEach(dstorePort -> {
//                    String[] msg = messages.getOrDefault(dstorePort, new String[2]);
//                    msg[1] = msg[1] + " " + filename;   //saves the name of files to remove
//                    messages.put(dstorePort, msg);
//                });
//                dstores.subList(R, dstores.size()).clear();   //removes extra files
//            }
//            dstores.forEach(dstorePort -> {    //recreates dstorePort -> [filenames] map
//                List<String> list = dstoreContents.getOrDefault(dstorePort, new ArrayList<>());
//                list.add(filename);
//                dstoreContents.put(dstorePort, list);
//            });
//        });
//
//        double compute = (double) R * (filesAllocation.size()) / rebalanceFiles.size();
//        double floor = Math.floor(compute), ceil = Math.ceil(compute);
//
//        dstoreContents.forEach((k, v) -> {
//            if (v.size() < floor)
//                freeSpaces.push(new Integer[]{k, (int) ceil - v.size()});
//            else if (v.size() > ceil) {
//                List<String> tail = v.subList(R, v.size());
//                filesToMove.put(k, tail);
//                tail.clear();
//            }
//        });
//
//        filesToMove.forEach((originDS, files) -> {
//            freeSpaces.forEach((newDS, capacity) -> {
//                if (capacity > files.size()) {
//                    capacity = capacity - files.size();
//
//                }
//            });
//        });
//
//        filesToMove.entrySet().forEach(entry -> {
//            Integer port = entry.getKey();
//            List<String> files = entry.getValue();
//            while (!files.isEmpty()) {
//                Integer[] top = freeSpaces.peek();
//                int destinationPort = top[0];
//                int freeSpace = top[1];
//                if (freeSpace > files.size()) {
////                    freeSpaces.pop(); freeSpaces.push(new Integer[]{destinationPort,freeSpace - files.size()});
//                    top[1] = freeSpace - files.size();
//                    String[] msg = messages.get(destinationPort);
//                    for (String s : files) {
//                        msg[0] = msg[0] + " " + s;  //TODO: make sure that the files are not duplicate
//                    }
//                }
//            }
//        });
//
//    }



//        Map<Integer, >
//
//        for (String item : files) {
//            if (countMap.containsKey(item))
//                countMap.put(item, countMap.get(item) + 1);
//            else
//                countMap.put(item, 1);
//        }
//        files.clear();
//        countMap.forEach((k,v) -> {
//            if(v < R){
//                files.addAll(Collections.nCopies(R-v,k));   //TODO: finished here
//            }
//        });


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
            waitingRemoveAcks.put(filename, latch);
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
            waitingStoreAcks.put(filename, latch);
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
