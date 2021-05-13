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
    final public Index index;
    final private ServerSocket ss;
    final private int REBALANCE_PERIOD;

    public final AtomicBoolean rebalance = new AtomicBoolean(false);
    private HashMap<Integer, String[]> rebalceResult = new HashMap<>();
    public BlockingQueue<QueuedOperation> clientQueue = new LinkedBlockingQueue<>();
    public BlockingQueue<QueuedOperation> dstoreQueue = new LinkedBlockingQueue<>();
    public ConcurrentHashMap<Integer, ArrayList<String>> rebalanceFiles = new ConcurrentHashMap<>();
    public ScheduledExecutorService scheduler;
    public CountDownLatch rebalanceLatch;
    public CountDownLatch rebalanceCompleteLatch;

    public final ConcurrentHashMap<Integer, ControllerDstoreSession> dstoreSessions = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, CountDownLatch> waitingStoreAcks = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, CountDownLatch> waitingRemoveAcks = new ConcurrentHashMap<>();

    public final Object rebalanceLock = new Object();
    private final Object storeLock = new Object();
    private final Object removeAcksLock = new Object();
    private final Object dstoresLock = new Object();
    private final Object storeAcksLock = new Object();

    public Controller(int cport, int r, int timeout, int rebalance_period) throws Exception {
        R = r;
        TIMEOUT = timeout;
        REBALANCE_PERIOD = rebalance_period;
        ss = new ServerSocket(cport);
        index = new Index(this);
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);
        new Thread(this::processClientQueue).start();
        if (R > 1) {
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.schedule(this::automaticRebalance, rebalance_period, TimeUnit.MILLISECONDS);
        }
        run();
    }

    private void run() throws IOException {
        while (true) {
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
                            new Thread(new ControllerDstoreSession(dstorePort, client, this, line)).start();
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
                    try {
                        new Thread(new ControllerClientSession(q.getClient(), this, q.getMessage())).start();
                    } // creates a new session
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        q.getClientSession().singleOperation(q.message);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public void joinOperation(int dstorePort, ControllerDstoreSession session) {
        if (R > 1) {
            if (rebalance.compareAndSet(false, true)) {
                dstoreSessions.put(dstorePort, session);
                System.out.println("(+) DSTORES: " + dstoreSessions.size());
                rebalance(true);
            } else {
                dstoreQueue.add(new QueuedOperation(dstorePort, session));
            }
        } else {
            dstoreSessions.put(dstorePort, session);
        }
    }

    public void dstoreClosedNotify(int dstorePort) {
        synchronized (dstoresLock) {
            dstoreSessions.remove(dstorePort);
            index.removeDstore(dstorePort);
            System.out.println("(-) DSTORES: " + dstoreSessions.mappingCount());
        }
    }

    public void addStoreAck(String filename, ControllerDstoreSession dstore) {
        synchronized (storeAcksLock) {
//            System.out.println("Store ack for : " + filename + " from " + dstore.getDstorePort());
//            System.out.println("Store hashmap contains : " + filename + "? " + waitingStoreAcks.containsKey(filename));
            waitingStoreAcks.computeIfPresent(filename, (key, value) -> {
//                index.addDstore(filename, dstore);
                if (value.getCount() == 1) {
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
        synchronized (removeAcksLock) {
//            System.out.println("Remove ack for : " + filename + " from " + dstore.getDstorePort());
//            System.out.println("Remove hashmap contains : " + filename + " ? " + waitingRemoveAcks.containsKey(filename));

            waitingRemoveAcks.computeIfPresent(filename, (key, value) -> {
//                index.removeDstore(filename, dstore);
                if (value.getCount() == 1) {
                    value.countDown();
                    return null;
                } else {
                    value.countDown();
                    return value;
                }
            });
        }
    }

    public void setRebalanceResult(HashMap<Integer, String[]> result) {
        this.rebalceResult = result;
    }
// synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {}

//--------------------------------------Operations--------------------------------------------

    public void automaticRebalance() {
        if (rebalance.compareAndSet(false, true)) {
            rebalance(false);
        } else {
            System.out.println("(X) PREVIOUS REBALANCE STILL IN PROGRESS");
        }
    }

    public void rebalance(boolean join) {
        synchronized (rebalanceLock) {
            System.out.println("(i) REBALANCE STARTING");
            while (!index.readyToRebalance()) {}
//                try { Thread.sleep(10); }
//                catch (InterruptedException e) {};
            System.out.println("(i) REBALANCE: FINISHED WAITING FOR ANY STORE/REMOVE");
            rebalanceAction();
//                System.out.println("(i) REBALANCE: FINISHED MAIN REBALANCE");
            while (!dstoreQueue.isEmpty()) {
                QueuedOperation q = dstoreQueue.poll();
                System.out.println("UNQUEUED: " + q.getMessage());
                dstoreSessions.put(q.getPort(), q.getDstoreSession());
                System.out.println("(+) DSTORES: " + dstoreSessions.size());
                rebalanceAction();
            }
            System.out.println("(i) REBALANCE: FINISHED");
            if (join) {
                scheduler.shutdownNow();
                System.out.println("JOIN: REMOVED SCHEDULED: " + scheduler.isTerminated());
            } else {
                scheduler.shutdown();
                System.out.println("AUTOMATIC: REMOVED SCHEDULED: " + scheduler.isTerminated());
            }
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.schedule(this::automaticRebalance, REBALANCE_PERIOD, TimeUnit.MILLISECONDS);
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
                dstoreSessions.keySet().forEach(port -> {
                    if (!rebalanceFiles.containsKey(port)) dstoresToRemove.add(port);
                });
                dstoresToRemove.forEach(dstorePort -> {
                    try {
                        dstoreSessions.get(dstorePort).closeSession();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
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
                index.updateIndex();
                index.print();
                System.out.println("(i) INDEX CONFIRMATION: INDEX UPDATE FINISHED");
                Thread t = new Thread(() -> {
                    try {
                        if (!rebalanceCompleteLatch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                            System.out.println("(X) REBALANCE ERROR: NOT ALL REBALANCE_COMPLETE RECEIVED");
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                t.start();
                rebalceResult.forEach((port, str) -> {
                    ControllerDstoreSession session = dstoreSessions.get(port);
                    if (session != null && str != null) {
                        session.sendMessageToDstore(Protocol.REBALANCE_TOKEN + " " + String.join(" ", str));
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
            if (!index.setRemoveInProgress(filename)) {
                session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                return;
            }
        }
//        Thread.sleep(5000);
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
//        synchronized (storeLock) {
        if (dstoreSessions.mappingCount() < R) {
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        try {
            dstores = new ArrayList<>(index.getDstores(filename, true));
        } catch (NullPointerException e) {
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        CountDownLatch latch = new CountDownLatch(R);
        waitingRemoveAcks.put(filename, latch);

        new Thread ( () -> dstores.forEach(dstoreSession -> dstoreSession.sendMessageToDstore("REMOVE " + filename))).start();
        if (!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
            System.out.println("(X) Waiting for REMOVE_ACKS: TIMEOUT REACHED");
        }
        waitingRemoveAcks.remove(filename);
        index.removeFile(filename);
        session.send(Protocol.REMOVE_COMPLETE_TOKEN);
        index.print();
//        }
    }

    public void controllerListOperation(ControllerClientSession session) {
        if (dstoreSessions.mappingCount() < R) {
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        } else {
            StringBuilder output = new StringBuilder(Protocol.LIST_TOKEN);
            List<IndexFile> files = new ArrayList<>(index.getFiles());
            for (IndexFile f : files) {
                if (f.exists())
                    output.append(" ").append(f.getName());
            }
            session.send(output.toString());
        }
    }

    public void controllerStoreOperation(String filename, int filesize, ControllerClientSession session) throws InterruptedException {
//        CountDownLatch latch = null;
        synchronized (rebalanceLock) {
            if (!index.setStoreInProgress(filename, filesize)) {
                session.send(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                return;
            }
        }
        System.out.println("(i) STORE -> NO REBALANCE SHOULD BE HAPPENING");

//      synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
        List<ControllerDstoreSession> list = null;
        synchronized (storeLock) {
            try { list = new ArrayList<>(index.getRDstores(R)); }
            catch (NullPointerException e) {
                session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                index.removeFile(filename);
                return;
            }
            index.setDstores(filename, list);
        }
        if (list.size() < R) {
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            index.removeFile(filename);
            return;
        }
        StringBuilder output = new StringBuilder(Protocol.STORE_TO_TOKEN);
        list.forEach(dstore -> output.append(" ").append(dstore.getDstorePort()));

        CountDownLatch latch = new CountDownLatch(R);
        waitingStoreAcks.put(filename, latch);

        session.send(output.toString());
        if (!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
            System.out.println("(X) Waiting for STORE_ACKS: TIMEOUT REACHED");
            waitingStoreAcks.remove(filename);
            System.out.println("(i) REBALANCE CAN START");
            index.removeFile(filename);
        } else {
            waitingStoreAcks.remove(filename);
            System.out.println("(i) REBALANCE CAN START");
            index.setStoreComplete(filename);
            session.send(Protocol.STORE_COMPLETE_TOKEN);
        }
        index.print();
    }


    public void controllerLoadOperation(String filename, ControllerClientSession session) {
        if (!index.fileExists(filename)) {
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
//        synchronized (filenameKeys.computeIfAbsent(filename, k -> new Object())) {
        if (dstoreSessions.mappingCount() < R) {
            session.send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        List<ControllerDstoreSession> dstoreSessions;
        try {
            dstoreSessions = new ArrayList<>(index.getDstores(filename, false));
        } catch (NullPointerException e) {
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
        if (dstoreSessions.isEmpty()) {
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
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
        try {
            dstoreSessions = new ArrayList<>(index.getDstores(filename, false));
        } catch (NullPointerException e) {
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }
        if (dstoreSessions.isEmpty()) {
            session.send(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
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
        if (args.length < 4) {
            System.out.println("Not enough parameters provided");
            return;
        }
        try {
            cport = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Wrong value of cport!");
            return;
        }
        try {
            R = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Wrong value of R!");
            return;
        }
        try {
            timeout = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("Wrong value of timeout!");
            return;
        }
        try {
            rebalance_period = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("Wrong value of rebalance_period!");
            return;
        }

        try {
            new Controller(cport, R, timeout, rebalance_period);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
