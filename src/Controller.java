import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Controller {

    final public int R;
    final public int CPORT;
    final public int TIMEOUT;
    final public int REBALANCE_PERIOD;
    final private ServerSocket ss;
    final public Index index;
    public ConcurrentHashMap<Integer,ControllerDstoreSession> dstoreSessions;
    public ConcurrentHashMap<String, CountDownLatch> waitingAcks;

    public Controller(int cport, int r, int timeout, int rebalance_period) throws Exception {
        CPORT = cport;
        R = r;
        TIMEOUT = timeout;
        REBALANCE_PERIOD = rebalance_period;
        ss = new ServerSocket(cport);
        dstoreSessions = new ConcurrentHashMap<>();
        waitingAcks = new ConcurrentHashMap<>();
        index = new Index(r);
        run();
    }

    private void run() throws IOException {
        while (true){
            Socket client = ss.accept();
            System.out.println("Controller connection established by: " + client.getRemoteSocketAddress());
            new Thread(() -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line = in.readLine();
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

    public synchronized void dstoreClosedNotify(int dstorePort) {
        dstoreSessions.remove(dstorePort);
        index.removeDstore(dstorePort);
        System.out.println("Dstores: " + dstoreSessions.size());
    }

    public void addDstoreAck(String filename, ControllerDstoreSession d) {
        waitingAcks.compute(filename,(key,value) -> {
            index.addDstore(filename, d);
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

    public void controllerStoreOperation(String filename, int filesize, PrintWriter out) throws InterruptedException {
        ControllerDstoreSession[] dstores = dstoreSessions.values().toArray(new ControllerDstoreSession[0]);
        if(dstores.length < R) {
            out.println("ERROR_NOT_ENOUGH_DSTORES"); out.flush();
        } else if (!index.setStoreInProgress(filename, filesize)) {
            out.println("ERROR_FILE_ALREADY_EXISTS"); out.flush();
        } else {
            StringBuilder output = new StringBuilder("STORE_TO");
            List<Integer> list = new ArrayList<>();
            IntStream.range(0, dstores.length).forEach(list::add);
            Collections.shuffle(list);

            for(int i = 0; i < R; i++)
                output.append(" ").append(dstores[list.get(i)].getDstorePort());
            System.out.println("Selected dstores: " + output);
            CountDownLatch latch = new CountDownLatch(R);
            waitingAcks.put(filename, latch);
            out.println(output);
            out.flush();

            if(!latch.await(TIMEOUT, TimeUnit.MILLISECONDS)){
                //TODO: log an error here -> timeout reached
                System.out.println("timeout reached");
                index.setStoreComplete(filename);
                waitingAcks.remove(filename);
            } else {
                index.setStoreComplete(filename);
                out.println("STORE_COMPLETE");
                waitingAcks.remove(filename);
                out.flush();
            }
        }
    }

    public void controllerLoadOperation(String filename, PrintWriter out) {
        List<ControllerDstoreSession> dstores = index.getDstores(filename);
        ControllerDstoreSession dstore = dstores.get(new Random().nextInt(dstores.size()));
        out.println("LOAD_FROM ");
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
            return;
        }
    }
}
