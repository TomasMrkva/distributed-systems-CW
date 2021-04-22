import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Controller {

    final public int R;
    final public int CPORT;
    final public int TIMEOUT;
    final public int REBALANCE_PERIOD;
    final private ServerSocket ss;
    public ConcurrentHashMap<Integer,ControllerDstoreSession> dstoreSessions;
    //public ConcurrentHashMap<String,Set<Integer>> dstoreAcks;
    public ConcurrentHashMap<String, CountDownLatch> waitingAcks;
//    public ConcurrentHashMap<Integer, ArrayList<String>> dstoresFiles;

    public Controller(int cport, int r, int timeout, int rebalance_period) throws Exception {
        CPORT = cport;
        R = r;
        TIMEOUT = timeout;
        REBALANCE_PERIOD = rebalance_period;
        ss = new ServerSocket(cport);
        dstoreSessions = new ConcurrentHashMap<>();
        waitingAcks = new ConcurrentHashMap<>();
        //dstoreAcks = new ConcurrentHashMap<>();
//        dstoresFiles = new ConcurrentHashMap<>();
        run();
    }

    private void run() throws IOException {
        while (true){
            Socket client = ss.accept();
            System.out.println("Controller connection established");
            new Thread( () -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line = in.readLine();
                    String[] lineSplit = line.split(" ");
                    if (lineSplit[0].equals("JOIN")) {
                        int dstorePort = Integer.parseInt(lineSplit[1]);
                        ControllerDstoreSession cd = new ControllerDstoreSession(dstorePort,client,this, line);
                        dstoreSessions.put(dstorePort,cd);
                        System.out.println("Dstores:" + dstoreSessions.size());
                        new Thread(cd).start();
                    } else {
                        System.out.println(line);
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
        dstoreSessions.remove(dstorePort);
        System.out.println("Dstores:" + dstoreSessions.size());
    }

    public void addAcksLatch(String filename, CountDownLatch latch){
        waitingAcks.put(filename, latch);
    }

    public void addDstoreAck(String filename) {
        waitingAcks.compute(filename,(key,value) -> {
           if(value.getCount() == 1) {
               value.countDown();
               return null;
           } else {
               value.countDown();
               return value;
           }
        });
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
