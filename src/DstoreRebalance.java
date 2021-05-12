import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DstoreRebalance {

    private Map<String, List<Integer>> filesToSend;
    private List<String> filesToRemove;
    private final Dstore dstore;

    public DstoreRebalance(Dstore dstore) {
        this.dstore = dstore;
    }

    public void rebalance(String[] lineSplit) throws IOException {
        parseRemove( parseAdd(lineSplit) );
        System.out.println("(i) REBALANCE: FILES TO SEND");
        filesToSend.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });
        System.out.println("(i) REBALANCE: FILES TO REMOVE");
        System.out.println(Arrays.toString(filesToRemove.toArray()));

        if (!filesToSend.isEmpty()){
            CountDownLatch latch = new CountDownLatch(filesToSend.values().size());
            rebalanceSend(latch);
            try { latch.await(dstore.TIMEOUT, TimeUnit.MILLISECONDS); }      //TODO: ADD TIMEOUT
            catch (InterruptedException e) {
                System.out.println("(X) DSTORE REBALANCE ERROR, SENDING TIMED OUT");
            }
        }
        if (!filesToRemove.isEmpty()) {
            for (String filename : filesToRemove) {
                Dstore.FileRecord f = dstore.files.remove(filename);
                boolean deleted = f.delete();
                System.out.println("DELETING: " + f.getName() + " " + deleted);
            }
        }
        dstore.send(Protocol.REBALANCE_COMPLETE_TOKEN, dstore.CSOCKET);
    }

    private void rebalanceSend(CountDownLatch latch) {
        filesToSend.forEach( (filename, dstores) -> {
            for (int port : dstores) {
                try {
                    Socket socket = new Socket("localhost", port);
                    System.out.println("CREATED A SOCKET PORT: " + port);
                    new Thread( () -> {
                        try { sendRebalanceFile(socket, filename, latch); }
                        catch (IOException e) { e.printStackTrace();}
                    }).start();
                    dstore.send(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + dstore.files.get(filename).getFilesize(), socket);
//                    new Thread( () -> {
//                        try {
//                            send(Protocol.REBALANCE_STORE_TOKEN + " " + filename + " " + getFile(filename).getFilesize(), socket);
//                        } catch (IOException e) { e.printStackTrace(); }
//                    }).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void sendRebalanceFile(Socket socket, String filename, CountDownLatch latch) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        socket.setSoTimeout(dstore.TIMEOUT);
        String line;
        try {
            line = in.readLine();
            System.out.println(line);
        } catch (Exception e) {
            e.printStackTrace();
            latch.countDown();
            return;
        } if (line == null) {
            throw new AssertionError("(!!!) LINE TO READ WAS NULL");
        }
        socket.shutdownInput();
        if(!line.equals(Protocol.ACK_TOKEN)) {
//            latch.countDown();
            throw new AssertionError("(!!!) LINE TO READ WAS NULL");
        } else {
            InputStream inf;
            OutputStream outf = socket.getOutputStream();
            try {
                System.out.println("HERE");
                File file = new File(dstore.FILE_FOLDER + "/" + filename);
                System.out.println(file.getName());
                inf = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                latch.countDown();
                return;
            }
            byte[] bytes = inf.readNBytes(dstore.files.get(filename).getFilesize());
            outf.write(bytes);
            inf.close();
            outf.close();
            System.out.println("(i) SUCCESSFULY SENT FILE: " + filename + " TO PORT: " + socket.getPort() );
        }
        latch.countDown();
    }


    private void parseRemove(String[] msg) {
        filesToRemove = new ArrayList<>();
        int numberToRemove;
        try { numberToRemove = Integer.parseInt(msg[0]); }
        catch (NumberFormatException e) { return; }
        for (int i = 1; i <= numberToRemove; i++) {
            filesToRemove.add(msg[i]);
        }
    }

    private String[] parseAdd(String[] lineSplit) {
        String[] msg = Arrays.copyOfRange(lineSplit,1,lineSplit.length);
        filesToSend = new HashMap<>();
        try {
            int numberTosend = Integer.parseInt(msg[0]);
            int j = 1;
            for (int i = 0; i < numberTosend; i++) {
                String filename = msg[j];
                j++;
                int dstoresAmount = Integer.parseInt(msg[j]);
                j++;
                for (int x = 0; x < dstoresAmount; x++) {
                    int dstorePort = Integer.parseInt(msg[j]);
                    j++;
                    List<Integer> list = filesToSend.getOrDefault(filename, new ArrayList<>());
                    list.add(dstorePort);
                    filesToSend.put(filename, list);
                }
            }
            return Arrays.copyOfRange(lineSplit,j+1,lineSplit.length);
        } catch (NumberFormatException e) {
            filesToSend.clear();
            return Arrays.copyOfRange(lineSplit,2,lineSplit.length);
        }
    }
}
