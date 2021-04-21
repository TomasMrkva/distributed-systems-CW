import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Controller -> Dstore connection
 */
public class ControllerClientSession implements Runnable {

    private final BufferedReader in;
    private final PrintWriter out;
    private final Socket connection;
    private final Controller controller;
    private final String ERROR_MESSAGE = "ERROR_NOT_ENOUGH_DSTORES";

    public ControllerClientSession(Socket connection, Controller controller) throws IOException {
        this.connection = connection;
        this.controller = controller;
        in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        out = new PrintWriter(connection.getOutputStream());
    }

    @Override
    public void run(){
        String line;
        try{
            System.out.println("Controller -> Client connection established");
            while((line = in.readLine()) != null) {
                String[] lineSplit = line.split(" ");
                if(lineSplit[0].equals("STORE")){
                    String filename = lineSplit[1];
                    int filesize = Integer.parseInt(lineSplit[2]);
                    String output = storeOperation(filename, filesize);
                    if(!output.equals(ERROR_MESSAGE)) {
                        CountDownLatch latch = new CountDownLatch(controller.R);
                        controller.addAcksLatch(filename, latch);
                        out.println(output);
                        out.flush();
                        if(!latch.await(controller.TIMEOUT, TimeUnit.MILLISECONDS)){
                            //TODO: log an error here -> timeout reached
                        } else {

                        }
                    }
                }
                else {
                    System.out.println("NOT MATCHED");
                }
            }
        } catch (Exception ignored){}
        finally { try { connection.close(); }
            catch (IOException e) { e.printStackTrace(); }
        }
    }

    private String storeOperation(String filename, Integer filesize){
        if(controller.dstoreSessions.size() < controller.R){
            return ERROR_MESSAGE;
        } else {
            StringBuilder output = new StringBuilder("STORE_TO");
            Iterator<ControllerDstoreSession> iter = controller.dstoreSessions.values().iterator();
            for (int i = 0; i < controller.R && iter.hasNext(); i++){
                ControllerDstoreSession cstoreSession = iter.next();
                output.append(" ").append(cstoreSession.getDstorePort());
            }
            return output.toString();
        }
    }
}
