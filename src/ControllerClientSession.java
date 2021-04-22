import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Controller -> Dstore connection
 */
public class ControllerClientSession extends Session {

    private final PrintWriter out;
    private final Controller controller;
    private final String ERROR_MESSAGE = "ERROR_NOT_ENOUGH_DSTORES";

    public ControllerClientSession(Socket connection, Controller controller, String message) throws IOException {
        super(connection,message,"Client");
        this.controller = controller;
        out = new PrintWriter(connection.getOutputStream());
    }

    public void singleOperation(String message) throws InterruptedException {
//        System.out.println("Controller -> Client connection established");
        String[] messageSplit = message.split(" ");
        switch (messageSplit[0]){
            case "STORE":
                System.out.println("STORE:" + Arrays.toString(messageSplit));
                storeOperation(messageSplit[1],Integer.parseInt(messageSplit[2]));
                break;
            default:
                System.out.println("NOT MATCHED");
        }
    }

    private void storeOperation(String filename, Integer filesize) throws InterruptedException {
        if(controller.dstoreSessions.size() < controller.R){
            out.println(ERROR_MESSAGE);
            out.flush();
        } else {
            StringBuilder output = new StringBuilder("STORE_TO");
            Iterator<ControllerDstoreSession> iter = controller.dstoreSessions.values().iterator();
            for (int i = 0; i < controller.R && iter.hasNext(); i++){
                ControllerDstoreSession cstoreSession = iter.next();
                output.append(" ").append(cstoreSession.getDstorePort());
            }
            CountDownLatch latch = new CountDownLatch(controller.R);
            controller.addAcksLatch(filename, latch);
            out.println(output);
            out.flush();
            if(!latch.await(controller.TIMEOUT, TimeUnit.MILLISECONDS)){
                //TODO: log an error here -> timeout reached
                System.out.println("timeout reached");
                controller.waitingAcks.remove(filename);
            } else {
                out.println("STORE_COMPLETE");
                controller.waitingAcks.remove(filename);
                out.flush();
            }
        }
    }
}
