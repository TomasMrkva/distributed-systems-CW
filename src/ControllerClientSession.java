import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Controller -> Dstore connection
 */
public class ControllerClientSession extends Session {

    private final PrintWriter out;
    private final Controller controller;

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
//                System.out.println("STORE:" + Arrays.toString(messageSplit));
                controller.controllerStoreOperation(messageSplit[1],Integer.parseInt(messageSplit[2]),out);
                break;
            case "LOAD":
                controller.controllerLoadOperation(messageSplit[1],out);
            default:
                System.out.println("NOT MATCHED");
        }
    }
}
