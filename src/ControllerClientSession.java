import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

/**
 * Controller -> Dstore connection
 */
public class ControllerClientSession extends Session {

    private final PrintWriter out;
    private final Controller controller;
    public final List<Integer> loadCounter;

    public ControllerClientSession(Socket connection, Controller controller, String message) throws IOException {
        super(connection,message,"Client");
        this.controller = controller;
        out = new PrintWriter(connection.getOutputStream());
        loadCounter = new ArrayList<>();
    }

    public void singleOperation(String message) throws InterruptedException {
//        System.out.println("Controller -> Client connection established");
        String[] messageSplit = message.split(" ");
        switch (messageSplit[0]) {
            case "STORE" -> controller.controllerStoreOperation(messageSplit[1], Integer.parseInt(messageSplit[2]), out);
            case "LOAD" -> controller.controllerLoadOperation(messageSplit[1], out, this);
            case "RELOAD" -> controller.controllerReloadOperation(messageSplit[1], out, this);
            default -> System.out.println("NOT MATCHED");
        }
    }
}
