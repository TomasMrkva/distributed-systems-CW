import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

public class ControllerClientSession extends Session {

    private final PrintWriter out;
    private final Controller controller;
    public final List<Integer> loadCounter;
    public final Socket socket;

    public ControllerClientSession(Socket connection, Controller controller, String message) throws IOException {
        super(connection,message,"Client");
        this.socket = connection;
        this.controller = controller;
        out = new PrintWriter(connection.getOutputStream());
        loadCounter = new ArrayList<>();
    }

    public void send(String message){
        out.println(message);
        out.flush();
        sendLog(message);
    }

    private void sendLog(String message){
        ControllerLogger.getInstance().messageSent(socket, message);
    }

    private void recieveLog(String message){
        ControllerLogger.getInstance().messageReceived(socket, message);
    }

    public void singleOperation(String message) throws InterruptedException {
        ControllerLogger.getInstance().messageReceived(socket, message);
        if(controller.dstoreSessions.size() < controller.R){
            send(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }
        String[] messageSplit = message.split(" ");
        switch (messageSplit[0]) {
            case "STORE" -> {
                if (controller.rebalance.get())
                    controller.clientQueue.add(new Controller.QueuedOperation(message, socket, this));
                else
                    storeMessage(messageSplit);
            }
            case "LOAD" -> loadMessage(messageSplit);
            case "RELOAD" -> reloadMessage(messageSplit);
            case "REMOVE" -> {
                if (controller.rebalance.get())
                    controller.clientQueue.add(new Controller.QueuedOperation(message, socket, this));
                else
                    removeMessage(messageSplit);
            }
            case "LIST" -> listMessage(messageSplit);
            default -> recieveLog("Malformed message: " + String.join(" ", messageSplit));
        }
    }

//    @Override
//    public void cleanup() throws IOException {}

    private void storeMessage(String[] messageSplit) throws InterruptedException {
        if (messageSplit.length != 3 || !Common.isNumeric(messageSplit[2]))
            System.out.println("Malformed STORE message: " + String.join(" ", messageSplit));
        else controller.controllerStoreOperation(messageSplit[1], Integer.parseInt(messageSplit[2]), this);
    }

    private void loadMessage(String[] messageSplit) {
        if (messageSplit.length != 2)
            System.out.println("Malformed LOAD message: " + String.join(" ", messageSplit));
        else controller.controllerLoadOperation(messageSplit[1], this);
    }

    private void reloadMessage(String[] messageSplit) {
        if (messageSplit.length != 2)
            System.out.println("Malformed RELOAD message: " + String.join(" ", messageSplit));
        else controller.controllerReloadOperation(messageSplit[1],this);
    }

    private void removeMessage(String[] messageSplit) throws InterruptedException {
        if (messageSplit.length != 2)
            System.out.println("Malformed REMOVE message: " + String.join(" ", messageSplit));
        else controller.controllerRemoveOperation(messageSplit[1], this);
    }

    private void listMessage(String[] messageSplit) {
        if (messageSplit.length != 1)
            System.out.println("Malformed LIST message: " + String.join(" ", messageSplit));
        else controller.controllerListOperation(this);
    }




}
