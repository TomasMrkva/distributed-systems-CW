import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

public class ControllerDstoreSession extends Session {

    private final int dstorePort;
    private final PrintWriter out;
    private final Controller controller;
    private final Socket dstoreSocket;

    public ControllerDstoreSession(int dstorePort, Socket dstoreSocket, Controller controller, String message) throws IOException {
        super(dstoreSocket,message,"Dstore");
        this.dstorePort = dstorePort;
        this.dstoreSocket = dstoreSocket;
        this.controller = controller;
        out = new PrintWriter(dstoreSocket.getOutputStream());
    }

    public int getDstorePort(){
        return dstorePort;
    }

    public void sendMessageToDstore(String message){
        ControllerLogger.getInstance().messageSent(dstoreSocket, message);
        out.println(message); out.flush();
    }

    @Override
    public void singleOperation(String message) {
        String[] messageSplit = message.split(" ");
        ControllerLogger.getInstance().messageReceived(dstoreSocket, message);
        String filename;
        switch (messageSplit[0]){
            case "JOIN":
                if (messageSplit.length != 2) {
                    System.out.println("Malformed JOIN message: " + message);
                    break;
                }
                System.out.println("Dstores: " + controller.dstoreSessions.size());
                new Thread( () -> controller.joinOperation(dstorePort, this)).start();
                break;
            case "STORE_ACK":
                if (messageSplit.length != 2) {
                    System.out.println("Malformed STORE_ACK message: " + message);
                    break;
                }
                filename = messageSplit[1];
                controller.addStoreAck(filename, this);
                break;
            case "REMOVE_ACK":
            case "ERROR_FILE_DOES_NOT_EXIST":
                if (messageSplit.length != 2) {
                    System.out.println("Malformed REMOVE_ACK/ERROR_FILE_DOES_NOT_EXIST message: " + message);
                    break;
                }
                filename = messageSplit[1];
                controller.addRemoveAck(filename, this);
                break;
            case "LIST":
                controller.rebalanceFiles.put(dstorePort, new ArrayList<>(Arrays.asList(messageSplit).subList(1, messageSplit.length)));
                controller.rebalanceLatch.countDown();
                break;
            case "REBALANCE_COMPLETE":
                if (messageSplit.length != 1) {
                    System.out.println("Malformed REMOVE_ACK/ERROR_FILE_DOES_NOT_EXIST message: " + message);
                    break;
                }
                controller.rebalanceCompleteLatch.countDown();
                break;
            default:
                System.out.println("Malformed message: " + message);
        }
    }

    public void closeSession() throws IOException {
        dstoreSocket.close();
    }

    @Override
    public void cleanup() {
        controller.dstoreClosedNotify(dstorePort);
    }

}
