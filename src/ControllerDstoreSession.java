import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

public class ControllerDstoreSession extends Session {

    private final int dstorePort;
    private final PrintWriter out;
    private final Controller controller;
    private final Socket dstoreSocket;
    private int numberOfFiles;

    public ControllerDstoreSession(int dstorePort, Socket dstoreSocket, Controller controller, String message) throws IOException {
        super(dstoreSocket,message,"Dstore");
        this.dstorePort = dstorePort;
        this.dstoreSocket = dstoreSocket;
        this.controller = controller;
        out = new PrintWriter(dstoreSocket.getOutputStream());
        this.numberOfFiles = 0;
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
                System.out.println("Dstores: " + controller.dstoreSessions.size());
                new Thread( () -> controller.joinOperation(dstorePort, this)).start();
                break;
            case "STORE_ACK":
                filename = messageSplit[1];
//                System.out.println("STORE_ACK " + "received from port: " + dstorePort);
//                controller.index.addDstore(filename, this);   //TODO: might need to revisit this
                controller.addStoreAck(filename, this);
                break;
            case "REMOVE_ACK":
            case "ERROR_FILE_DOES_NOT_EXIST":
                filename = messageSplit[1];
                controller.addRemoveAck(filename, this);
                break;
            case "LIST":
                controller.rebalanceFiles.put(dstorePort, new ArrayList<>(Arrays.asList(messageSplit).subList(1, messageSplit.length)));
                controller.rebalanceLatch.countDown();
                System.out.println("RECEIVED LIST : " + Arrays.asList(messageSplit).subList(1, messageSplit.length) + controller.rebalanceLatch.getCount());
                break;
            case "REBALANCE_COMPLETE":
                controller.rebalanceCompleteLatch.countDown();
                break;
            default:
                System.out.println("Unrecognized command in controllerDstoreSession: " +  message);
//                fileList(messageSplit);
        }
    }

//    private void fileList(String[] fileList) {
//        System.out.println("File list for port: " + dstorePort + " is: " + Arrays.toString(fileList));
//        numberOfFiles = fileList.length;
//    }

    public void closeSession() throws IOException {
        dstoreSocket.close();
    }

    @Override
    public void cleanup() {
//        System.out.println("CLOSING DSTORE" + dstorePort);
        controller.dstoreClosedNotify(dstorePort);
    }

    @Override
    public String toString() {
        return "ControllerDstoreSession{" +
                "dstorePort=" + dstorePort +
                '}';
    }
}
