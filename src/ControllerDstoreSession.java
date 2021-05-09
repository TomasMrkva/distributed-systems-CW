import java.io.*;
import java.net.Socket;
import java.util.Arrays;

/**
 * Controller -> Dstore connection
 */
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
                controller.dstoreSessions.put(dstorePort, this);
                System.out.println("Dstores: " + controller.dstoreSessions.size());
                break;
            case "STORE_ACK":
                filename = messageSplit[1];
//                System.out.println("STORE_ACK " + "received from port: " + dstorePort);
                controller.addStoreAck(filename, this);
                break;
            case "REMOVE_ACK":
                filename = messageSplit[1];
                controller.addRemoveAck(filename, this);
                break;
            case "ERROR_FILE_DOES_NOT_EXIST":
                System.out.println(message);
                break;
            case "LIST":
                controller.rebalanceFiles.put(dstorePort, Arrays.asList(messageSplit).subList(1, messageSplit.length));
                controller.rebalanceLatch.countDown();
                System.out.println("RECEIVED LIST : " + Arrays.asList(messageSplit).subList(1, messageSplit.length) + controller.rebalanceLatch.getCount());
                break;
            default:
                System.out.println("Unrecognized command in controllerDstoreSession: " +  message);
//                fileList(messageSplit);
        }
    }

    private void fileList(String[] fileList) {
        System.out.println("File list for port: " + dstorePort + " is: " + Arrays.toString(fileList));
        numberOfFiles = fileList.length;
    }

    public void closeSession() throws IOException {
        dstoreSocket.close();
    }

    @Override
    public void cleanup() throws IOException {
        System.out.println("SOMETHING WRONG HAPPENED");
        File file = new File("errors.log");
        Writer fileWriter = new FileWriter(file,true);
        fileWriter.write("1\n");
        fileWriter.close();
        controller.dstoreClosedNotify(dstorePort);
        super.cleanup();
    }

    @Override
    public String toString() {
        return "ControllerDstoreSession{" +
                "dstorePort=" + dstorePort +
                '}';
    }
}
