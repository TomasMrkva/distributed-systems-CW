import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * Controller -> Dstore connection
 */
public class ControllerDstoreSession extends Session {

    private int dstorePort;
//    private BufferedReader in;
    private PrintWriter out;
//    private Socket receivingSocket;
//    private Socket sendingSocket;
    private Controller controller;
    private Socket dstoreSocket;
    public int numberOfFiles;

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
            default:
                System.out.println("Unrecognized command in controllerDstoreSession: " +  message);
//                fileList(messageSplit);
        }
    }

    private void fileList(String[] fileList) {
        System.out.println("File list for port: " + dstorePort + " is: " + Arrays.toString(fileList));
        numberOfFiles = fileList.length;
    }

    @Override
    public void cleanup() throws IOException {
        System.out.println("SOMETHING WRONG HAPPENED");
        File file = new File("errors.log");
        Writer fileWriter = new FileWriter(file,true);
        fileWriter.write("1");
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
