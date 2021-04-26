import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
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
    public int numberOfFiles;

    public ControllerDstoreSession(int dstorePort, Socket dstoreSocket, Controller controller, String message) throws IOException {
        super(dstoreSocket,message,"Dstore");
        this.dstorePort = dstorePort;
//        this.receivingSocket = dstoreSocket;
//        this.sendingSocket = new Socket("localhost",dstorePort);
        this.controller = controller;
        out = new PrintWriter(dstoreSocket.getOutputStream());
        this.numberOfFiles = 0;
//        in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//        out = new PrintWriter(connection.getOutputStream());
    }

    public int getDstorePort(){
        return dstorePort;
    }

    public void sendMessageToDstore(String message){
        out.println(message); out.flush();
    }

    @Override
    public void singleOperation(String message) throws InterruptedException {
        String[] messageSplit = message.split(" ");
        String filename;
        switch (messageSplit[0]){
            case "STORE_ACK":
                filename = messageSplit[1];
                System.out.println("STORE_ACK " + "received from port: " + dstorePort);
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
    public void cleanup() {
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
