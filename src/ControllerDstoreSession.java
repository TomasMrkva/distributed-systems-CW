import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

/**
 * Controller -> Dstore connection
 */
public class ControllerDstoreSession extends Session {

    private int dstorePort;
//    private BufferedReader in;
//    private PrintWriter out;
    private Socket receivingSocket;
    private Socket sendingSocket;
    private Controller controller;
    public int numberOfFiles;

    public ControllerDstoreSession(int dstorePort, Socket receivingSocket, Controller controller, String message) throws IOException {
        super(receivingSocket,message,"Dstore");
        this.dstorePort = dstorePort;
        this.receivingSocket = receivingSocket;
        this.sendingSocket = new Socket("localhost",dstorePort);
        this.controller = controller;
        this.numberOfFiles = 0;
//        in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//        out = new PrintWriter(connection.getOutputStream());
    }

    public int getDstorePort(){
        return dstorePort;
    }

    @Override
    public void singleOperation(String message) throws InterruptedException {
        String[] messageSplit = message.split(" ");
        switch (messageSplit[0]){
            case "STORE_ACK":
                String filename = messageSplit[1];
                System.out.println("STORE_ACK " + "received from port: " + dstorePort);
                controller.addDstoreAck(filename, this);
                break;
            default:
                fileList(messageSplit);
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
