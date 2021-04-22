import java.io.IOException;
import java.net.Socket;

/**
 * Controller -> Dstore connection
 */
public class ControllerDstoreSession extends Session {

    private int dstorePort;
//    private BufferedReader in;
//    private PrintWriter out;
    private Socket connection;
    private Controller controller;

    public ControllerDstoreSession(int dstorePort, Socket connection, Controller controller, String message) throws IOException {
        super(connection,message,"Dstore");
        this.dstorePort = dstorePort;
        this.connection = connection;
        this.controller = controller;
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
                System.out.println("STORE_ACK" + "received");
                controller.addDstoreAck(filename);
                break;
            default:
                System.out.println("NOT MATCHED " + messageSplit[0]);
        }
    }

    @Override
    public void cleanup() {
        controller.dstoreClosedNotify(dstorePort);
        super.cleanup();
    }

}
