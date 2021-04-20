import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Controller -> Dstore connection
 */
public class ControllerDstoreSession implements Runnable {

    private int dstorePort;
    private BufferedReader in;
    private PrintWriter out;
    private Socket connection;
    private Controller controller;

    public ControllerDstoreSession(int dstorePort, Socket connection, Controller controller) throws IOException {
        this.dstorePort = dstorePort;
        this.connection = connection;
        this.controller = controller;
        in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        out = new PrintWriter(connection.getOutputStream());
    }

    public int getDstorePort(){
        return dstorePort;
    }

    @Override
    public void run(){
        String line;
        try{
            System.out.println("Controller -> Dstore connection established for Dstore: " + dstorePort );
            while((line = in.readLine()) != null) {
                String[] lineSplit = line.split(" ");
                if(lineSplit[0].equals("STORE_ACK")){
                    System.out.println("STORE_ACK" + "received");
                }
                else {
                    System.out.println("NOT MATCHED");
                }
            }
        } catch (Exception e){}
        finally {
            try {
                controller.dstoreClosedNotify(dstorePort);
                connection.close();
            }
            catch (IOException e) { e.printStackTrace(); }
        }

    }
}
