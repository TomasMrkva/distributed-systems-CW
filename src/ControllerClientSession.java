import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Controller -> Dstore connection
 */
public class ControllerClientSession implements Runnable {

    private BufferedReader in;
    private PrintWriter out;
    private Socket connection;
    private Controller controller;
    private String ERROR_MESSAGE = "ERROR_NOT_ENOUGH_DSTORES";

    public ControllerClientSession(Socket connection, Controller controller) throws IOException {
        this.connection = connection;
        this.controller = controller;
        in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        out = new PrintWriter(connection.getOutputStream());
    }

    @Override
    public void run(){
        String line;
        try{
            System.out.println("Controller -> Client connection established");
            while((line = in.readLine()) != null) {
                String[] lineSplit = line.split(" ");
                if(lineSplit[0].equals("STORE")){
                    String output = storeOperation(lineSplit);
                    if(!output.equals(ERROR_MESSAGE)) {
                        //TODO: notify controller to wait for dstore acks
                    }
                    out.println(output);
                    out.flush();
                }
                else {
                    System.out.println("NOT MATCHED");
                }
            }
        } catch (Exception e){}
        finally { try { connection.close(); }
            catch (IOException e) { e.printStackTrace(); }
        }
    }

    private String storeOperation(String[] lineSplit){
        String filename = lineSplit[1];
        int filesize = Integer.parseInt(lineSplit[2]);
        if(controller.dstoreSessions.size() < controller.R){
            return ERROR_MESSAGE;
        } else {
            String output = "STORE_TO";
            for (int i = 0; i < controller.R; i++){
                output = output + " " + controller.dstoreSessions.get(i);
            }
            return output;
        }
    }
}
