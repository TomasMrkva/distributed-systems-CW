import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {

    final int PORT;
    final int CPORT;
    final int TIMEOUT;
    final String FILE_FOLDER;
    final ServerSocket socket;

    public Dstore(int port, int cport, int timeout, String file_folder) throws Exception {
        PORT = port;
        CPORT = cport;
        TIMEOUT = timeout;
        FILE_FOLDER = file_folder;
        socket = new ServerSocket(CPORT);
        for(;;){
            try{Socket controllerSocket = socket.accept(); }
            catch(Exception e){System.out.println("error "+e);}
        }
    }

    //java Controller cport R timeout rebalance_period
    public static void main(String[] args) {
        int port, cport, timeout;
        String file_folder = args[3];
        try { port = Integer.parseInt(args[0]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of cport!"); return;}
        try { cport = Integer.parseInt(args[1]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of R!"); return;}
        try { timeout = Integer.parseInt(args[2]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of timeout!"); return;}

        try {
            Dstore dstore = new Dstore(port, cport, timeout, file_folder);

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
