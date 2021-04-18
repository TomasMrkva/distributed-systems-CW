import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {

    final int CPORT;
    final int TIMEOUT;
    final Socket socket;

    public Client(int cport, int timeout) throws Exception {
        CPORT = cport;
        TIMEOUT = timeout;
        socket = new Socket("localhost", CPORT);
        run();
    }

    private void run() throws Exception {
        PrintWriter out = new PrintWriter(socket.getOutputStream());
        for (;;){
            String input = System.console().readLine();
            out.println("Message: " + input); out.flush();
        }
    }

    public static void main(String[] args) {
        int cport, timeout;
        try { cport = Integer.parseInt(args[0]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of cport!"); return;}
        try { timeout = Integer.parseInt(args[1]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of timeout!"); return;}

        try {
            Client client = new Client(cport, timeout);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
