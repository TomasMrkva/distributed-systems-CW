import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Controller {

    final int R;
    final int CPORT;
    final int TIMEOUT;
    final int REBALANCE_PERIOD;
    final ServerSocket socket;

    public Controller(int r, int cport, int timeout, int rebalance_period) throws Exception {
        R = r;
        CPORT = cport;
        TIMEOUT = timeout;
        REBALANCE_PERIOD = rebalance_period;
        socket = new ServerSocket(cport);
        run();
    }

    private void run() {
        for (;;) {
            try {
                final Socket client = socket.accept();
                new Thread(() -> {
                    try {
                        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        String line;
                        while ((line = in.readLine()) != null)
                            System.out.println(line + " received");
                        client.close();
                    } catch (Exception e) {}
                }).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //java Controller cport R timeout rebalance_period
    public static void main(String[] args) {
        int cport, R, timeout, rebalance_period;
        if(args.length < 4) {
            System.out.println("Not enough parameters provided");
            return;
        }
        try { cport = Integer.parseInt(args[0]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of cport!"); return;}
        try { R = Integer.parseInt(args[1]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of R!"); return;}
        try { timeout = Integer.parseInt(args[2]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of timeout!"); return;}
        try { rebalance_period = Integer.parseInt(args[3]); }
        catch (NumberFormatException e) { System.out.println("Wrong value of rebalance_period!"); return;}

        try { Controller controller = new Controller(cport, R, timeout, rebalance_period); }
        catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
