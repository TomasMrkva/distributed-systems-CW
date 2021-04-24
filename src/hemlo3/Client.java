import java.util.*;
import java.io.*;
import java.net.*;

public class Client extends TCPClient {

    private Integer timeout;

    public Client(Integer cport, Integer timeout) throws Exception {
        super("127.0.0.1", cport);
        this.timeout = timeout;
    }

    private void store(String msg) {
        dispatch(msg);
        String where = await(this.timeout);
        System.out.println(" ::" + where);
        if (where != null) {
            String[] ps = where.split(" ");
            if (ps.length > 0 && ps[0].equals("STORE_TO")) {
                for (int i = 1; i < ps.length; i++) {
                    try {
                        Integer port = Integer.parseInt(ps[i]);
                        new Thread(new ClientDstoreOperation(port, this.timeout, ClientDstoreOperation.Operation.STORE, msg)).start();
                    } catch (Exception e) {
                        // connection failed ?
                        e.printStackTrace();
                    }
                }
                String complete = await(this.timeout);
                if (complete != null && complete.equals("STORE_COMPLETE")) {

                } else {
                    // log idk
                }
            } else {
                // log
                System.out.println("(!) STORE_TO malformed");
            }
        } else {
            // log
            System.out.println("(!) No ACK or malformed ACK received");
        }
    }

    public void exec(String msg) {
        String[] ws = msg.split(" ");
        if (ws.length > 0) {
            switch (ws[0]) {
                case "STORE":
                    store(msg);
                    break;
            }
        } else {
            // log ?
            System.out.println("(!) Unrecognized command");
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Error: invalid amount of command line arguments provided");
            return;
        }

        Integer cport;
        Integer timeout;
        try {
            cport = Integer.parseInt(args[0]);
            timeout = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Error: unable to parse command line arguments");
            return;
        }

        Client client;
        try {
            client = new Client(cport, timeout);
        } catch (Exception e) {
            System.out.println("Error: connection failed");
            return;
        }

        Scanner input = new Scanner(System.in);
        String line;
        while (!(line = input.nextLine()).equals("QUIT")) {
            client.exec(line);
        }
    }
}
