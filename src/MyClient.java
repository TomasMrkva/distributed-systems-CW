//import java.io.*;
//import java.net.Socket;
//
//public class MyClient {
//
//    final int CPORT;
//    final int TIMEOUT;
//    final Socket socket;
//    final BufferedReader in;
//    final PrintWriter out;
//
//    public MyClient(int cport, int timeout) throws Exception {
//        socket = new Socket("localhost", cport);
//        CPORT = cport;
//        TIMEOUT = timeout;
//        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//        out = new PrintWriter(socket.getOutputStream());
//    }
//
//
//    private void run() throws IOException {
//        while(true){
//            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//            String line;
//            while((line = reader.readLine()) != null){
//                String[] lineSplit = line.split(" ");
//                switch(lineSplit[0]){
//                    case "STORE":
//
//                }
//            }
//        }
//    }
//
//    private void storeOperation() {
//
//    }
//
//    public static void main(String[] args) {
//        int cport, timeout;
//        try { cport = Integer.parseInt(args[0]); }
//        catch (NumberFormatException e) { System.out.println("Wrong value of cport!"); return;}
//        try { timeout = Integer.parseInt(args[1]); }
//        catch (NumberFormatException e) { System.out.println("Wrong value of timeout!"); return;}
////
////        try {
////            MyClient client = new MyClient(cport, timeout);
////        } catch (Exception e) {
////            e.printStackTrace();
////            return;
////        }
//        Client client = new Client(cport, timeout,Logger.LoggingType.ON_FILE_AND_TERMINAL);
//
//    }
//
//}
