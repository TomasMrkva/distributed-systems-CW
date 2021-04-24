//import java.io.*;
//import java.net.Socket;
//
//public class MyClient {
//
//    final int CPORT;
//    final int TIMEOUT;
//    final Socket socket;
//
//    public MyClient(int cport, int timeout) throws Exception {
//        CPORT = cport;
//        TIMEOUT = timeout;
//        System.out.println("HEMLO");
//        socket = new Socket("localhost", CPORT);
//        run();
//    }
//
//    private void run() throws Exception {
//        PrintWriter out = new PrintWriter(socket.getOutputStream());
//        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//
//        for (;;){
//            String input = System.console().readLine();
//            out.println(input); out.flush();
//            System.out.println("input" + input + " flushed");
//            String line = in.readLine();
//            System.out.println(line);
////            String[] lineArray = input.split(" ");
////            if(lineArray[0].equals("STORE")){
////                out.println(input); out.flush();
////                String line;
////                while(!(line = in.readLine()).equals("cancel")) {
////                    String[] responseLine = line.split(" ");
////                    if(responseLine[0].equals("STORE_TO")){
////                        //System.out.println(line);
////                        int port = Integer.parseInt(responseLine[1]);
////                        Socket socket = new Socket("localhost", port);
////                        PrintWriter p = new PrintWriter(socket.getOutputStream());
////                        String input2 = System.console().readLine();
////                        p.println(input2); p.flush();
////                    }
////                }
////            }
//
//        }
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
//        try { client.connect(); } catch(IOException e) { e.printStackTrace(); return; }
//        try { client.store(new File("Clipboard01.pdf")); } catch(IOException e) { e.printStackTrace(); }
//
//        try { client.store(new File("Clipboard01.pdf")); } catch(IOException e) { e.printStackTrace(); }
//
//        try { client.store(new File("Clipboard01.jpg")); } catch(IOException e) { e.printStackTrace(); }
//
//
//    }
//}
