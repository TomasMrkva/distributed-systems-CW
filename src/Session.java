import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public abstract class Session implements Runnable {

    final Socket connection;
    final String START_MESSAGE;
    final BufferedReader in;
    final String name;

    public Session(Socket connection, String startMessage, String name) throws IOException {
        this.connection = connection;
        START_MESSAGE = startMessage;
        this.name = name;
        in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    }

    public abstract void singleOperation(String message) throws InterruptedException;

    public void cleanup() throws IOException {}

    private void loop() {
        try {
            String line;
            while((line = in.readLine()) != null){
                singleOperation(line);
            }
        } catch (IOException | InterruptedException e){
//            System.out.println("(X) SESSION: SOMETHING WRONG HAPPENED: " + e.getMessage().toUpperCase());
//            e.printStackTrace();
        } finally {
            try {
                cleanup();
//                System.out.println("(i) SESSION: CLOSING CONNECTION: " + connection.getPort());
                connection.close();
            }
            catch (IOException e) { e.printStackTrace(); }
        }
    }

    @Override
    public void run(){
//        System.out.println("Controller -> " + name + " connection established");
        try {
            singleOperation(START_MESSAGE);
            loop();
        } catch (InterruptedException e) {
//            e.printStackTrace();
        }
    }

}