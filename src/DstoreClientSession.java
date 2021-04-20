import java.io.*;
import java.net.Socket;

/**
 * Dstore <-> Client connection
 */
public class DstoreClientSession {

    private Dstore dstore;
    private Socket socket;

    public DstoreClientSession(Dstore dstore, Socket socket, String[] lineSplit) throws Exception {
        this.dstore = dstore;
        this.socket = socket;
        run(lineSplit);
    }

    private void run(String[] linesplit) throws Exception{
        switch (linesplit[0]){
            case "SAVE":
                store(linesplit);
                break;
            default:
        }
    }

    private void store(String[] linesplit) throws IOException {
        String line;
        String filename = dstore.FILE_FOLDER + "/" + linesplit[1];
        int filesize = Integer.parseInt(linesplit[2]);

        PrintWriter out = new PrintWriter(socket.getOutputStream());
        InputStream fileIn = socket.getInputStream();
        OutputStream fileOut = new FileOutputStream(new File(filename));

        out.println("ACK");
        byte[] bytes = new byte[8*1024]; // could be filesize
        int count;
        while ((count = fileIn.read(bytes)) > 0) {
            fileOut.write(bytes, 0, count);
        }
        fileOut.close();
    }

}
