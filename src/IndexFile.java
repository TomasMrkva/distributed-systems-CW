import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IndexFile {

    private final String filename;
    private final int filesize;
    private Index.Operation operation;
    private final List<ControllerDstoreSession> dstores;

    public IndexFile(String filename, int filesize) {
        this.filename = filename;
        this.filesize = filesize;
        dstores = Collections.synchronizedList(new ArrayList<>());
        operation = null;
    }

    public String getName(){
        return filename;
    }

    public int getSize() {
        return filesize;
    }

    public synchronized void setOperation(Index.Operation op) {
        operation = op;
    }

    public synchronized Index.Operation getOperation() {
        return operation;
    }

    public synchronized boolean exists(){
        return operation == Index.Operation.STORE_COMPLETE;
    }

    public synchronized boolean canStore() {
        return operation == null || operation == Index.Operation.REMOVE_COMPLETE;
    }

    public synchronized boolean inProgress(){
        return operation == Index.Operation.STORE_IN_PROGRESS || operation == Index.Operation.REMOVE_IN_PROGRESS;
    }

    public synchronized boolean addDstore(ControllerDstoreSession controllerDstoreSession) {
        return dstores.add(controllerDstoreSession);
    }

    public synchronized void setDstores(List<ControllerDstoreSession> newDstores){
        dstores.clear();
        dstores.addAll(newDstores);
    }

    public synchronized List<ControllerDstoreSession> getDstores(){
        return dstores;
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        IndexFile myFile = (IndexFile) o;
//        return filename.equals(myFile.getName());
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(filename);
//    }

    @Override
    public String toString() {
        return "IndexFile{" +
                "filename='" + filename + '\'' +
                ", filesize=" + filesize +
                ", operation=" + operation +
                ", dstores=" + dstores +
                '}';
    }
}