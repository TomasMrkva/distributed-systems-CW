import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MyFile {

    private final String filename;
    private final int filesize;
    private Index.Operation operation;
    private final List<ControllerDstoreSession> dstores;

    public MyFile(String filename, int filesize) {
        this.filename = filename;
        this.filesize = filesize;
        dstores = Collections.synchronizedList(new ArrayList<>());
        operation = null;
    }

    public String getName(){
        return filename;
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

    public synchronized boolean canStore(){
        return operation == null || operation == Index.Operation.REMOVE_COMPLETE;
    }

    public synchronized boolean inProgress(){
        return operation == Index.Operation.STORE_IN_PROGRESS || operation == Index.Operation.REMOVE_IN_PROGRESS;
    }

    public boolean addDstore(ControllerDstoreSession controllerDstoreSession) {
        return dstores.add(controllerDstoreSession);
    }

    public boolean removeDstore(ControllerDstoreSession controllerDstoreSession) {
        return dstores.remove(controllerDstoreSession);
    }

    public List<ControllerDstoreSession> getDstores(){
        synchronized (dstores){
            return dstores;
        }
    }

    public void setDstores(List<ControllerDstoreSession> newDstores){
        synchronized (dstores){
            dstores.clear();
            dstores.addAll(newDstores);
        }
    }

    public int getFilesize() {
        return filesize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyFile myFile = (MyFile) o;
        return filename.equals(myFile.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }

    @Override
    public String toString() {
        return "MyFile{" +
                "filename='" + filename + '\'' +
                ", filesize=" + filesize +
                ", operation=" + operation +
                ", dstores=" + dstores +
                '}';
    }
}