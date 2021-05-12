import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
//        System.out.println(operation);
        return operation == null || operation == Index.Operation.REMOVE_COMPLETE;
    }

    public synchronized boolean inProgress(){
        return operation == Index.Operation.STORE_IN_PROGRESS || operation == Index.Operation.REMOVE_IN_PROGRESS;
    }

    public synchronized boolean addDstore(ControllerDstoreSession controllerDstoreSession) {
//        for (ControllerDstoreSession dstore : dstores) {
//            if (dstore.getDstorePort() == controllerDstoreSession.getDstorePort()) {
//                return true;
//            }
//        }
        return dstores.add(controllerDstoreSession);
    }

    public synchronized boolean addDstores(List<ControllerDstoreSession> controllerDstoreSessions) {
        dstores.clear();
        dstores.addAll(controllerDstoreSessions);
        return true;
    }

    public synchronized boolean removeDstore(ControllerDstoreSession controllerDstoreSession) {
        return dstores.remove(controllerDstoreSession);
    }

    public List<ControllerDstoreSession> getDstores(){
//        List<ControllerDstoreSession> returnList = new ArrayList<>();
//        synchronized (dstores){
//            for (ControllerDstoreSession cd : dstores) {
//                boolean contains = returnList.stream().anyMatch(d -> d.getDstorePort() == cd.getDstorePort());
//                if (!contains) returnList.add(cd);
//            }
//            return returnList;
//        }
        synchronized (dstores){
            return dstores;
        }
    }

    public synchronized void setDstores(List<ControllerDstoreSession> newDstores){
        synchronized (dstores){
            dstores.clear();
            dstores.addAll(newDstores);
        }
    }

    public int getSize() {
        return filesize;
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        MyFile myFile = (MyFile) o;
//        return filename.equals(myFile.getName());
//    }

//    @Override
//    public int hashCode() {
//        return Objects.hash(filename);
//    }

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