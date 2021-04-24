import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Index {

    public enum Operation {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    private final ConcurrentHashMap<String, Operation> operation;
    private final ConcurrentHashMap<String, List<ControllerDstoreSession>> files;
//    private final ConcurrentSkipListMap<Integer, AtomicInteger> dstoresCapacity;

    public Index() {
        operation = new ConcurrentHashMap<>();
        files = new ConcurrentHashMap<>();
//        dstoresCapacity = new ConcurrentSkipListMap<>(); //TODO: think about how to store the file numbers
    }

//    public synchronized Operation getOperation(String filename) {
//        return operation.get(filename);
//    }

    public synchronized boolean setStoreInProgress(String filename){
        Operation op = operation.get(filename);
        if(op == Operation.STORE_IN_PROGRESS || op == Operation.STORE_COMPLETE)
            return false;
        operation.put(filename,Operation.STORE_IN_PROGRESS);
        files.put(filename, Collections.synchronizedList(new ArrayList<>()));
        return true;
    }

    public synchronized void setStoreComplete(String filename){
        operation.put(filename,Operation.STORE_COMPLETE);
    }

//    public List<ControllerDstoreSession> getDstores(String filename) {
//        return files.get(filename);
//    }

//    public void createFile(String filename) {
//        files.put(filename, Collections.synchronizedList(new ArrayList<>()));
//    }

    public void addDstore(String filename, ControllerDstoreSession dstore) {
        files.compute(filename, (key, value) -> {
            value.add(dstore);
            return value;
        });
    }

//    public void incrementValue(Integer port){
//        dstoresCapacity.
//    }

    /**
     * Removes a dstore with the specified parameter form the list of dstores in the file.
     * This method is called when a dstore becomes unavailable
     * @param dstorePort the port of dstore to remove
     */
    public void removeDstore(int dstorePort) {
        files.values().forEach(controllerDstoreSessions -> {
            controllerDstoreSessions.removeIf(dstore -> dstore.getDstorePort() == dstorePort);
        });
        files.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        System.out.println(files.size());
    }

    /**
     * Removes the specified filename,dstore entry from the hashmap.
     * This method is called when a user wanted to remove
     * @param filename
     * @param dstore
     */
    public void removeDstore(String filename, ControllerDstoreSession dstore) {
        files.compute(filename, (key,value) -> {
            value.removeIf( ds -> (ds.getDstorePort() == dstore.getDstorePort()));
            System.out.println(value.size());
            return (value.isEmpty()) ? null : value; //optional, removes the file from index if no dstores store it
        });
    }

}