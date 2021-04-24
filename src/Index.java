import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Index {

    public enum Operation {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

//    private final ConcurrentHashMap<String, Operation> operation;
//    private final ConcurrentHashMap<String, List<ControllerDstoreSession>> files;
//    private final ConcurrentHashMap<String, Integer> fileSizes;
    private final List<MyFile> files;

//    private final ConcurrentSkipListMap<Integer, AtomicInteger> dstoresCapacity;

    public Index(int R) {
//        operation = new ConcurrentHashMap<>();
//        files = new ConcurrentHashMap<>();
//        fileSizes = new ConcurrentHashMap<>();
        files = Collections.synchronizedList(new ArrayList<>());
//        dstoresCapacity = new ConcurrentSkipListMap<>(); //TODO: think about how to store the file numbers
    }

//    public synchronized Operation getOperation(String filename) {
//        return operation.get(filename);
//    }

//    public synchronized boolean setStoreInProgresss(String filename){
//        Operation op = operation.get(filename);
//        if(op == Operation.STORE_IN_PROGRESS || op == Operation.STORE_COMPLETE)
//            return false;
//        operation.put(filename,Operation.STORE_IN_PROGRESS);
//        files.put(filename, Collections.synchronizedList(new ArrayList<>()));
////        fileSizes.put(filename, filesize);
//        return true;
//    }

    public synchronized boolean setStoreInProgress(String filename, int filesize){
        for(MyFile f : files) {
            if(f.getName().equals(filename)){
                Operation op = f.getOperaion();
                if(op == Operation.STORE_IN_PROGRESS || op == Operation.STORE_COMPLETE)
                    return false;
            }
        }
        MyFile f = new MyFile(filename,filesize);
        f.setOperation(Operation.STORE_IN_PROGRESS);
        files.add(f);
        return true;
    }

    public synchronized List<Integer> getNDstores(int n){
        List<Integer> list = new ArrayList<>();
        files.forEach(myFile -> myFile.getDstores().forEach(dstore -> list.add(dstore.getDstorePort())));
        System.out.println("Unordered list with duplicates");
        System.out.println(Arrays.toString(files.toArray()));
        System.out.println();
        Map<Integer, Long> counts = list.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        System.out.println("Unordered map without duplicates");
        counts.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });
        counts = counts.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));
        System.out.println();
        System.out.println("Ordered map without duplicates");
        counts.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });
        List<Integer> result = new ArrayList<>(counts.keySet());
        System.out.println();
        System.out.println(Arrays.toString(result.toArray()));
        return result.stream().limit(n).collect(Collectors.toList());
    }

    public static void main(String[] args) throws IOException {
        ControllerDstoreSession c1 = new ControllerDstoreSession(1,null,null,null);
        ControllerDstoreSession c2 = new ControllerDstoreSession(2,null,null,null);
        ControllerDstoreSession c3 = new ControllerDstoreSession(3,null,null,null);
        ControllerDstoreSession c4 = new ControllerDstoreSession(4,null,null,null);
        ControllerDstoreSession c5 = new ControllerDstoreSession(5,null,null,null);
        ControllerDstoreSession c6 = new ControllerDstoreSession(6,null,null,null);
        MyFile m1 = new MyFile("m1",1);
        MyFile m2 = new MyFile("m2",1);
        MyFile m3 = new MyFile("m3",1);
        MyFile m4 = new MyFile("m4",1);
        MyFile m5 = new MyFile("m5",1);
        MyFile m6 = new MyFile("m6",1);
        m1.addDstore(c1); m1.addDstore(c2); m1.addDstore(c3); m1.addDstore(c4);
        m2.addDstore(c1); m1.addDstore(c3);
        m3.addDstore(c4); m3.addDstore(c4);
        m4.addDstore(c5); m4.addDstore(c1);
        m5.addDstore(c2); m5.addDstore(c5); m5.addDstore(c1);
        Index index = new Index(4);
        MyFile[] files = {m1,m2,m3,m4,m5,m6};
        index.files.addAll(Arrays.asList(files));
        System.out.println("Result:");
        System.out.println(Arrays.toString(index.getNDstores(3).toArray()));

    }

//    public synchronized void setStoreComplete(String filename){
//        operation.put(filename,Operation.STORE_COMPLETE);
//    }

    public synchronized boolean setStoreComplete(String filename){
        for(MyFile f : files) {
            if(f.getName().equals(filename)){
                f.setOperation(Operation.STORE_COMPLETE);
                return true;
            }
        }
        return false;
    }

    public synchronized Integer getFileSize(String filename){
        for(MyFile f : files) {
            if(f.getName().equals(filename)){
                return f.getFilesize();
            }
        }
        return null;
    }

    public synchronized List<ControllerDstoreSession> getDstores(String filename){
        for(MyFile f : files) {
            if(f.getName().equals(filename)){
                return f.getDstores();
            }
        }
        return null;
    }
//    public List<ControllerDstoreSession> getDstores(String filename) {
//        return files.get(filename);
//    }

//    public void createFile(String filename) {
//        files.put(filename, Collections.synchronizedList(new ArrayList<>()));
//    }

//    public void addDstore(String filename, ControllerDstoreSession dstore) {
//        files.compute(filename, (key, value) -> {
//            value.add(dstore);
//            return value;
//        });
//    }

    public synchronized boolean addDstore(String filename, ControllerDstoreSession dstore) {
        for(MyFile f : files) {
            if(f.getName().equals(filename)){
                f.addDstore(dstore);
                return true;
            }
        }
        return false;
    }

//    public void incrementValue(Integer port){
//        dstoresCapacity.
//    }


//    public void removeDstore(int dstorePort) {
//        files.values().forEach(controllerDstoreSessions -> {
//            controllerDstoreSessions.removeIf(dstore -> dstore.getDstorePort() == dstorePort);
//        });
//        boolean removed = files.entrySet().removeIf(entry -> entry.getValue().isEmpty());
//        System.out.println(files.size());
//    }
    /**
     * Removes a dstore with the specified parameter form the list of dstores in the file.
     * This method is called when a dstore becomes unavailable
     * @param dstorePort the port of dstore to remove
     */
    public synchronized void removeDstore(int dstorePort) {
        Iterator<MyFile> it = files.iterator();
        while(it.hasNext()){
            MyFile f = it.next();
            f.getDstores().removeIf(dstoreSession -> dstoreSession.getDstorePort() == dstorePort);
            if(f.getDstores().isEmpty())
                it.remove();
        }
    }

    /**
     * Removes the specified filename,dstore entry from the hashmap.
     * This method is called when a user wanted to remove
     * @param filename
     * @param dstore
     */
    public synchronized void removeDstore(String filename, ControllerDstoreSession dstore){
        Iterator<MyFile> it = files.iterator();
        while(it.hasNext()){
            MyFile f = it.next();
            if(f.getName().equals(filename))
                f.removeDstore(dstore);
            if(f.getDstores().isEmpty())
                it.remove();
        }
    }
//    public void removeDstore(String filename, ControllerDstoreSession dstore) {
//        files.compute(filename, (key,value) -> {
//            value.removeIf( ds -> (ds.getDstorePort() == dstore.getDstorePort()));
//            System.out.println(value.size());
//            return (value.isEmpty()) ? null : value; //optional, removes the file from index if no dstores store it
//        });
//    }

}