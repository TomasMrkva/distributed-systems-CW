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

    private final Set<MyFile> files;
    private final Controller controller;
    private final HashMap<String,List<ControllerDstoreSession>> indexUpdate;

    public Index(Controller controller) {
        files = Collections.synchronizedSet(new HashSet<>());
        this.controller = controller;
        this.indexUpdate = new HashMap<>();
    }

    public void saveFutureUpdate(HashMap<String,List<Integer>> indexUpdate) {
        this.indexUpdate.clear();
        indexUpdate.forEach( (filename, ports) -> {
            List<ControllerDstoreSession> dstores = new ArrayList<>();
            for (Integer port : ports) {
                ControllerDstoreSession cd = controller.dstoreSessions.get(port);
                if (cd != null) {
                    dstores.add(cd);
                }
            }
            this.indexUpdate.put(filename,dstores);
        });
    }

    public void print() {
        synchronized (files) {
            for (MyFile f : files) {
                System.out.println(f.getName() + " " + f.getSize() + " " + f.getOperation());
                f.getDstores().forEach( d -> System.out.print(" " + d.getDstorePort()));
            }
        }
    }

    public void updateIndex() {
        synchronized (files) {
            Iterator<MyFile> it = files.iterator();
            while (it.hasNext()) {
                MyFile f = it.next();
                List<ControllerDstoreSession> dstores = indexUpdate.get(f.getName());
                if (dstores == null) {
                    it.remove();
                    System.out.println("(X) INDEX:42 SOMETHING WENT WRONG");
                } else {
                    f.setDstores(dstores);
                    f.setOperation(Operation.STORE_COMPLETE);
                }
            }
            indexUpdate.clear();
        }
    }

    public boolean setStoreInProgress(String filename, int filesize){
        synchronized (files) {
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    System.out.println(f.getName() + " " + f.getOperation());
                    if(!f.canStore()) return false;
                }
            }
            MyFile f = new MyFile(filename,filesize);
            f.setOperation(Operation.STORE_IN_PROGRESS);
            files.add(f);
            return true;
        }
    }

    public boolean setStoreComplete(String filename){
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    f.setOperation(Operation.STORE_COMPLETE);
                    return true;
                }
            }
//            return false;
            throw new AssertionError();
        }
    }

    public boolean setRemoveInProgress(String filename){
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    if(!f.exists()) {
                        return false;
                    } else {
                        f.setOperation(Operation.REMOVE_IN_PROGRESS);
                        return true;
                    }
                }
            }
            return false;
        }
    }

//    public boolean setRemoveComplete(String filename){
//        synchronized (files){
//            for(MyFile f : files) {
//                if(f.getName().equals(filename)){
//                    if(f.getOperation() == Operation.REMOVE_IN_PROGRESS) {
//                        f.setOperation(Operation.REMOVE_COMPLETE);
//                        return true;
//                    } else {
//                        System.err .println("ERROR IN INDEX 98: -> THIS SHOULD NEVER HAPPEN!");
//                        return false;
//                    }
//                }
//            }
//            throw new AssertionError();
////            return false;
//        }
//    }

    public Set<MyFile> getFiles(){
        synchronized (files){
           return files;
        }
    }

    public boolean fileExists(String filename){
        synchronized (files){
            for (MyFile f : files){
                if (f.getName().equals(filename)){
                    return f.exists();
                }
            }
        }
        return false;
    }

    public void removeFile(String filename){
        synchronized (files){
//            System.out.println("FILESIZE BEFORE REMOVE:" + files.size());
            files.removeIf( myFile -> myFile.getName().equals(filename) );
//            System.out.println("FILESIZE AFTER REMOVE: " + files.size());
        }
    }

    public Integer getFileSize(String filename){
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    return f.getSize();
                }
            }
            throw new AssertionError();
//            return null;
        }
    }

    /**
     * Returns dstores associated with the file
     * @param filename file which dstores are being requested
     * @param remove boolean flag to check whether remove opearation is calling this function or not
     * @return list of controller-dstore sessions or null if conditions are not met
     */
    public List<ControllerDstoreSession> getDstores(String filename, boolean remove) {
        synchronized (files) {
            for(MyFile f : files) {
                if (f.getName().equals(filename)) {
                    if (remove && f.getOperation() == Operation.REMOVE_IN_PROGRESS) {
                        return f.getDstores();
                    } else if (!remove && f.exists()) {
                        return f.getDstores();
                    } else {
                        return null;
                    }
                }
            }
            return null;
        }
    }

    public boolean readyToRebalance() {
        synchronized (files) {
            for (MyFile f : files){
                if (f.inProgress()) {
                    System.out.println(f.getName() + " IN PROGRESS");
                    return false;
                }
            }
        }
        return true;
    }

    public List<ControllerDstoreSession> getRDstores(int r){
        List<ControllerDstoreSession> dstores = new ArrayList<>();
        synchronized (files) {
            files.forEach(myFile -> {
                Operation op = myFile.getOperation();
                if (op == null || op == Operation.STORE_IN_PROGRESS || op == Operation.STORE_COMPLETE) {
                    dstores.addAll(myFile.getDstores());
                }
            });
        }
        dstores.addAll(new ArrayList<>(controller.dstoreSessions.values()));
        Collections.shuffle(dstores);
        Map<ControllerDstoreSession, Long> counts = dstores.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        counts = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
        List<ControllerDstoreSession> result = new ArrayList<>(counts.keySet());
        System.out.println("(X) STORE: SELECTED DSTORES " + Arrays.toString(result.toArray()));
        return result.stream().limit(r).collect(Collectors.toList());
    }

//    public MyFile getFile(String filename) {
//        synchronized (files) {
//            for (MyFile f : files){
//                if (f.getName().equals(filename))
//                    return f;
//            }
//        }
//        return null;
//    }

    public boolean addDstore(String filename, ControllerDstoreSession dstore) {
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    f.addDstore(dstore);
                    return true;
                }
            }
            throw new AssertionError();
//            return false;
        }
    }
    public boolean addDstores(String filename, List<ControllerDstoreSession> dstores) {
        synchronized (files){
            dstores.removeIf(cd -> controller.dstoreSessions.get(cd.getDstorePort()) == null);
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    f.addDstores(dstores);
                    return true;
                }
            }
            throw new AssertionError();
//            return false;
        }
    }

//    public boolean removeDstores(String filename, List<ControllerDstoreSession> dstores) {
//        synchronized (files){
//            for(MyFile f : files) {
//                if(f.getName().equals(filename)){
//                    f.getDstores().removeAll(dstores);
//                    return true;
//                }
//            }
//            return false;
//        }
//    }

    /**
     * Removes a dstore with the specified parameter form the list of dstores in the file.
     * This method is called when a dstore becomes unavailable
     * @param dstorePort the port of dstore to remove
     */
    public void removeDstore(int dstorePort) {
        synchronized (files){
            Iterator<MyFile> it = files.iterator();
            while(it.hasNext()){
                MyFile f = it.next();
                System.out.println("REMOVED DSTORES ? " + f.getDstores().removeIf(dstoreSession -> dstoreSession.getDstorePort() == dstorePort));
                if(f.getDstores().isEmpty())
                    it.remove();
            }
        }
    }

    /**
     * Removes the specified filename,dstore entry from the hashmap.
     * This method is called when a user wanted to remove
     * @param filename
     * @param dstore
     */
    public void removeDstore(String filename, ControllerDstoreSession dstore){
        synchronized (files){
            Iterator<MyFile> it = files.iterator();
            while(it.hasNext()){
                MyFile f = it.next();
                if(f.getName().equals(filename))
                    f.removeDstore(dstore);
                if(f.getDstores().isEmpty())
                    it.remove();
            }
        }
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
        Index index = new Index(null);
        MyFile[] files = {m1,m2,m3,m4,m5,m6};
        index.files.addAll(Arrays.asList(files));
        System.out.println("Result:");
        System.out.println(Arrays.toString(index.getRDstores(3).toArray()));

    }


}