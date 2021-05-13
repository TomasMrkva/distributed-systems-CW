import java.util.*;
import java.util.stream.Collectors;

public class Index {

    public enum Operation {
        STORE_IN_PROGRESS,
        STORE_COMPLETE,
        REMOVE_IN_PROGRESS,
        REMOVE_COMPLETE
    }

    private final Map<String, IndexFile> files;
    private final Controller controller;
    private final HashMap<String,List<ControllerDstoreSession>> indexUpdate;

    public Index(Controller controller) {
        files = Collections.synchronizedMap(new HashMap<>());
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
//        synchronized (files) {
//            files.forEach( (name, f) -> {
//                System.out.print(f.getName() + " " + f.getSize() + " " + f.getOperation());
//                f.getDstores().forEach( d -> System.out.print(" " + d.getDstorePort()));
//                System.out.println();
//            });
//        }
    }

    public void updateIndex() {
        synchronized (files) {
            Iterator<Map.Entry<String, IndexFile>> it = files.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, IndexFile> entry = it.next();
                IndexFile f = entry.getValue();
                List<ControllerDstoreSession> dstores = indexUpdate.get(f.getName());
                if (dstores == null) {
                    it.remove();
//                    System.out.println("(X) INDEX:42 SOMETHING WENT WRONG");
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
//            for(IndexFile f : files) {
//                if(f.getName().equals(filename)){
//                    System.out.println(f.getName() + " " + f.getOperation());
//
//                }
//            }
            IndexFile previous = files.get(filename);
            if (previous != null) {
                if (!previous.canStore())
                    return false;
            }
            IndexFile updated = new IndexFile(filename,filesize);
            updated.setOperation(Operation.STORE_IN_PROGRESS);
            files.put(filename,updated);
            return true;
        }
    }

    public void setStoreComplete(String filename){
        synchronized (files){
            IndexFile f = files.get(filename);
//            if (f == null) throw new AssertionError();
            if (f != null)
                f.setOperation(Operation.STORE_COMPLETE);
            //            for(IndexFile f : files) {
//                if(f.getName().equals(filename)){
//                    f.setOperation(Operation.STORE_COMPLETE);
//                    return true;
//                }
            }
//            return false;

    }

    public boolean setRemoveInProgress(String filename){
        synchronized (files){
//            for(IndexFile f : files) {
//                if(f.getName().equals(filename)){
//                    if(!f.exists()) {
//                        return false;
//                    } else {
//                        f.setOperation(Operation.REMOVE_IN_PROGRESS);
//                        return true;
//                    }
//                }
                IndexFile f = files.get(filename);
                if (f == null) return false;
                else if(!f.exists()) {
                    return false;
                } else {
                    f.setOperation(Operation.REMOVE_IN_PROGRESS);
                    return true;
                }
            }
//            return false;
//        }
    }

//    public boolean setRemoveComplete(String filename){
//        synchronized (files){
//            for(IndexFile f : files) {
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

    public List<IndexFile> getFiles(){
        synchronized (files){
           return new ArrayList<>(files.values());
        }
    }

    public boolean fileExists(String filename){
        synchronized (files) {
//            for (IndexFile f : files){
//                if (f.getName().equals(filename)){
//                    return f.exists();
//                }
//            }
            IndexFile f = files.get(filename);
            if (f == null) return false;
            else return f.exists();
        }
//        return false;
    }

    public void removeFile(String filename){
//        synchronized (files){
            files.remove(filename);
//            System.out.println("FILESIZE BEFORE REMOVE:" + files.size());
//            files.removeIf( myFile -> myFile.getName().equals(filename) );
//            System.out.println("FILESIZE AFTER REMOVE: " + files.size());
//        }
    }

    public Integer getFileSize(String filename){
        synchronized (files){
//            for(IndexFile f : files) {
//                if(f.getName().equals(filename)){
//                    return f.getSize();
//                }
                IndexFile f = files.get(filename);
                return f.getSize();
//                if (f == null) throw new AssertionError();
//                else return f.getSize();
            }

//            return null;
//        }
    }

    /**
     * Returns dstores associated with the file
     * @param filename file which dstores are being requested
     * @param remove boolean flag to check whether remove opearation is calling this function or not
     * @return list of controller-dstore sessions or null if conditions are not met
     */
    public List<ControllerDstoreSession> getDstores(String filename, boolean remove) {
        synchronized (files) {
            IndexFile f = files.get(filename);
            if (f == null) {
                return null;
            } else if (remove && f.getOperation() == Operation.REMOVE_IN_PROGRESS) {
                return f.getDstores();
            } else if (!remove && f.exists()) {
                return f.getDstores();
            }
            return null;
        }
    }

    public boolean readyToRebalance() {
        synchronized (files) {
            for (IndexFile f : files.values()) {
                if (f.inProgress()) {
//                    System.out.println(f.getName() + " IN PROGRESS");
                    return false;
                }
            }
//            for (IndexFile f : files){
//                if (f.inProgress()) {
//                    System.out.println(f.getName() + " IN PROGRESS");
//                    return false;
//                }
//            }
            return true;
        }
    }

    public List<ControllerDstoreSession> getRDstores(int r){
        List<ControllerDstoreSession> dstores = new ArrayList<>();
        synchronized (files) {
            files.forEach( (name, indexFile) -> {
                Operation op = indexFile.getOperation();
                if (op == null || op == Operation.STORE_IN_PROGRESS || op == Operation.STORE_COMPLETE) {
                    dstores.addAll(indexFile.getDstores());
                }
            });
        }
        try { dstores.addAll(new ArrayList<>(controller.dstoreSessions.values())); }
        catch (NullPointerException e) { return null;}
        Collections.shuffle(dstores);
        Map<ControllerDstoreSession, Long> counts = dstores.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        counts = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
        List<ControllerDstoreSession> result = new ArrayList<>(counts.keySet());
//        System.out.println("(X) STORE: SELECTED DSTORES " + Arrays.toString(result.toArray()));
        return result.stream().limit(r).collect(Collectors.toList());
    }

//    public IndexFile getFile(String filename) {
//        synchronized (files) {
//            for (IndexFile f : files){
//                if (f.getName().equals(filename))
//                    return f;
//            }
//        }
//        return null;
//    }

//    public boolean addDstore(String filename, ControllerDstoreSession dstore) {
//        synchronized (files) {
//            for(IndexFile f : files) {
//                if(f.getName().equals(filename)){
//                    f.addDstore(dstore);
//                    return true;
//                }
//            }
//            throw new AssertionError();
////            return false;
//        }
//    }

    public boolean setDstores(String filename, List<ControllerDstoreSession> dstores) {
        synchronized (files) {
            dstores.removeIf(cd -> controller.dstoreSessions.get(cd.getDstorePort()) == null);
//            for(IndexFile f : files) {
//                if(f.getName().equals(filename)){
//                    f.addDstores(dstores);
//                    return true;
//                }
//            }
            IndexFile f = files.get(filename);
//            if (f == null) throw new AssertionError();
            if (f != null) {
                f.setDstores(dstores);
                return true;
            } return false;

//            return false;
        }
    }

//    public boolean removeDstores(String filename, List<ControllerDstoreSession> dstores) {
//        synchronized (files){
//            for(IndexFile f : files) {
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
            Iterator<IndexFile> it = files.values().iterator();
            while(it.hasNext()){
                IndexFile f = it.next();
//                System.out.println("REMOVED DSTORES ? " + f.getDstores().removeIf(dstoreSession -> dstoreSession.getDstorePort() == dstorePort));
                if(f.getDstores().isEmpty())
                    it.remove();
            }
        }
    }

//    /**
//     * Removes the specified filename,dstore entry from the hashmap.
//     * This method is called when a user wanted to remove
//     * @param filename
//     * @param dstore
//     */
//    public void removeDstore(String filename, ControllerDstoreSession dstore){
//        synchronized (files){
//            Iterator<IndexFile> it = files.iterator();
//            while(it.hasNext()){
//                IndexFile f = it.next();
//                if(f.getName().equals(filename))
//                    f.removeDstore(dstore);
//                if(f.getDstores().isEmpty())
//                    it.remove();
//            }
//        }
//    }

//    public static void main(String[] args) throws IOException {
//        ControllerDstoreSession c1 = new ControllerDstoreSession(1,null,null,null);
//        ControllerDstoreSession c2 = new ControllerDstoreSession(2,null,null,null);
//        ControllerDstoreSession c3 = new ControllerDstoreSession(3,null,null,null);
//        ControllerDstoreSession c4 = new ControllerDstoreSession(4,null,null,null);
//        ControllerDstoreSession c5 = new ControllerDstoreSession(5,null,null,null);
//        ControllerDstoreSession c6 = new ControllerDstoreSession(6,null,null,null);
//        IndexFile m1 = new IndexFile("m1",1);
//        IndexFile m2 = new IndexFile("m2",1);
//        IndexFile m3 = new IndexFile("m3",1);
//        IndexFile m4 = new IndexFile("m4",1);
//        IndexFile m5 = new IndexFile("m5",1);
//        IndexFile m6 = new IndexFile("m6",1);
//        m1.addDstore(c1); m1.addDstore(c2); m1.addDstore(c3); m1.addDstore(c4);
//        m2.addDstore(c1); m2.addDstore(c3);
//        m3.addDstore(c4); m3.addDstore(c4);
//        m4.addDstore(c5); m4.addDstore(c1);
//        m5.addDstore(c2); m5.addDstore(c5); m5.addDstore(c1);
//        Index index = new Index(null);
////        IndexFile[] files = {m1,m2,m3,m4,m5,m6};
////        index.files.addAll(Arrays.asList(files));
////        index.files.put("m1",m1);
//        index.setStoreInProgress("m1",1);
//        index.setStoreComplete("m1");
//        index.files.put("m2",m2);
//        index.files.put("m3",m3);
//        index.files.put("m4",m4);
//        index.files.put("m5",m5);
//        index.files.put("m6",m6);
//        System.out.println("Result:");
//        index.print();
//        index.setRemoveInProgress("m1");
//        index.print();
//        index.removeDstore(1);
//        index.print();
//        Map<Object,Object> map = Collections.synchronizedMap(new HashMap<>());
//        System.out.println(map.values());
////        System.out.println(Arrays.toString(index.getRDstores(3).toArray()));
//
//    }


}