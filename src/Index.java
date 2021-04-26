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

    private final List<MyFile> files;
//    private final Object lockStoreProgress;
//    private final Object lockStoreFinish;
    private final Controller controller;

    public Index(Controller controller) {
        files = Collections.synchronizedList(new ArrayList<>());
//        lockStoreProgress = new Object();
//        lockStoreFinish = new Object();
        this.controller = controller;
//        dstoreLock =  new Object();
    }

    //    public void createFile(String filename) {
//        files.put(filename, Collections.synchronizedList(new ArrayList<>()));
//    }

    public boolean setStoreInProgress(String filename, int filesize){
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    if(!f.canStore()) return false;
                }
            }
            MyFile f = new MyFile(filename,filesize);
            f.setOperation(Operation.STORE_IN_PROGRESS);
            files.add(f);
            return true;
        }
    }

    public boolean exists(String filename){
        synchronized (files){
            for(MyFile f : files){
                if(f.exists())
                    return true;
            }
            return false;
        }
    }

    public boolean setRemoveInProgress(String filename){
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    if(!f.exists())
                        return false;
                    else {
                        f.setOperation(Operation.REMOVE_IN_PROGRESS);
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public List<MyFile> getFiles(){
        return files;
    }

    public boolean fileExists(String filename){
        synchronized (files){
            for (MyFile f : files){
                if (f.getName().equals(filename))
                    return true;
            }
        }
        return false;
    }

    public List<Integer> getNDstores(int n){
        List<Integer> dstores = new ArrayList<>();
        synchronized (files){
            files.forEach(myFile -> myFile.getDstores().forEach(dstore -> dstores.add(dstore.getDstorePort())));
        }
        dstores.addAll(controller.dstoreSessions.keySet());
        Map<Integer, Long> counts = dstores.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        counts = counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));
        List<Integer> result = new ArrayList<>(counts.keySet());
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
        Index index = new Index(null);
        MyFile[] files = {m1,m2,m3,m4,m5,m6};
        index.files.addAll(Arrays.asList(files));
        System.out.println("Result:");
        System.out.println(Arrays.toString(index.getNDstores(3).toArray()));

    }

    public boolean setStoreComplete(String filename){
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    f.setOperation(Operation.STORE_COMPLETE);
                    return true;
                }
            }
            return false;
        }
    }

    public void removeFile(String filename){
        synchronized (files){
//            System.out.println("FILESIZE BEFORE REMOVE:" + files.size());
            files.removeIf( myFile -> myFile.getName().equals(filename) );
            System.out.println("FILESIZE AFTER REMOVE: " + files.size());
        }
    }

    public Integer getFileSize(String filename){
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    return f.getFilesize();
                }
            }
            return null;
        }
    }

    public List<ControllerDstoreSession> getDstores(String filename){
        synchronized (files) {
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    return f.getDstores();
                }
            }
            return null;
        }
    }

    public boolean addDstore(String filename, ControllerDstoreSession dstore) {
        synchronized (files){
            for(MyFile f : files) {
                if(f.getName().equals(filename)){
                    f.addDstore(dstore);
                    return true;
                }
            }
            return false;
        }
    }

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
                f.getDstores().removeIf(dstoreSession -> dstoreSession.getDstorePort() == dstorePort);
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
//        synchronized (Lock.DSTORE){
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
//        }
    }

}