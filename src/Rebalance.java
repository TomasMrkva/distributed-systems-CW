import java.util.*;

public class Rebalance {

    private HashMap<Integer, List<String>> dstoreFiles;
    private Map<String, List<Integer>> filesAllocation;   // filenames -> [Dstore_Ports]

    private Map<Integer, String> filesToRemove, filesToAdd;
    private Map<Integer, List<String>> filesToMove;
    private Stack<Integer[]> freeSpaces;
    private final Controller controller;
    private final int R;
    private final double floor, ceil;

    public Rebalance(HashMap<Integer, List<String>> dstoreFiles, Controller controller, int R) {
        this.dstoreFiles = dstoreFiles;
        filesAllocation = new HashMap<>();
        filesToRemove = new HashMap<>();
        filesToMove = new HashMap<>();
        freeSpaces = new Stack<>();
        this.controller = controller;
        this.R = R;
        double compute = (double) R * (filesAllocation.size()) / dstoreFiles.size();
        floor = Math.floor(compute);
        ceil = Math.ceil(compute);
        setup();
    }

    private void setup() {
        dstoreFiles.forEach((port, fileList) -> fileList.forEach(filename -> {
            List<Integer> list = filesAllocation.getOrDefault(filename, new ArrayList<>());
            list.add(port);
            filesAllocation.put(filename, list);
        }));
    }

    public void runRebalance() {
        indexFilter();
        extraFilesFilter();
    }

//    private void updateFilesToRemove(int port, String filename) {
//        String msg = filesToRemove.getOrDefault(port, "");
//        msg = msg + " " + filename;   //saves the name of files to remove
//        filesToRemove.put(port, msg);
//    }

    private void removeFile(String filename) {
        List<Integer> dstores = filesAllocation.remove(filename);
        for (Integer port : dstores) {
            List<String> files = dstoreFiles.get(port);
            files.remove(filename);
            dstoreFiles.put(port, files);   //updates the dstore with removed file

            String msg = filesToRemove.getOrDefault(port, "");
            msg = msg + " " + filename;   //saves the name of files to remove
            filesToRemove.put(port, msg);
        }
    }

    private void removeFile(String filename, int port) {
        List<Integer> ports = filesAllocation.get(filename);
        ports.remove(port);
        filesAllocation.put(filename, ports);

        List<String> files = dstoreFiles.get(port);
        files.remove(filename);
        dstoreFiles.put(port,files);

        String msg = filesToRemove.getOrDefault(port, "");
        msg = msg + " " + filename;   //saves the name of files to remove
        filesToRemove.put(port, msg);
    }

    private void indexFilter() {
        List<MyFile> filesToRemove = new ArrayList<>();
        Iterator<MyFile> it = controller.index.getFiles().iterator();
        while(it.hasNext()) {
            MyFile file = it.next();
            if(file.getOperation() == Index.Operation.REMOVE_COMPLETE || file.getOperation() == Index.Operation.REMOVE_IN_PROGRESS) {
                filesToRemove.add(file);
                it.remove();
            }
        }
        //TODO: check if iterator removes edits files from index
        filesToRemove.forEach( file -> removeFile(file.getName()));
    }

    private void extraFilesFilter() {
        filesAllocation.forEach( (filename, dstores) -> {
            if (dstores.size() > R) {
                List<Integer> tail = dstores.subList(R, dstores.size());
                for (Integer port : tail) {
                    removeFile(filename, port);
                }
            }
        });
    }

    private void evenFilesDistribution() {
        //TODO: start spreading files

        dstoreFiles.forEach((k, v) -> {
            if (v.size() < floor)
                freeSpaces.push(new Integer[]{k, (int) ceil - v.size()});
            else if (v.size() > ceil) {
                List<String> tail = v.subList(R, v.size());
                filesToMove.put(k, tail);
                tail.clear();
            }
        });

        filesToMove.forEach((originDS, files) -> {
            freeSpaces.forEach((newDS, capacity) -> {
                if (capacity > files.size()) {
                    capacity = capacity - files.size();

                }
            });
        });

        filesToMove.entrySet().forEach(entry -> {
            Integer port = entry.getKey();
            List<String> files = entry.getValue();
            while (!files.isEmpty()) {
                Integer[] top = freeSpaces.peek();
                int destinationPort = top[0];
                int freeSpace = top[1];
                if (freeSpace > files.size()) {
//                    freeSpaces.pop(); freeSpaces.push(new Integer[]{destinationPort,freeSpace - files.size()});
                    top[1] = freeSpace - files.size();
                    String[] msg = messages.get(destinationPort);
                    for (String s : files) {
                        msg[0] = msg[0] + " " + s;  //TODO: make sure that the files are not duplicate
                    }
                }
            }
        });
    }




}
