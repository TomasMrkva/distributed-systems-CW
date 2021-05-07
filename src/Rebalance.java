import java.util.*;

public class Rebalance {

    private final HashMap<Integer, List<String>> dstoreFiles;
    private Map<String, List<Integer>> filesAllocation;   // filenames -> [Dstore_Ports]

    private final HashMap<Integer, StringBuilder> filesToRemove, filesToAdd;
    private final HashMap<Integer, List<String>> overLimitFiles;
    private final HashMap<Integer, List<String>> underLimitFiles;
    private final HashMap<Integer, List<String>> correctFilesSize;
    private final HashMap<Integer, List<String>> floorLimitFiles;
    private final HashMap<String, Integer> defaultDstores;
    private final Controller controller;
    private final Index index;
    private final int R;
    private int floor, ceil;

    public Rebalance(HashMap<Integer, List<String>> dstoreFiles, Controller controller, int R, Index index) {
        this.dstoreFiles = dstoreFiles;
        overLimitFiles = new HashMap<>();
        underLimitFiles = new HashMap<>();
        correctFilesSize = new HashMap<>();
        floorLimitFiles = new HashMap<>();
        filesToAdd = new HashMap<>();
        filesToRemove = new HashMap<>();
        filesAllocation = new HashMap<>();
        defaultDstores = new HashMap<>();
        this.controller = controller;
        this.index = index;
        this.R = R;
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
        replicateFiles();
        computeLimit();
        divideDstores();
        evenFilesDistribution();
    }

    private void formatRemoveMessages() {
        filesToRemove.forEach( (port, msg) -> {
            String[] removeFiles = msg.toString().split(" ");
            msg.insert(0, removeFiles.length + " ");
        });
    }

    private void formatAddMessages() {
        filesToAdd.forEach( (port,msg) -> {
            String[] addFiles = msg.toString().split(" ");
            Map<String, List<Integer>> map = new HashMap<>();
            for (int i = 0; i < addFiles.length; i += 2){
                String filename = addFiles[i];
                int destPort = Integer.parseInt(addFiles[i+1]);
                List<Integer> destPorts = map.getOrDefault(filename, new ArrayList<>());
                destPorts.add(destPort);
                map.put(filename, destPorts);
            }
            StringBuilder newMsg = new StringBuilder(map.size() + " ");
            map.forEach((filename, ports) -> {
                newMsg.append(filename).append(" ");
                ports.forEach( destPort -> newMsg.append(destPort).append(" "));
            });
            filesToAdd.put(port, newMsg);
        });
    }

//    public HashMap<Integer, String> getRemoveMessages() {
//        return filesToRemove;
//    }
//
//    public HashMap<Integer, String> getAddMessages() {
//        return filesToAdd;
//    }

    private void updateFilesToRemove(int port, String message) {
        StringBuilder msg = filesToRemove.getOrDefault(port, new StringBuilder());
        msg.append(message).append(" ");    //saves the name of files to remove
        filesToRemove.put(port, msg);
    }

    private void updateFilesToAdd(int port, String message) {
        StringBuilder msg = filesToAdd.getOrDefault(port, new StringBuilder());
        msg.append(message).append(" ");   //saves the name of files to remove
        filesToAdd.put(port, msg);
    }

    private void removeFile(String filename) {
        List<Integer> dstores = filesAllocation.remove(filename);
        for (Integer port : dstores) {
            List<String> files = new ArrayList<>(dstoreFiles.get(port));
            files.remove(filename);
            dstoreFiles.put(port, files);   //updates the dstore with removed file

            updateFilesToRemove(port, filename);
//            String msg = filesToRemove.getOrDefault(port, "");
//            msg = msg + " " + filename;   //saves the name of files to remove
//            filesToRemove.put(port, msg);
        }
    }

    private void removeFile(String filename, int port) {
        List<Integer> ports = new ArrayList<>(filesAllocation.get(filename));
        ports.remove(Integer.valueOf(port));
        filesAllocation.put(filename, ports);

        List<String> files = new ArrayList<>(dstoreFiles.get(port));
        files.remove(filename);
        dstoreFiles.put(port,files);

        updateFilesToRemove(port, filename);
//        String msg = filesToRemove.getOrDefault(port, "");
//        msg = msg + " " + filename;   //saves the name of files to remove
//        filesToRemove.put(port, msg);
    }

    private void indexFilter() {
        List<MyFile> filesToRemove = new ArrayList<>();
//        Iterator<MyFile> it = controller.index.getFiles().iterator();
        Iterator<MyFile> it = index.getFiles().iterator();
        while(it.hasNext()) {
            MyFile file = it.next();
            if(file.getOperation() == Index.Operation.REMOVE_COMPLETE || file.getOperation() == Index.Operation.REMOVE_IN_PROGRESS) {
                filesToRemove.add(file);
                it.remove();
            }
        }
        filesToRemove.forEach( file -> removeFile(file.getName())); //removes files from index
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

    private void computeLimit() {
        double compute = ((double) R * filesAllocation.size()) / dstoreFiles.size();
        floor = (int) Math.floor(compute);
        ceil = (int) Math.ceil(compute);
    }

    private void divideDstores() {
        dstoreFiles.forEach( (k, v) -> {
            if (v.size() < floor)
                underLimitFiles.put(k,v);
            else if (v.size() > ceil) {
                overLimitFiles.put(k,v);
            } else if (v.size() == floor){
                floorLimitFiles.put(k,v);
            } else {
                correctFilesSize.put(k,v);
            }

        });
    }

    private void findDefaultDstores() {
        List<Integer> usedDstores = new ArrayList<>();
        filesAllocation.forEach( (file, dstores) -> {
            Random r = new Random();
            defaultDstores.put(file, dstores.get(r.nextInt(dstores.size())));
        });
    }

    private void replicateFiles() {
        filesAllocation.forEach( (filename, dstores) -> {
            while(dstores.size() < R){
                boolean found = false;
                Iterator<Map.Entry<Integer, List<String>>> it1 = underLimitFiles.entrySet().iterator();
                while (it1.hasNext()) {
                    int destinationPort = it1.next().getKey();
                    if (!dstores.contains(destinationPort)) {
                        found = true;
                        updateFilesToAdd(defaultDstores.get(filename), filename + " " + destinationPort);
                        List<String> destinationFiles = new ArrayList<>(dstoreFiles.get(destinationPort));
                        destinationFiles.add(filename);
                        dstoreFiles.put(destinationPort, destinationFiles);
                        dstores.add(destinationPort);
                        if (destinationFiles.size() == floor) {
                           it1.remove();
                           floorLimitFiles.put(destinationPort,destinationFiles);
                        }
                        break;
                    }
                }
                if (found) continue;
                Iterator<Map.Entry<Integer, List<String>>> it2 = floorLimitFiles.entrySet().iterator();
                while (it2.hasNext()) {
                    int destinationPort = it2.next().getKey();
                    if (!dstores.contains(destinationPort)) {
                        updateFilesToAdd(defaultDstores.get(filename), filename + " " + destinationPort);
                        List<String> destinationFiles = new ArrayList<>(dstoreFiles.get(destinationPort));
                        destinationFiles.add(filename);
                        dstoreFiles.put(destinationPort, destinationFiles);
                        dstores.add(destinationPort);
                        correctFilesSize.put(destinationPort,destinationFiles);
                        it2.remove();
                        break;
                    }
                }
            }
            filesAllocation.put(filename, dstores);
        });
    }

    private void evenFilesDistribution() {
        Iterator<Map.Entry<Integer, List<String>>> it = overLimitFiles.entrySet().iterator();
        while ( it.hasNext() ) {
            Map.Entry<Integer, List<String>> currentPair = it.next();
            Integer originPort = currentPair.getKey();
            List<String> originFiles = currentPair.getValue();

            while (originFiles.size() > ceil) {
                String filename = originFiles.remove(0);
                if (!underLimitFiles.isEmpty()) {
                    underLimitFiles.putAll(floorLimitFiles);
                    floorLimitFiles.clear();
                }
                Map.Entry<Integer, List<String>> selectedEntry = null;
                for (Map.Entry<Integer, List<String>> entry : underLimitFiles.entrySet()){
                    if (!entry.getValue().contains(filename)){
                        selectedEntry = entry;
                        break;
                    }
                }
                if (selectedEntry == null) {
                    //TODO: exception here
                    System.err.println("ERROR WHILE DISTRIBUTING FILES DURING REBALANCE");
                    break;
                }
                int destPort = selectedEntry.getKey();
                List<String> destFiles = selectedEntry.getValue();
                selectedEntry.getValue().add(filename);
                updateFilesToAdd(originPort,  filename + " " + destPort);
                updateFilesToRemove(originPort, filename);
                int freeSpaces = ceil - selectedEntry.getValue().size();
                if (freeSpaces == 0) {
                    underLimitFiles.remove(destPort);
                    correctFilesSize.put(destPort, destFiles);
                }
            }
            correctFilesSize.put(originPort,originFiles);
            it.remove();
        }
        correctFilesSize.putAll(underLimitFiles);
//        correctFilesSize.putAll(floorLimitFiles);
    }

    private void printState() {
        System.out.println("[DSTORE FILES]");
        dstoreFiles.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });

        System.out.println("[FILES ALLOCATION]");
        filesAllocation.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });

        System.out.println("[DEFAULT DSTORES]");
        defaultDstores.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });

        System.out.println("[FILES TO REMOVE]");
        filesToRemove.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + Arrays.toString(entry.getValue().toString().split(" ")));
        });

        System.out.println("[FILES TO ADD]");
        filesToAdd.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        });

        System.out.println("[OVER-LIMIT FILES]");
        overLimitFiles.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });

        System.out.println("[UNDER-LIMIT FILES]");
        underLimitFiles.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });

        System.out.println("[CORRECT FILES]");
        correctFilesSize.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });

        System.out.println("[FLOOR FILES]");
        floorLimitFiles.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });
    }


    public static void main(String[] args) {
        Index index = new Index(null);
        index.setStoreInProgress("a.txt", 1);
        index.setStoreComplete("a.txt");
        index.setStoreInProgress("b.txt", 1);
        index.setStoreComplete("b.txt");
        index.setStoreInProgress("c.txt", 1);
        index.setStoreComplete("c.txt");
        index.setStoreInProgress("d.txt", 1);
        index.setStoreComplete("d.txt");
        index.setRemoveInProgress("a.txt");
        index.setRemoveComplete("a.txt");
        index.setRemoveInProgress("b.txt");

        HashMap<Integer, List<String>> temp = new HashMap<>();
        temp.put(10, Arrays.asList("a.txt", "b.txt"));
        temp.put(11, Arrays.asList("c.txt","a.txt","d.txt"));
        temp.put(12, Arrays.asList("b.txt","c.txt"));
        temp.put(13, Arrays.asList("d.txt"));

        Rebalance rebalance = new Rebalance(temp,null, 3, index);

        System.out.println("***START***");
        rebalance.printState();

        rebalance.indexFilter();
//        for (MyFile m : rebalance.index.getFiles()) {
//            System.out.println(m.getName() + " : " + m.getOperation() + " -> " + m.getDstores());
//        }
        System.out.println("\n***AFTER INDEX FILTER***");
        rebalance.printState();

        System.out.println("\n***AFTER EXTRA FILES FILTER***");
        rebalance.extraFilesFilter();
        rebalance.printState();

        rebalance.computeLimit();
        System.out.println("\nCEIL: " + rebalance.ceil + "  FLOOR: " + rebalance.floor);

        System.out.println("\n***AFTER DEFAULT DSTORES ***");
        rebalance.findDefaultDstores();
        rebalance.printState();


        System.out.println("\n***AFTER DIVING DSTORES***");
        rebalance.divideDstores();
        rebalance.printState();

        System.out.println("\n***AFTER REPLICATING FILES***");
        rebalance.replicateFiles();
        rebalance.printState();



        System.out.println("\n***AFTER DISTRIBUTING DSTORES***");
        rebalance.evenFilesDistribution();
        rebalance.printState();

        rebalance.formatRemoveMessages();
        rebalance.printState();

        rebalance.formatAddMessages();
        rebalance.printState();
    }



}
