import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toMap;

public class Rebalance implements Runnable {

    private final HashMap<Integer, ArrayList<String>> dstoreFiles;
    private final Map<String, List<Integer>> filesAllocation;   // filenames -> [Dstore_Ports]

    private final HashMap<Integer, StringBuilder> filesToRemove, filesToSend;
    private final HashMap<Integer, List<String>> overLimitFiles,correctFilesSize;
    private Map<Integer, List<String>> underLimitFiles;
    private final HashMap<String, Integer> masterDstores;

    private final CountDownLatch latch;
    private final Controller controller;
    private final Index index;
    private final int R;
    private int floor, ceil;

    public Rebalance(HashMap<Integer, ArrayList<String>> dstoreFiles, Controller controller, CountDownLatch latch) {
        this.latch = latch;
        this.dstoreFiles = dstoreFiles;
        overLimitFiles = new HashMap<>();
        underLimitFiles = new HashMap<>();
        correctFilesSize = new HashMap<>();
        filesToSend = new HashMap<>();
        filesToRemove = new HashMap<>();
        filesAllocation = new HashMap<>();
        masterDstores = new HashMap<>();
        this.controller = controller;
        this.index = controller.index;
        this.R = controller.R;
        setup();
    }

    private void setup() {
        dstoreFiles.forEach((port, fileList) -> fileList.forEach(filename -> {
            List<Integer> list = filesAllocation.getOrDefault(filename, new ArrayList<>());
            list.add(port);
            filesAllocation.put(filename, list);
        }));
    }

    @Override
    public void run() {
        indexFilter();
        extraFilesFilter();
        computeLimit();
        findDefaultDstores();
        divideDstores();
        replicateFiles();
        distributeFiles();
        formatRemoveMessages(); formatAddMessages();
//        System.out.println("[CORRECT FILES]");
//        correctFilesSize.entrySet().forEach(entry -> {
//            System.out.println(entry.getKey() + " " + entry.getValue());
//        });
//        System.out.println("[FILES TO REMOVE]");
//        filesToRemove.entrySet().forEach(entry -> {
//            System.out.println(entry.getKey() + " " + entry.getValue());
//        });
//        System.out.println("[FILES TO SEND]");
//        filesToSend.entrySet().forEach(entry -> {
//            System.out.println(entry.getKey() + " " + entry.getValue());
//        });
        controller.setRebalanceResult(constructCombinedMsg());
        sendToIndex();
        latch.countDown();
    }

    private void formatRemoveMessages() {
        filesToRemove.forEach( (port, msg) -> {
            String[] removeFiles = msg.toString().trim().split(" ");
            msg.insert(0, removeFiles.length);
        });
    }

    private void formatAddMessages() {
        filesToSend.forEach( (port, msg) -> {
            String[] addFiles = msg.toString().split(" ");
            Map<String, List<Integer>> map = new HashMap<>();
            for (int i = 0; i < addFiles.length; i += 2) {
                String filename = addFiles[i];
                int destPort = Integer.parseInt(addFiles[i+1]);
                List<Integer> destPorts = map.getOrDefault(filename, new ArrayList<>());
                destPorts.add(destPort);
                map.put(filename, destPorts);
            }
            StringBuilder newMsg = new StringBuilder(String.valueOf(map.size()));
            map.forEach((filename, ports) -> {
                newMsg.append(" ").append(filename);
                newMsg.append(" ").append(ports.size());
                ports.forEach( destPort -> newMsg.append(" ").append(destPort));
            });
            filesToSend.put(port, newMsg);
        });
    }

    private HashMap<Integer, String[]> constructCombinedMsg() {
        HashMap<Integer, String[]> combinedMessages = new HashMap<>();
        filesToSend.forEach( (port, strBuilder) -> {
            String[] msg = combinedMessages.getOrDefault(port,new String[2]);
            msg[0] = strBuilder.toString();
            msg[1] = "0";
            combinedMessages.put(port,msg);
        });
        filesToRemove.forEach( (port, strBuilder) -> {
            String[] msg = combinedMessages.getOrDefault(port,new String[2]);
            if(msg[0] == null) msg[0] = "0";
            msg[1] = strBuilder.toString();
            combinedMessages.put(port,msg);
        });
        return combinedMessages;
    }

    private void sendToIndex() {
        HashMap<String,List<Integer>> filesMap = new HashMap<>();
        correctFilesSize.forEach( (port,files) -> {
            for (String filename : files) {
                List<Integer> ports = filesMap.getOrDefault(filename,new ArrayList<>());
                ports.add(port);
                filesMap.put(filename,ports);
            }
        });
        index.saveFutureUpdate(filesMap);
    }

    private void updateFilesToRemove(int port, String filename) {
        StringBuilder msg = filesToRemove.getOrDefault(port, new StringBuilder());
        msg.append(" ").append(filename);    //saves the name of files to remove
        filesToRemove.put(port, msg);
    }

    private void updateFilesToSend(int sender, String filename, Integer receiver) {
//        List<Integer> ports = filesAllocation.get(filename);
//        ports.add(receiver);
//        filesAllocation.put(filename,ports);

//        List<String> files = dstoreFiles.get(receiver);
//        files.add(filename);
//        dstoreFiles.put(receiver,files);

        StringBuilder msg = filesToSend.getOrDefault(sender, new StringBuilder());
        msg.append(filename).append(" ").append(receiver).append(" ");   //saves the name of files to remove
        filesToSend.put(sender, msg);
    }

    private void removeFileEverywhere(String filename) {
        List<Integer> dstores = filesAllocation.remove(filename);
        if (dstores != null) {
            for (Integer port : dstores) {
                ArrayList<String> files = new ArrayList<>(dstoreFiles.get(port));
                files.remove(filename);
                dstoreFiles.put(port, files);   //updates the dstore after the removed file

                StringBuilder msg = filesToRemove.getOrDefault(port, new StringBuilder());
                msg.append(" ").append(filename);    //saves the name of files to remove
                filesToRemove.put(port, msg);
            }
        }
    }

    private void indexFilter() {
        List<String> filesToRemove = new ArrayList<>();
        List<String> indexFiles = new ArrayList<>();
        for (IndexFile file : index.getFiles()) {
            if (file.getOperation() == Index.Operation.REMOVE_COMPLETE || file.getOperation() == Index.Operation.REMOVE_IN_PROGRESS || file.getOperation() == null) {
                filesToRemove.add(file.getName());
//                it.remove();
            } else {
                indexFiles.add(file.getName());
            }
        }
        filesAllocation.forEach( (filename, dstores) -> {
            if (!indexFiles.contains(filename)){
                filesToRemove.add(filename);
            }
//            else if (index.getFiles().stream().map(IndexFile::getName).noneMatch(filename::equals)) {
//                filesToRemove.add(filename);
//            }
        });
        indexFiles.forEach( indexFileName -> {
            if (!filesAllocation.containsKey(indexFileName)){
                filesToRemove.add(indexFileName);
            }
        });
        filesToRemove.forEach(this::removeFileEverywhere);
        filesToRemove.forEach(index::removeFile);
    }

    private void extraFilesFilter() {
        filesAllocation.forEach( (filename, dstores) -> {
            if (dstores.size() > R) {
                List<Integer> tail = dstores.subList(R, dstores.size());
                Iterator<Integer> it = tail.iterator();
                while (it.hasNext()){
//                for (Integer port : tail) {
                    Integer port = it.next();
//                    List<Integer> ports = filesAllocation.get(filename);
//                    System.out.println("BEFORE REMOVE: " + ports);
                    it.remove();
//                    System.out.println("AFTER REMOVE: " + ports);
                    filesAllocation.put(filename,filesAllocation.get(filename));

                    ArrayList<String> files = dstoreFiles.get(port);
                    files.remove(filename);
                    dstoreFiles.put(port,files);
                    updateFilesToRemove(port,filename);
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
            if (v.size() == ceil) {
                correctFilesSize.put(k,v);
            } else if (v.size() > ceil) {
                overLimitFiles.put(k,v);
            } else {
                underLimitFiles.put(k,v);
            }
        });
        dstoreFiles.clear();
    }

    private void findDefaultDstores() {
        Map<String, List<Integer>> sorted = filesAllocation.entrySet().stream()
                .sorted(comparingInt(e -> e.getValue().size()))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> { throw new AssertionError(); },
                        LinkedHashMap::new
                ));

//        System.out.println("SORTED MAP ");
//        sorted.entrySet().forEach(entry -> {
//            System.out.println(entry.getKey() + ":" + entry.getValue());
//        });

        Random ran = new Random();
        Set<Integer> usedDstores = new HashSet<>();
        sorted.forEach((filename, ports) -> {
            for (int i=0; i < ports.size(); i++) {
                int currentPort = ports.get(i);
                if (!usedDstores.contains(currentPort)) {
                    usedDstores.add(currentPort);
                    masterDstores.put(filename,currentPort);
                    break;
                } else if (i == ports.size()-1) {
                    int randomPort = ports.get(ran.nextInt(ports.size()));
                    usedDstores.add(randomPort);
                    masterDstores.put(filename,randomPort);
                    break;
                }
            }
        });
    }

    private void sortUnderLimit() {
        underLimitFiles = underLimitFiles.entrySet().stream()
                .sorted(comparingInt(e -> e.getValue().size()))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> { throw new AssertionError(); },
                        LinkedHashMap::new));
    }

    private void replicateFiles() {
        sortUnderLimit();
        int max = 0;
        for ( List<String> files : underLimitFiles.values()) {
            if (files.size() > max)
                max = files.size();
        }
        int finalMax = max;
        filesAllocation.forEach( (filename, dstores) -> {
            while (dstores.size() < R) {
                boolean found = false;
                Iterator<Map.Entry<Integer, List<String>>> it = underLimitFiles.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Integer, List<String>> entry = it.next();
                    int destPort = entry.getKey();
                    List<String> destinationFiles = entry.getValue();
                    if (!dstores.contains(destPort)) {
                        found = true;
//                        updateFilesToSend(masterDstores.get(filename), filename, destPort);
                        StringBuilder msg = filesToSend.getOrDefault(masterDstores.get(filename), new StringBuilder());
                        msg.append(filename).append(" ").append(destPort).append(" ");   //saves the name of files to remove
                        filesToSend.put(masterDstores.get(filename), msg);
                        destinationFiles.add(filename);
//                        dstoreFiles.put(destPort, destinationFiles);
                        dstores.add(destPort);
//                        filesAllocation.put(filename,dstores);
                        if (destinationFiles.size() == ceil) {
                            it.remove();
                            correctFilesSize.put(destPort, destinationFiles);
                        } else if (destinationFiles.size() == finalMax){
                            it.remove();
                            underLimitFiles.put(destPort,destinationFiles);
                        } else if (destinationFiles.size() <= floor){
                            it.remove();
                            underLimitFiles.put(destPort,destinationFiles);
                        }
                        break;
                    }
                }
                if(!found) {
                    System.out.println("(X) REBALANCE ALGORITHM: ERROR WHILE REPLICATING FILES");
                    break;
                }
            }
        });
        filesAllocation.clear();
    }

    private void distributeFiles() {
        Iterator<Map.Entry<Integer, List<String>>> it = overLimitFiles.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, List<String>> currentPair = it.next();
            Integer originPort = currentPair.getKey();
            List<String> originFiles = currentPair.getValue();

            while (originFiles.size() > ceil) {
//                String foundFilename = originFiles.remove(0);
                String foundFilename = null;
                Map.Entry<Integer, List<String>> selectedEntry = null;
                for (Map.Entry<Integer, List<String>> entry : underLimitFiles.entrySet()) {
//                    System.out.println("CURRENT DSTORE: " + entry.getKey());
                    for (String filename : originFiles) {
                        if (!entry.getValue().contains(filename)) {
                            selectedEntry = entry;
//                            System.out.println("CURRENT DSTORE FOUND: " + filename + " " + entry.getKey());
                            foundFilename = filename;
                            break;
                        }
                    }
                    if (foundFilename != null) {
                        originFiles.remove(foundFilename);
                        break;
                    }
                }
                if (selectedEntry == null) {
                    System.out.println("(X) REBALANCE ERROR: NO ENTRY FOUND");
                    break;
                }
                int destPort = selectedEntry.getKey();
                List<String> destFiles = selectedEntry.getValue();
                destFiles.add(foundFilename);
                updateFilesToSend(masterDstores.get(foundFilename), foundFilename, destPort);
                updateFilesToRemove(originPort, foundFilename);
                if (destFiles.size() == ceil) {
                    underLimitFiles.remove(destPort);
                    correctFilesSize.put(destPort, destFiles);
                } else if (destFiles.size() == floor) {
                    underLimitFiles.remove(destPort);
                    underLimitFiles.put(destPort, destFiles);
                } else {
                    underLimitFiles.put(destPort, destFiles);
                }
            }
            correctFilesSize.put(originPort, originFiles);
            it.remove();
        }
        underLimitFiles.forEach( (k, v) -> {
            if (v.size() >= floor) correctFilesSize.put(k, v);
        });
        underLimitFiles.entrySet().removeIf(entries -> entries.getValue().size() >= floor);
        if (!underLimitFiles.isEmpty()) {
            System.out.println("(i) Retrying to distribute files");
            fillUnderLimitFiles();
        }
    }

    private void fillUnderLimitFiles() {
        HashMap<Integer, List<String>> ceilFiles = new HashMap<>();
        correctFilesSize.forEach((k, v) -> {
            if (v.size() == ceil) ceilFiles.put(k, v);
        });
        if(ceilFiles.isEmpty())
            return;
        underLimitFiles.forEach( (destPort, destFiles) -> {
            while (destFiles.size() < floor) {
                boolean found = false;
                Iterator<Map.Entry<Integer, List<String>>> it = ceilFiles.entrySet().iterator();                        // List<OriginFiles> it
                while (it.hasNext()) {                                                                              // ceilingFiles entry
                    Map.Entry<Integer, List<String>> entry = it.next();
                    int originPort = entry.getKey();                                        // ceilingFiles -> (originPort, originFiles)
                    List<String> originFiles = entry.getValue();
                    Iterator<String> iter = originFiles.iterator();                        // List<OriginFiles> it
                    while (iter.hasNext()) {
                        String filename = iter.next();                                      // filename it.get(next);
                        if (!destFiles.contains(filename)) {                                // underLimit -> (destPort,destFiles).contains(filename)
                            destFiles.add(filename);                                        // underLimit (destFiles).add(filename)
                            updateFilesToSend(masterDstores.get(filename), filename, destPort);
                            updateFilesToRemove(originPort, filename);
                            it.remove();
                            iter.remove();                                                  //originFiles.remove(filename)
                            correctFilesSize.put(originPort, originFiles);
                            found = true;
                            break;
                        }
                    }
                    if (destFiles.size() == floor) break;
                }
                if (!found) {
//                    System.out.println("(X) REBALANCE ERROR: Could not distribute dstores");
                    break;
                }
            }
            correctFilesSize.put(destPort, destFiles);
        });
    }

}
