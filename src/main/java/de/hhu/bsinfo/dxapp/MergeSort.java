package de.hhu.bsinfo.dxapp;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.ms.MasterNodeEntry;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.TaskScript;
import de.hhu.bsinfo.dxram.ms.tasks.mergesort.ExportTask;
import de.hhu.bsinfo.dxram.ms.tasks.mergesort.MergeTask;
import de.hhu.bsinfo.dxram.ms.tasks.mergesort.RessourceTask;
import de.hhu.bsinfo.dxram.ms.tasks.mergesort.SortTask;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.*;


/**
 * "Parallel Mergesort  DXRAM application "
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */

public class MergeSort extends AbstractApplication {

    private final static short GLOBAL_PEER_MINIMUM = 1;
    private final static short GLOBAL_PEER_MAXIMUM = 10;
    private final static int GLOBAL_CHUNK_SIZE = 64;

    private static boolean WRITE_OUT_FLAG = false;

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "MergeSort";
    }

    @Override
    public void main(final String[] p_args) {

        // Get services
        BootService bootService = getService(BootService.class);
        ChunkService chunkService = getService(ChunkService.class);
        NameserviceService nameService = getService(NameserviceService.class);
        MasterSlaveComputeService masterSlaveComputeService = getService(MasterSlaveComputeService.class);

        System.out.println("Hello, I am application " + getApplicationName() + " on a peer and my node id is " +
                NodeID.toHexString(bootService.getNodeID()) + " with " + p_args.length + " cmd args: " +
                Arrays.toString(p_args));

        // Put your application code running on the DXRAM node/peer here

        // Get all IDs of the online nodes get MS-role
        List<Short> onlineNodeIDs = bootService.getOnlineNodeIDs();
        List<Short> onlineWorkerNodeIDs = new ArrayList<>();
        Iterator<Short> onlineNodeIDsIterator = onlineNodeIDs.iterator();

        // Get all IDs of the masternodes
        ArrayList<MasterNodeEntry> masterNodes = masterSlaveComputeService.getMasters();
        ArrayList<Short> masterNodeIDs = new ArrayList<>();
        for (MasterNodeEntry tmp: masterNodes){
            masterNodeIDs.add(tmp.getNodeId());
        }

        // Print online nodes and add them to the IDlist
        System.out.println("Liste vorhandener NodeIDs:");
        for (int i=1; onlineNodeIDsIterator.hasNext(); i++){
            Short tmp = onlineNodeIDsIterator.next();
            if (masterNodeIDs.contains(tmp))
                System.out.println(i + ". " + NodeID.toHexString(tmp) + " - " + bootService.getNodeRole(tmp).toString() +  " - masternode");
            else
                System.out.println(i + ". " + NodeID.toHexString(tmp) + " - " + bootService.getNodeRole(tmp).toString() );

            if (bootService.getNodeRole(tmp).toString().equals("peer") && !masterNodeIDs.contains(tmp))
                onlineWorkerNodeIDs.add(tmp);
        }

        // Read data from file given in argument p_args[0] to inputData
        String filepath = "dxapp/data/" + p_args[0];
        String seperator = ", ";
        System.out.println("Working Directory = " + System.getProperty("user.dir"));
        System.out.println("Filepath = " + filepath);
        List<Integer> inputData = readData(filepath, seperator);

        WRITE_OUT_FLAG = (p_args[1].equals("-e"));

        System.out.println(masterSlaveComputeService.getComputeRole().toString());

        // Get resources (number of available cores)
        RessourceTask ressourceTask = new RessourceTask();
        TaskScript ressourceSkript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "resource Task", ressourceTask);
        masterSlaveComputeService.submitTaskScript(ressourceSkript);
        System.out.println("Führe resource-Task aus!");

        // Save data of the resource-Task
        int[] resources = new int[onlineWorkerNodeIDs.size()];
        int split = 0;
        for (int i=0; i<onlineWorkerNodeIDs.size();i++){
            long chunkID= nameService.getChunkID("RC-" +i,100);
            resources[i] = getIntData(chunkID, chunkService);
            split += resources[i];
        }

        int sizeOfPartedData = inputData.size() / split;
        int overhead = inputData.size() % split;

        System.out.println("Größe der InputDate: " + inputData.size());
        System.out.println("Größe der Splits: " + split);
        System.out.println("Größe der Splitdaten: " + sizeOfPartedData);

        int[] addressChunkSize = new int[onlineWorkerNodeIDs.size()];

        // Write Chunk Ids to Matrix
        Iterator dataIterator = inputData.iterator();
        for (int i=0; i<onlineWorkerNodeIDs.size(); i++){
            long[] tmpIds;
            long[] tmpAddressChunkId = new long[1];
            long[] tmpSizeChunkId = new long[1];

            // Eventuell Idee verbessern
            if (overhead!= 0){
                if (overhead >= resources[i]){
                    tmpIds = new long[resources[i]*(sizeOfPartedData+1)];
                    overhead -= resources[i];
                } else {
                    tmpIds = new long[sizeOfPartedData+overhead];
                }

            } else {
                tmpIds = new long[resources[i]*sizeOfPartedData];
            }

            chunkService.create().create(onlineWorkerNodeIDs.get(i), tmpIds, tmpIds.length, GLOBAL_CHUNK_SIZE);
            for (long tmpId : tmpIds) {
                editChunkInt((Integer) dataIterator.next(), tmpId, 1, chunkService);
            }

            // Create, register AddressChunk
            chunkService.create().create(onlineNodeIDs.get(i), tmpAddressChunkId, 1, GLOBAL_CHUNK_SIZE*tmpIds.length);
            editChunkArray(tmpIds, tmpAddressChunkId[0], chunkService);
            nameService.register(tmpAddressChunkId[0], "AC" + i);

            // Size of AddressChunk
            chunkService.create().create(onlineNodeIDs.get(i), tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(tmpIds.length, tmpSizeChunkId[0], 1, chunkService);
            nameService.register(tmpSizeChunkId[0], "SAC" + i);

            // Create GoThrough-Parameter
            chunkService.create().create(onlineNodeIDs.get(i), tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(2, tmpSizeChunkId[0], 1, chunkService);
            nameService.register(tmpSizeChunkId[0], "GT" + i);

            addressChunkSize[i]=tmpIds.length;
        }

        SortTask sortTask = new SortTask();
        TaskScript sortSkript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Sort Task", sortTask);
        masterSlaveComputeService.submitTaskScript(sortSkript);
        System.out.println("Führe sort-Task aus!");


        int goThrough = onlineWorkerNodeIDs.size();

        MergeTask mergeTask = new MergeTask();
        TaskScript mergeScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Merge Task", mergeTask);
        while (goThrough > 1){
            masterSlaveComputeService.submitTaskScript(mergeScript);
            System.out.println("Führe merge-Task aus!");
            goThrough /=2;
        }

        if (WRITE_OUT_FLAG){
            ExportTask exportTask = new ExportTask();
            TaskScript exportScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Export Task", exportTask);
            masterSlaveComputeService.submitTaskScript(exportScript);
            System.out.println("Führe export-Task aus!");
        }
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }

    private void editChunkInt(int value, long chunkId, int size , ChunkService chunkService){
        ByteBuffer byteBuffer = ByteBuffer.allocate(size*GLOBAL_CHUNK_SIZE);
        byteBuffer.putInt(value);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }

    private void editChunkArray (long[] array, long chunkId, ChunkService chunkService){
        ByteBuffer byteBuffer = ByteBuffer.allocate(array.length*GLOBAL_CHUNK_SIZE);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.put(array);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }

    private long[] getLongArray(long chunkId, int size, ChunkService chunkService) {
        ChunkByteArray testChunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE*size);
        chunkService.get().get(testChunk);
        byte[] byteData = testChunk.getData();
        LongBuffer longBuffer = ByteBuffer.wrap(byteData)
                .order(ByteOrder.BIG_ENDIAN)
                .asLongBuffer();

        long[] longArray = new long[size];
        longBuffer.get(longArray);

        return longArray;

    }

    private int getIntData(long chunkId, ChunkService chunkService){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();
        return ByteBuffer.wrap(byteData).getInt();
    }

    private List<Integer> readData(String filepath, String seperator){
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(filepath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        scanner.useDelimiter(seperator);
        List<Integer> list = new ArrayList<Integer>();

        while (scanner.hasNext()) {
            list.add(scanner.nextInt());
        }
        scanner.close();
        return list;
    }
}
