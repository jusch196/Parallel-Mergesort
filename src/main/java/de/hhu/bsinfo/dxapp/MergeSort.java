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
import de.hhu.bsinfo.dxram.ms.tasks.mergesortapplication.*;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.*;


/**
 * "Parallel Mergesort  DXRAM application "
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
public class MergeSort extends AbstractApplication {
    private static String filepath ="";
    private static String seperator =", ";

    private final static int GLOBAL_CHUNK_SIZE = 64;

    private static int writeOutSize = 1;
    private static boolean normal = false;

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

        List<Short> onlineNodeIDs = bootService.getOnlineNodeIDs();
        List<Short> onlineWorkerNodeIDs = new ArrayList<>();
        Iterator<Short> onlineNodeIDsIterator = onlineNodeIDs.iterator();
        ArrayList<String> arguments = new ArrayList<>(Arrays.asList(p_args));

        if (arguments.contains("--path")){
            int argumentIndex = arguments.indexOf("--path")+1;
            filepath = arguments.get(argumentIndex);
        }
        if (arguments.contains("--normal")){
            normal = true;
        }
        if (arguments.contains("-- out")){
            int argumentIndex = arguments.indexOf("--out")+1;
            writeOutSize = Integer.parseInt(arguments.get(argumentIndex));
            if (writeOutSize < 0)
                throw new IllegalArgumentException("Exportparameter has to be positive");
        }

        ArrayList<MasterNodeEntry> masterNodes = masterSlaveComputeService.getMasters();
        ArrayList<Short> masterNodeIDs = new ArrayList<>();
        for (MasterNodeEntry tmp: masterNodes){
            masterNodeIDs.add(tmp.getNodeId());
        }

        // Find online nodes and add them to the IDlist
        while (onlineNodeIDsIterator.hasNext()){
            Short tmp = onlineNodeIDsIterator.next();
            if (bootService.getNodeRole(tmp).toString().equals("peer") && !masterNodeIDs.contains(tmp))
                onlineWorkerNodeIDs.add(tmp);
        }

        //List<Integer> inputData = readData(filepath, seperator);
        List<Integer> inputData = null;
        try {
            inputData = readData(filepath, seperator);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Einlesen abgeschlossen");

        if (!normal) {

            long[] tmpSizeChunkId = new long[1];
            chunkService.create().create(bootService.getNodeID(), tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(writeOutSize, tmpSizeChunkId[0], 1, chunkService);

            nameService.register(tmpSizeChunkId[0], "WO");

            short GLOBAL_PEER_MINIMUM = (short) onlineWorkerNodeIDs.size();
            short GLOBAL_PEER_MAXIMUM = (short) onlineWorkerNodeIDs.size();

            // Get resources (number of available cores)
            ResourceTask resourceTask = new ResourceTask();
            TaskScript resourceScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "resource Task", resourceTask);
            masterSlaveComputeService.submitTaskScript(resourceScript);

            // Save data of the resource-Task
            int[] resources = new int[onlineWorkerNodeIDs.size()];
            int split = 0;
            for (int i = 0; i < onlineWorkerNodeIDs.size(); i++) {
                long chunkID = nameService.getChunkID("RC" + i, 100);
                resources[i] = getIntData(chunkID, chunkService);
                split += resources[i];
            }

            int sizeOfPartedData = inputData.size() / split;
            int overhead = inputData.size() % split;

            System.out.println("ressourcen: " + Arrays.toString(resources));
            System.out.println("Size of parted data: " + sizeOfPartedData);
            System.out.println("overhead: " + overhead);
            System.out.println("onlineWorkernodes: " + onlineWorkerNodeIDs.size());
            System.out.println("size of data: " + inputData.size());

            int[] addressChunkSize = new int[onlineWorkerNodeIDs.size()];

            // Write Chunk Ids to Matrix
            Iterator<Integer> dataIterator = inputData.iterator();
            for (int i = 0; i < onlineWorkerNodeIDs.size(); i++) {
                long[] tmpIds;
                long[] tmpAddressChunkId = new long[1];
                tmpSizeChunkId = new long[1];

                if (overhead > 0) {
                    if (overhead >= resources[i]) {
                        tmpIds = new long[resources[i] * (sizeOfPartedData + 1)];
                        overhead -= resources[i];
                    } else {
                        tmpIds = new long[sizeOfPartedData + overhead];
                    }
                } else {
                    tmpIds = new long[resources[i] * sizeOfPartedData];
                }

                short actualNodeID = getShortData(nameService.getChunkID("SID" + i, 100), chunkService);

                chunkService.create().create(actualNodeID, tmpIds, tmpIds.length, GLOBAL_CHUNK_SIZE);
                for (long tmpId : tmpIds)
                    editChunkInt(dataIterator.next(), tmpId, 1, chunkService);

                // Create, register AddressChunk
                chunkService.create().create(actualNodeID, tmpAddressChunkId, 1, GLOBAL_CHUNK_SIZE * tmpIds.length);
                editChunkLongArray(tmpIds, tmpAddressChunkId[0], chunkService);
                nameService.register(tmpAddressChunkId[0], "AC" + i);

                // Size of AddressChunk
                chunkService.create().create(actualNodeID, tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
                editChunkInt(tmpIds.length, tmpSizeChunkId[0], 1, chunkService);
                nameService.register(tmpSizeChunkId[0], "SAC" + i);

                addressChunkSize[i] = tmpIds.length;
            }

            System.out.println("CHUNKS EINGELESEN");

            // Create GoThrough-Parameter
            chunkService.create().create(bootService.getNodeID(), tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(2, tmpSizeChunkId[0], 1, chunkService);
            nameService.register(tmpSizeChunkId[0], "GT");

            chunkService.create().create(bootService.getNodeID(), tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(onlineWorkerNodeIDs.size(), tmpSizeChunkId[0], 1, chunkService);
            nameService.register(tmpSizeChunkId[0], "WN");

            SortTask sortTask = new SortTask();
            TaskScript sortScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Sort Task", sortTask);
            masterSlaveComputeService.submitTaskScript(sortScript);

            int cycle = onlineWorkerNodeIDs.size();

            MergeTask mergeTask = new MergeTask();
            TaskScript mergeScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Merge Task", mergeTask);

            UpdateGTTask updateGTTask = new UpdateGTTask();
            TaskScript updateGTTaskScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Update GT Task", updateGTTask);

            while (cycle > 1) {
                masterSlaveComputeService.submitTaskScript(mergeScript);
                masterSlaveComputeService.submitTaskScript(updateGTTaskScript);

                if (cycle % 2 == 0)
                    cycle /= 2;
                else
                    cycle = (int) Math.ceil((double) cycle / 2);
            }

            ExportTask exportTask = new ExportTask();
            TaskScript exportScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Export Task", exportTask);
            masterSlaveComputeService.submitTaskScript(exportScript);

        /*
        CleanUpTask cleanUpTask = new CleanUpTask();
        TaskScript cleanUpScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Cleanup Task", cleanUpTask);
        TaskScriptState cleanUpState = masterSlaveComputeService.submitTaskScript(cleanUpScript);
        */
        }
        else
            System.out.println("Normal");
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }

    /**
     * Edits the integervalue of a chunk
     *
     * @param value
     *          Integervalue to put
     * @param chunkId
     *          ChunkID of the editable chunk
     * @param size
     *          Size definines how many 64-BIT-integer should be written
     * @param chunkService
     *          Chunkservice to manage the operation
     */
    private void editChunkInt(int value, long chunkId, int size , ChunkService chunkService) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(size*GLOBAL_CHUNK_SIZE);
        byteBuffer.putInt(value);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }

    /**
     * Edits the longarray of a chunk
     *
     * @param array
     *          longarray to put
     * @param chunkId
     *          ChunkID of the editable chunk
     * @param chunkService
     *          Chunkservice to manage the operation
     */
    private void editChunkLongArray(long[] array, long chunkId, ChunkService chunkService) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(array.length*GLOBAL_CHUNK_SIZE);
        LongBuffer longBuffer = byteBuffer.asLongBuffer();
        longBuffer.put(array);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
    }

    /**
     * Get the integervalue of a chunk
     * @param chunkId
     *          ID of the chunk
     * @param chunkService
     *          Chunkservice to manage the operation
     * @return
     *      Integervalue of the chunk
     */
    private int getIntData(long chunkId, ChunkService chunkService){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();
        return ByteBuffer.wrap(byteData).getInt();
    }

    /**
     * Get the shortvalue of a chunk
     * @param chunkId
     *          ID of the chunk
     * @param chunkService
     *          Chunkservice to manage the operation
     * @return
     *      Shortvalue of the chunk
     */
    private short getShortData(long chunkId, ChunkService chunkService){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();
        return ByteBuffer.wrap(byteData).getShort();
    }

    /**
     * Reads the values of a file line by line into an arraylist
     * @param filepath
     *          Defines the filepath
     * @param seperator
     *          Defines the seperator in example ", "
     * @return
     *          Returns a list containing the values
     */
    private List<Integer> readData(String filepath, String seperator) throws IOException {
        List<Integer> list = new ArrayList<>();

        File file = new File(filepath);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String readLine = "";

        while ((readLine = bufferedReader.readLine()) != null){
            List<String> items = Arrays.asList(readLine.split(seperator));
            for (String s : items)
                list.add(Integer.valueOf(s));
        }

        return list;
    }
}