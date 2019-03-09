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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * "Parallel Mergesort  DXRAM application "
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
public class MergeSort extends AbstractApplication {
    private static String filepath ="";
    private static String seperator =", ";

    private final static int GLOBAL_CHUNK_SIZE = 64;
    private static ChunkService chunkService;

    private static int writeOutSize = 1;
    private static boolean normal = false;
    private static boolean local = false;

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "MergeSort";
    }

    @Override
    public void main(final String[] p_args) throws IOException {

        // Get services
        BootService bootService = getService(BootService.class);
        chunkService = getService(ChunkService.class);
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
        } else throw new IllegalArgumentException("Error: Path ist missing");
        if (arguments.contains("--normal")){
            normal = true;
        }
        if (arguments.contains("--local")){
            local = true;
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

        List<Integer> inputData = null;
        try {
            inputData = readData(filepath, seperator);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (local && normal)
            throw new IllegalArgumentException("Can't set '--normal' and '--local'!");

        if (local){
            assert inputData != null;
            int[] array = localImport(inputData);

            int availableResources = Runtime.getRuntime().availableProcessors();
            Thread[] threads = new Thread[availableResources];
            int[] partialListLength = new int[availableResources];
            int lengthOfSplits = array.length/availableResources;
            int overhead = array.length % availableResources;

            // Run John von Neumann mergesort on each partial list
            for (int i = 0, j = 0; i < availableResources; i++) {
                if (j < overhead) {
                    threads[i] = new SortAlgorithm(array, i * lengthOfSplits + j, lengthOfSplits+1);
                    partialListLength[i] = lengthOfSplits + 1;
                    j++;
                } else {
                    threads[i] = new SortAlgorithm(array, (i * lengthOfSplits) + j, lengthOfSplits);
                    partialListLength[i] = lengthOfSplits;
                }
            }

            boolean powTwo = true;

            while (availableResources > 1){

                double splitCheck = (double) availableResources/2;

                if (splitCheck %1 != 0)
                    powTwo = false;

                availableResources /= 2;
                threads = new Thread[availableResources];

                for (int i = 0; i < availableResources; i++)
                    mergeNaive(i, partialListLength, array, threads);

                int[] tmp;

                // Update listlength
                if (!powTwo){
                    tmp = new int[availableResources+1];

                    for (int i = 0; i < tmp.length-1; i++) {
                        tmp[i] = partialListLength[2 * i];

                        if (2 * i + 1 < partialListLength.length)
                            tmp[i] += partialListLength[2 * i + 1];

                    }

                    tmp[tmp.length-1] = partialListLength[partialListLength.length-1];
                    powTwo = true;
                    availableResources++;

                }
                else{
                    tmp = new int[availableResources];

                    for (int i = 0; i < tmp.length; i++) {
                        tmp[i] = partialListLength[2 * i];

                        if (2 * i + 1 < partialListLength.length)
                            tmp[i] += partialListLength[2 * i + 1];

                    }
                }

                partialListLength = tmp;
            }

            localExport(writeOutSize, array);
            local = false;

        } else if (normal){

            assert inputData != null;
            int[] array = localImport(inputData);

            mergeSortJvN(array, 0, array.length-1);
            localExport(writeOutSize, array);

            normal = false;
        }
        else {

            short GLOBAL_PEER_MINIMUM = (short) onlineWorkerNodeIDs.size();
            short GLOBAL_PEER_MAXIMUM = (short) onlineWorkerNodeIDs.size();

            long[] tmpSizeChunkId = new long[1];

            chunkService.create().create(bootService.getNodeID(), tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(writeOutSize, tmpSizeChunkId[0]);
            nameService.register(tmpSizeChunkId[0], "WO");

            // Get resources (number of available cores)
            ResourceTask resourceTask = new ResourceTask();
            TaskScript resourceScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "resource Task", resourceTask);
            masterSlaveComputeService.submitTaskScript(resourceScript);

            // Save data of the resource-Task
            int[] resources = new int[onlineWorkerNodeIDs.size()];
            int split = 0;

            for (int i = 0; i < onlineWorkerNodeIDs.size(); i++) {
                long chunkID = nameService.getChunkID("RC" + i, 100);
                resources[i] = getIntData(chunkID);
                split += resources[i];
            }

            assert inputData != null;
            int sizeOfPartedData = inputData.size() / split;
            int overhead = inputData.size() % split;

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

                short actualNodeID = getShortData(nameService.getChunkID("SID" + i, 100));
                chunkService.create().create(actualNodeID, tmpIds, tmpIds.length, GLOBAL_CHUNK_SIZE);

                for (long tmpId : tmpIds)
                    editChunkInt(dataIterator.next(), tmpId);

                // Create, register AddressChunk
                chunkService.create().create(actualNodeID, tmpAddressChunkId, 1, GLOBAL_CHUNK_SIZE * tmpIds.length);
                editChunkLongArray(tmpIds, tmpAddressChunkId[0]);
                nameService.register(tmpAddressChunkId[0], "AC" + i);

                // Size of AddressChunk
                chunkService.create().create(actualNodeID, tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
                editChunkInt(tmpIds.length, tmpSizeChunkId[0]);
                nameService.register(tmpSizeChunkId[0], "SAC" + i);

            }

            chunkService.create().create(bootService.getNodeID(), tmpSizeChunkId, 1, GLOBAL_CHUNK_SIZE);
            editChunkInt(onlineWorkerNodeIDs.size(), tmpSizeChunkId[0]);
            nameService.register(tmpSizeChunkId[0], "WN");

            SortTask sortTask = new SortTask();
            TaskScript sortScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Sort Task", sortTask);
            masterSlaveComputeService.submitTaskScript(sortScript);

            int cycle = onlineWorkerNodeIDs.size();

            MergeTask mergeTask = new MergeTask();
            TaskScript mergeScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Merge Task", mergeTask);

            UpdateWNTask updateWNTask = new UpdateWNTask();
            TaskScript updateWNTaskScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Update WN Task", updateWNTask);

            while (cycle > 1) {
                masterSlaveComputeService.submitTaskScript(mergeScript);
                masterSlaveComputeService.submitTaskScript(updateWNTaskScript);

                if (cycle % 2 == 0)
                    cycle /= 2;

                else
                    cycle = (int) Math.ceil((double) cycle / 2);
            }

            ExportTask exportTask = new ExportTask();
            TaskScript exportScript = new TaskScript(GLOBAL_PEER_MINIMUM, GLOBAL_PEER_MAXIMUM, "Export Task", exportTask);
            masterSlaveComputeService.submitTaskScript(exportScript);
        }
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
     */
    private void editChunkInt(int value, long chunkId ) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(GLOBAL_CHUNK_SIZE);
        byteBuffer.putInt(value);
        ChunkByteArray chunkByteArray = new ChunkByteArray(chunkId, byteBuffer.array());
        chunkService.put().put(chunkByteArray);
        byteBuffer.clear();
    }

    /**
     * Edits the longarray of a chunk
     *
     * @param array
     *          longarray to put
     * @param chunkId
     *          ChunkID of the editable chunk
     */
    private void editChunkLongArray(long[] array, long chunkId) {
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
     * @return
     *      Integervalue of the chunk
     */
    private int getIntData(long chunkId){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();

        return ByteBuffer.wrap(byteData).getInt();
    }

    /**
     * Get the shortvalue of a chunk
     * @param chunkId
     *          ID of the chunk
     * @return
     *      Shortvalue of the chunk
     */
    private short getShortData(long chunkId){
        ChunkByteArray chunk = new ChunkByteArray(chunkId, GLOBAL_CHUNK_SIZE);
        chunkService.get().get(chunk);
        byte[] byteData = chunk.getData();

        return ByteBuffer.wrap(byteData).getShort();
    }

    /**
     * Reads the values of a file line by line into an arraylist
     * @param filepath
     *          Defines the filepath
     * @param separator
     *          Defines the separator in example ", "
     * @return
     *          Returns a list containing the values
     */
    private List<Integer> readData(String filepath, String separator) throws IOException {
        List<Integer> list = new ArrayList<>();

        File file = new File(filepath);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String readLine = "";

        while ((readLine = bufferedReader.readLine()) != null){
            List<String> items = Arrays.asList(readLine.split(separator));

            for (String s : items)
                list.add(Integer.valueOf(s));
        }

        return list;
    }

    /**
     * Merges two halfs of an array seperated through a breakpointindex based on John von Neumann algorithm
     *
     * @param array
     *          Array which contains both halfs
     * @param start
     *          Startindex of the left half
     * @param breakpoint
     *          Startindex of the right half
     * @param end
     *          Endindex of the right half
     */
    private static void mergeJvN(int[] array, int start, int breakpoint, int end) {
        int i, j, k;
        int sizeLeft = breakpoint - start + 1;
        int sizeRight =  end - breakpoint;

        int[] left = new int[sizeLeft];
        int[] right = new int[sizeRight];

        for (i = 0; i < sizeLeft; i++)
            left[i] = array[start + i];

        for (j = 0; j < sizeRight; j++)
            right[j] = array[breakpoint + 1+ j];

        i = 0;
        j = 0;
        k = start;

        while (i < sizeLeft && j < sizeRight) {
            if (left[i] <= right[j]) {
                array[k] = left[i];
                i++;
            }
            else {
                array[k] = right[j];
                j++;
            }

            k++;
        }

        while (i < sizeLeft) {
            array[k] = left[i];
            i++;
            k++;
        }

        while (j < sizeRight) {
            array[k] = right[j];
            j++;
            k++;
        }
    }

    /**
     * Sorting an array with mergesortalgorithm based on John von Neumann
     *
     * @param array
     *          Array to sort
     * @param start
     *          Startindex of the array
     * @param end
     *          Endindex of the array
     */
    private static void mergeSortJvN(int[] array, int start, int end) {
        if (start < end) {
            int m = start+(end-start)/2;

            mergeSortJvN(array, start, m);
            mergeSortJvN(array, m+1, end);

            mergeJvN(array, start, m, end);
        }
    }

    /**
     * Merges two half's of a block which indices are stored in listlength*
     * @param blockIndex
     *          Index of the block to sort
     */
    private static void mergeNaive(int blockIndex, int[] partialListLength, int[] array, Thread[] threads){

        int start=0, breakpoint, end;

        for (int i=0; i<2*blockIndex;i++){
            start += partialListLength[i];
        }

        breakpoint = start + partialListLength[2*blockIndex];
        end = breakpoint + partialListLength[2*blockIndex+1] -1;

        threads[blockIndex] = new MergeAlgorithm(array,start,end,breakpoint);
    }

    /**
     * Exports the sorted data into one or many csv-files
     *
     * @param writeOutSize
     *          Number of files to be written
     * @param array
     *          Array to export
     */
    private static void localExport(int writeOutSize, int[] array) throws IOException {

        int length = array.length;

        if (writeOutSize > 0){
            int writeOutIndex;

            for (int i = 0; i < writeOutSize - 1; i++) {
                String filename = "dxapp/data/sortedData" + i + ".csv";
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(filename));
                writeOutIndex = i * length / writeOutSize;

                for (int j = 0; j < length / writeOutSize; j++)
                    outputWriter.write(array[writeOutIndex + j] + ", ");

                outputWriter.flush();
                outputWriter.close();
            }

            int name = writeOutSize-1;
            String filename = "dxapp/data/sortedData"+name+".csv";
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter(filename));

            for (int i=(writeOutSize-1)*length/writeOutSize; i<length;i++)
                outputWriter.write(array[i] + ", ");

            outputWriter.flush();
            outputWriter.close();
        }

    }

    /**
     * Converts an List to an int-Array
     * @param inputData
     *          The List to convert
     * @return
     *          Returns the array
     */
    private static int[] localImport(List<Integer> inputData){
        int[] array = new int[inputData.size()];

        for (int i=0; i<array.length;i++){
            array[i] = inputData.get(i);
        }

        inputData.clear();

        return array;
    }
}
