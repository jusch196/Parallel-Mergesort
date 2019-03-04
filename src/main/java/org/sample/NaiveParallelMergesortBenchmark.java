package org.sample;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
public class NaiveParallelMergesortBenchmark {

    // CSVfile options
    private static String sizeOfData = "ZM";
    private static String filepath = "ressources/Data_"+ sizeOfData +".csv";
    private static String seperator = ", ";

    private final static int GLOBAL_PARTITIAL_TEST_LENGTH = 2500000;
    private final static int start=0;

    private static int[] array;

    private static int[] leftAndRightQuarter;
    private static int[] leftAndRightHalf;

    @Setup(Level.Invocation)
    @Measurement(iterations = 5, time = 5)
    public void setup() throws FileNotFoundException {

        Scanner sc;
        List<Integer> list, rightlist, leftlist;
        String filename;

        // read file
        if (array == null){
            sc = new Scanner(new File(filepath));
            sc.useDelimiter(seperator);
            list = new ArrayList<>();

            while (sc.hasNext()){
                list.add(sc.nextInt());
            }
            sc.close();

            // Write data to array[]
            array = new int[list.size()];
            for (int i=0;i<list.size();i++){
                array[i] = list.get(i);
            }
            list.clear();
        }
        else {
            array = fisherYates(array);
        }

        if (leftAndRightQuarter == null){
            filename = "ressources/sorted/"+ sizeOfData +"_0.csv";
            sc = new Scanner(new File(filename));

            sc.useDelimiter(seperator);
            leftlist = new ArrayList<>();

            while (sc.hasNext()){
                leftlist.add(sc.nextInt());
            }
            sc.close();


            filename = "ressources/sorted/"+ sizeOfData +"_1.csv";

            sc = new Scanner(new File(filename));
            sc.useDelimiter(seperator);
            rightlist = new ArrayList<>();
            while (sc.hasNext()){
                rightlist.add(sc.nextInt());
            }
            sc.close();

            leftAndRightQuarter = new int[leftlist.size()+rightlist.size()];
            for (int i=0; i<leftlist.size();i++){
                leftAndRightQuarter[i] = leftlist.get(i);
            }
            for (int i=0; i<rightlist.size();i++){
                leftAndRightQuarter[i + leftlist.size()] = rightlist.get(i);
            }
            leftlist.clear();
            rightlist.clear();

            filename = "ressources/sorted/"+ sizeOfData +"half_0.csv";
            sc = new Scanner(new File(filename));

            sc.useDelimiter(seperator);
            while (sc.hasNext()){
                leftlist.add(sc.nextInt());
            }
            sc.close();

            filename = "ressources/sorted/"+ sizeOfData +"half_1.csv";
            sc = new Scanner(new File(filename));
            sc.useDelimiter(seperator);

            while (sc.hasNext()){
                rightlist.add(sc.nextInt());
            }
            sc.close();


            leftAndRightHalf = new int[leftlist.size()+rightlist.size()];

            for (int i=0; i<leftlist.size();i++)
                leftAndRightHalf[i] = leftlist.get(i);

            for (int i=0;i<rightlist.size();i++)
                leftAndRightHalf[leftlist.size()+i] = rightlist.get(i);

            leftlist.clear();
            rightlist.clear();

        }

    }
    private static int[] fisherYates(int[] array) {
        int n = array.length;
        Random random = new Random();

        // Loop over array and shuffle values with Fisher–Yates algorithm
        for (int i = 0; i < array.length; i++) {
            int randomValue = i + random.nextInt(n - i);
            int randomElement = array[randomValue];
            array[randomValue] = array[i];
            array[i] = randomElement;
        }
        return array;
    }

    @Benchmark
    @Measurement(iterations = 5, time = 5)
    @Threads(4)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testMethod_partialParallelSort() {
        naiveSort(array,start, GLOBAL_PARTITIAL_TEST_LENGTH);
    }

    @Benchmark
    @Measurement(iterations = 5, time = 5)
    @Threads(2)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testMethod_partialParallelMergeTwoCores() {
        naiveMerge(leftAndRightQuarter, start, 2* GLOBAL_PARTITIAL_TEST_LENGTH, GLOBAL_PARTITIAL_TEST_LENGTH);
    }

    @Benchmark
    @Measurement(iterations = 5, time = 5)
    @Threads(1)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testMethod_partialParallelMergeOneCore() {
        naiveMerge(leftAndRightHalf, 0,4* GLOBAL_PARTITIAL_TEST_LENGTH,2* GLOBAL_PARTITIAL_TEST_LENGTH);
    }

    /**
     * Merges two array next to each other using unparallel mergesortalgorithm
     *
     * @param array
     *          Stores all data including all partial Lists
     * @param start
     *          Startindex of the first half
     * @param length
     *          Endindex of the second half
     * @param breakpoint
     *          First index of the right half which seperates both halfs.
     */
    private static void naiveMerge(int[] array, int start, int length, int breakpoint) {

        int[] finalArray = new int[length];

        int indexLeft = 0;
        int indexRight = 0;
        int finalIndex = 0;

        while (indexLeft < breakpoint && indexRight < length-breakpoint) {

            if (array[start+indexLeft] < array[start + breakpoint + indexRight]) {
                finalArray[finalIndex] = array[start + indexLeft];
                indexLeft++;
            } else {
                finalArray[finalIndex] = array[start + breakpoint + indexRight];
                indexRight++;
            }
            finalIndex++;
        }

        while (indexLeft < breakpoint) {
            finalArray[finalIndex] = array[start + indexLeft];
            indexLeft++;
            finalIndex++;
        }

        while (indexRight < length-breakpoint) {
            finalArray[finalIndex] = array[start +breakpoint + indexRight];
            indexRight++;
            finalIndex++;
        }

        System.arraycopy(finalArray, 0, array, start, finalIndex);
    }

    /**
     * Sorts array with one thread using unparallel mergesortalgorithm
     * @param array
     *          Stores all data
     * @param start
     *          Startindex of the partial array
     * @param length
     *          Endindex of the partial array
     */
    private static void naiveSort(int[] array, int start, int length){

        if ( length > 1) {

            naiveSort(array,start, (length/2));
            naiveSort(array,start+(length/2), (length-(length/2)));

            naiveMerge(array,start, length, length/2);
        }
    }
}
