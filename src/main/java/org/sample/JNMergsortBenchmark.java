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
public class JNMergsortBenchmark {

    // CSVfile options
    private static String sizeOfData = "ZM";
    private static String filepath = "ressources/Data_"+ sizeOfData +".csv";
    private static String seperator = ", ";

    private static int[] array;


    @Setup(Level.Invocation)
    @Measurement(iterations = 5, time = 5)
    public void setup() throws FileNotFoundException {

        Scanner sc;
        List<Integer> list;

        // read file
        if (array == null) {

            sc = new Scanner(new File(filepath));
            sc.useDelimiter(seperator);
            list = new ArrayList<>();

            while (sc.hasNext()) {
                list.add(sc.nextInt());
            }
            sc.close();

            // Write data to array[]
            array = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = list.get(i);
            }
            list.clear();

        } else {
            array = fisherYates(array);
        }
    }

    // John von Neumann Mergesort
    @Benchmark
    @Measurement(iterations = 5, time = 5)
    @Threads(1)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testMethod_NormalMergesort(){
        mergeSortJvN(array,0,array.length-1);
    }

    /**
     * Merges two array stored next to each other together
     * with local mergesortalgorithm based on John von Neumann
     *
     * @param array
     *          Array to sort
     * @param left
     *          Startindex of the partial array to sort
     * @param breakpoint
     *          Breakpoint to seperate the two half's
     * @param right
     *          Endindex of the partial array to sort
     */
    private static void mergeJvN(int[] array, int left, int breakpoint, int right) {
        int i, j, k;
        int n1 = breakpoint - left + 1;
        int n2 =  right - breakpoint;

        // create temp arrays
        int[] L = new int[n1];
        int[] R = new int[n2];

        // Copy data to temp arrays L[] and R[]
        for (i = 0; i < n1; i++)
            L[i] = array[left + i];
        for (j = 0; j < n2; j++)
            R[j] = array[breakpoint + 1+ j];

        // Merge the temp arrays back into arr[l..r]
        i = 0; // Initial index of first subarray
        j = 0; // Initial index of second subarray
        k = left; // Initial index of merged subarray
        while (i < n1 && j < n2)
        {
            if (L[i] <= R[j])
            {
                array[k] = L[i];
                i++;
            }
            else
            {
                array[k] = R[j];
                j++;
            }
            k++;
        }

        //Copy the remaining elements of L[], if there are any
        while (i < n1)
        {
            array[k] = L[i];
            i++;
            k++;
        }

        // Copy the remaining elements of R[], if there are any
        while (j < n2)
        {
            array[k] = R[j];
            j++;
            k++;
        }
    }

    /**
     * Standard local mergesortalgorithm based on John von Neumann
     * @param array
     *          Input array
     * @param left
     *          Startindex of the partial array to sort
     * @param right
     *          Endindex of the partial array to sort
     */
    private static void mergeSortJvN(int[] array, int left, int right) {
        if (left < right)
        {
            // Same as (l+r)/2, but avoids overflow for
            // large l and h
            int m = left+(right-left)/2;

            // Sort first and second halves
            mergeSortJvN(array, left, m);
            mergeSortJvN(array, m+1, right);

            mergeJvN(array, left, m, right);
        }
    }

    /**
     * Gets an array and shuffles the values based on the fisher-yates-algorithm
     * @param array
     *      Input array
     * @return
     *      Shuffled array
     */
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

    @TearDown
    public static void teardown(){}
}
