/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sample;

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
public class BalancedMergesortBenchmark {

    // CSVfile options
    private static String sizeOfData = "ZT";
    private static String filepath = "ressources/Data_"+ sizeOfData +".csv";
    private static String seperator = ", ";

    private final static int GLOBAL_PARTITIAL_TEST_LENGTH = 2500000;
    private final static int OVERCUT =1/5;

    private static int[] array;
    private static int cores = 4;

    private static int[] leftQuarter;
    private static int[] rightQuarter;
    private static int[] leftAndRightQuarter;

    private static int[] leftHalf;
    private static int[] rightHalf;
    private static int[] leftAndRightHalf;
    private static int[][] balancedArray;

    // sorted Thread IDs
    private static int[] threadIDs;
    // Minima / Maxima
    static int[] minima;
    static int[] maxima;
    // Startindeces & Endindices
    static int[] begin;
    static int[] ending;


    @Setup(Level.Invocation)
    @Measurement(iterations = 5, time = 5)
    public void setup() throws FileNotFoundException {

        Scanner sc;
        List<Integer> list, rightlist, leftlist;
        String filename;

        // read file
        if (balancedArray == null){

            int[] listlength = {GLOBAL_PARTITIAL_TEST_LENGTH, GLOBAL_PARTITIAL_TEST_LENGTH, GLOBAL_PARTITIAL_TEST_LENGTH, GLOBAL_PARTITIAL_TEST_LENGTH};
            begin = new int[cores];
            ending = new int[cores];

            begin[0] = 0;
            for (int i=1; i<listlength.length;i++){
                begin[i] = begin[i-1]+listlength[i-1];
                ending[i-1] = begin[i]-1;
            }
            ending[cores-1] = begin[cores-1]+listlength[cores-1]-1;

            threadIDs = new int[cores];
            for (int i=0;i<cores;i++){
                threadIDs[i]=i;
            }

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

                balancedArray = new int[listlength.length][];
                for (int i=0; i<listlength.length;i++){
                    balancedArray[i] = new int[listlength[i]];
                }

                int preIndex =0;
                for (int i=0; i<listlength.length;i++){
                    if (listlength[i] >= 0)
                        System.arraycopy(array, preIndex, balancedArray[i], 0, listlength[i]);
                    preIndex += listlength[i];
                }

            }

            balancedArray = new int[listlength.length][];
            for (int i=0; i<listlength.length;i++){
                balancedArray[i] = new int[listlength[i]];
            }

            int preIndex =0;
            for (int i=0; i<listlength.length;i++){
                if (listlength[i] >= 0)
                    System.arraycopy(array, preIndex, balancedArray[i], 0, listlength[i]);
                preIndex += listlength[i];
            }

        }
        else {
            for (int i=0; i< balancedArray.length;i++)
                balancedArray[i] = fisherYates(balancedArray[i]);
        }

        if (leftQuarter == null){

            filename = "ressources/sorted/"+ sizeOfData +"_0.csv";
            sc = new Scanner(new File(filename));

            sc.useDelimiter(seperator);
            leftlist = new ArrayList<>();

            while (sc.hasNext()){
                leftlist.add(sc.nextInt());
            }
            sc.close();

            // Write data to array[]
            leftQuarter = new int[leftlist.size()];

            for (int i=0;i<leftlist.size();i++){
                leftQuarter[i] = leftlist.get(i);
            }

            leftlist.clear();
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
    public void testMethod_BalancedParallelMergesortSizeEquals4(){
        balancedMergesort(4,0);
    }

    @Benchmark
    @Measurement(iterations = 5, time = 5)
    @Threads(4)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void testMethodBalancedParallelMergesortMerge4Cores(){
        ArrayList<Integer> tmp = new ArrayList<>();
        balancedMerge(leftQuarter, leftQuarter, GLOBAL_PARTITIAL_TEST_LENGTH *OVERCUT, leftQuarter.length-1, GLOBAL_PARTITIAL_TEST_LENGTH *OVERCUT, rightQuarter.length-1, tmp);
        tmp.clear();
    }

    /**
     * Sorts threadIDs basing on sorting the minima of the assigned lists
     * and save their minima/maxima
     */
    private static void precalculateBalancedMergesort() {

        int[] threadIDs = {0, 1, 2, 3};
         minima = new int[4];
         maxima = new int[4];

        for (int i = 0; i < 4; i++) {
            minima[i] = balancedArray[i][0];
            maxima[i] = balancedArray[i][balancedArray[i].length - 1];
        }

        mergeSortBalanced(minima, 0, minima.length - 1, threadIDs);

        int[] tmpMaxima = new int[maxima.length];
        System.arraycopy(maxima, 0, tmpMaxima, 0, maxima.length);
        for (int i = 0; i < threadIDs.length; i++) {
            maxima[i] = tmpMaxima[threadIDs[i]];
        }
    }

    /**
     * Changing the threadIDs based on sorting the minima using the John von Neumann mergesortalgorithm
     * @param array
     *          Array storing the minima
     * @param leftIndex
     *          First index of the array
     * @param rightIndex
     *          Last index of the array
     * @param threadIDs
     *          Array storing the IDs of the threads
     */
    private static void mergeSortBalanced(int array[], int leftIndex, int rightIndex, int[] threadIDs) {
        if (leftIndex < rightIndex)
        {
            // Same as (l+r)/2, but avoids overflow for
            // large l and h
            int m = leftIndex+(rightIndex-leftIndex)/2;

            // Sort first and second halves
            mergeSortBalanced(array, leftIndex, m, threadIDs);
            mergeSortBalanced(array, m+1, rightIndex, threadIDs);

            mergeBalanced(array, leftIndex, m, rightIndex, threadIDs);
        }
    }

    /**
     * Changing the threadIDs based on sorting the minima using the John von Neumann mergesortalgorithm
     * @param array
     *          Array storing the minima
     * @param leftIndex
     *          First index of the array
     * @param breakpoint
     *          Breakpointindex to seperate the two halfs
     * @param rightIndex
     *          Last index of the array
     * @param threadIDs
     *          Array storing the IDs of the threads
     */
    private static void mergeBalanced(int array[], int leftIndex, int breakpoint, int rightIndex, int[] threadIDs) {
        int i, j, k;
        int n1 = breakpoint - leftIndex + 1;
        int n2 =  rightIndex - breakpoint;

        /* create temp arrays */
        int[] L = new int[n1];
        int[] R = new int[n2];
        int[] ids = new int[threadIDs.length];
        System.arraycopy(threadIDs, 0, ids, 0, threadIDs.length);

        /* Copy data to temp arrays L[] and R[] */
        for (i = 0; i < n1; i++){
            L[i] = array[leftIndex + i];
        }


        for (j = 0; j < n2; j++){
            R[j] = array[breakpoint + 1+ j];
        }


        /* Merge the temp arrays back into arr[l..r]*/
        i = 0; // Initial index of first subarray
        j = 0; // Initial index of second subarray
        k = leftIndex; // Initial index of merged subarray
        while (i < n1 && j < n2)
        {
            if (L[i] <= R[j])
            {
                array[k] = L[i];
                threadIDs[k] = ids[leftIndex+i];
                i++;
            }
            else
            {
                array[k] = R[j];
                threadIDs[k] = ids[breakpoint+1+j];
                j++;
            }
            k++;
        }

    /* Copy the remaining elements of L[], if there
       are any */
        while (i < n1)
        {
            array[k] = L[i];
            threadIDs[k] = ids[leftIndex+i];
            i++;
            k++;
        }

    /* Copy the remaining elements of R[], if there
       are any */
        while (j < n2)
        {
            array[k] = R[j];
            threadIDs[k] = ids[breakpoint+1+j];
            j++;
            k++;
        }
    }

    /**
     * Calculating the overcut of the partial lists
     *
     * @param firstID
     *          ID of the first core
     * @param secondID
     *          ID of the seocond core
     * @return
     *          Returns an array containing [0]=startindex of the overcut , [1]=endindex of the overcut
     */
    private static int[] getRange (int firstID, int secondID){
        int[] ret = new int[2];

        int leftIndex = balancedArray[firstID].length-1;
        int rightIndex = 0;

        int maxLeft = balancedArray[firstID][balancedArray[firstID].length-1];
        int smallRight = balancedArray[secondID][0];


        while (balancedArray[firstID][leftIndex] > smallRight && leftIndex >0){
            leftIndex--;
        }
        if (leftIndex != 0)
            leftIndex++;

        while (balancedArray[secondID][rightIndex] < maxLeft && rightIndex <balancedArray[secondID].length-1){
            rightIndex++;
        }
        if (rightIndex != balancedArray[secondID].length-1)
            rightIndex--;

        ret[0] = leftIndex;
        ret[1] = rightIndex;

        return ret;

    }

    /**
     * Gets index searched value in partial list
     * @param array
     *          Array to search value of x
     * @param leftIndex
     *          First index of partial list
     * @param rightIndex
     *          Last index of partial list
     * @param value
     *          Value to search
     * @return
     *      Index of value in partial list
     */
    private static int binsearch(int[] array, int leftIndex, int rightIndex, int value) {

        int m = -1;
        while (leftIndex<=rightIndex)
        {
            m=leftIndex+(rightIndex-leftIndex)/2;
            if (value<array[m])
                rightIndex=m-1;
            else if (value>array[m])
                leftIndex=m+1;
            else
                return m;
        }
        return m;
    }

    /**
     * Calculates overcuts of two partial lists
     *
     * @param sizeOfGroup
     *              	Current groupsize
     *
     * @param firstThreadID
     *          		ID of the first Thread to get position in threadIDs
     */
    private static void balancedMergesort(int sizeOfGroup, int firstThreadID) {
        precalculateBalancedMergesort();

        int[] beginOverlappLeftEnding = new int[sizeOfGroup/2];
        int[] endingOverlappRightStart = new int[sizeOfGroup/2];

        for (int i=0; i< sizeOfGroup/2; i++){

            int leftMax = maxima[firstThreadID+i];
            int rightMin = minima[firstThreadID+i+1];

            if (leftMax>rightMin){
                int[] tmp = getRange(firstThreadID+2*i, firstThreadID+2*i+1);
                beginOverlappLeftEnding[i] = tmp[0];
                endingOverlappRightStart[i] = tmp[1];
            } else {
                beginOverlappLeftEnding[i] = -1;
                endingOverlappRightStart[i] = -1;
            }

            int medianLeftIndex = balancedArray[firstThreadID+2*i].length/2;
            while (medianLeftIndex > 0 && balancedArray[firstThreadID + 2*i][medianLeftIndex] == balancedArray[firstThreadID + 2*i][medianLeftIndex+1])
                medianLeftIndex--;

            int splitRight = binsearch(balancedArray[firstThreadID + 2*i+1], 0, balancedArray[firstThreadID + 2*i+1].length-1, balancedArray[firstThreadID+2*i][medianLeftIndex]);
            while (splitRight< balancedArray[firstThreadID + 2*i+1].length-1 && balancedArray[firstThreadID + 2*i+1][splitRight] == balancedArray[firstThreadID + 2*i+1][splitRight+1])
                splitRight++;

            int[] left = new int[balancedArray[firstThreadID+2*i].length];
            int[] right = new int[balancedArray[firstThreadID+2*i+1].length];

            System.arraycopy(balancedArray[firstThreadID+2*i], 0, left,0,left.length);
            System.arraycopy(balancedArray[firstThreadID+2*i+1], 0, right,0,right.length);
        }
    }

    /**
     * Merges the overcuts basing on the balanced mergealgorithm and saves the result in an
     * Arraylist to overwrite the whole in the next step
     *
     * @param arrayLeft
     *          Left partial list
     * @param arrayRight
     *          Right partial list
     * @param leftStart
     *          First index of the left list
     * @param leftEnd
     *          Last index of the left list
     * @param rightStart
     *          First index of the right list
     * @param rightEnd
     *          Last index of the right list
     * @param list
     *          ArrayList<>() to return result
     */
    private static void balancedMerge(int[] arrayLeft, int[] arrayRight, int leftStart, int leftEnd, int rightStart, int rightEnd, ArrayList<Integer> list){
        int saveLeft = leftStart;

        int length = leftEnd-leftStart + rightEnd-rightStart+2;

        if (length > 1) {

            int finalIndex = 0;

            int[] finalArray = new int[length];

            while (leftStart < leftEnd && rightStart < rightEnd) {

                if (arrayLeft[leftStart] < arrayRight[rightStart]) {
                    finalArray[finalIndex] = arrayLeft[leftStart];
                    leftStart++;
                } else {
                    finalArray[finalIndex] = arrayRight[rightStart];
                    rightStart++;
                }
                finalIndex++;
            }

            while (leftStart < leftEnd) {
                finalArray[finalIndex] = arrayLeft[leftStart];
                leftStart++;
                finalIndex++;
            }

            while (rightStart < rightEnd) {
                finalArray[finalIndex] = arrayRight[rightStart];
                rightStart++;
                finalIndex++;
            }


            if (saveLeft > 0){
                for (int i=0; i<saveLeft;i++){
                    list.add(arrayLeft[i]);
                }
                for (int i:finalArray){
                    list.add(i);
                }

            } else if (saveLeft == 0 ){
                for (int i1 : finalArray) {
                    list.add(i1);
                }
                for (int i=rightEnd; i<arrayRight.length;i++){
                    list.add(arrayRight[i]);
                }
            }
        }
    }

    @TearDown
    public void teardown(){
    }

}
