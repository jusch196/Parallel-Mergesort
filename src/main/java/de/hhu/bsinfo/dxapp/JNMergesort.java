package de.hhu.bsinfo.dxapp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JNMergesort {
        // CSVfile options
        private static String sizeOfData = "ZM";
        private static String filepath = "resources/Data_"+ sizeOfData +".csv";
        private static String seperator = ", ";

        private static int[] array;

        public static void main(String[] args) throws IOException {

            long startTime = System.nanoTime();

            List<Integer> inputData = readDataOtherwise(filepath, seperator);
            int[] array = new int[inputData.size()];
            for (int i=0; i<array.length;i++){
                array[i] = inputData.get(i);
            }
            mergeSortJvN(array, 0, array.length);

            long end = System.nanoTime();

            long running = end-startTime;

            System.out.println("Laufzeit: " + running );
        }

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

        private static List<Integer> readDataOtherwise(String filepath, String seperator) throws IOException {
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
