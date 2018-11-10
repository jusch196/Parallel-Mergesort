package de.hhu.bsinfo.dxapp;

import java.util.Arrays;

public class SortAlgorithm extends Thread {

private static int[] array;

public SortAlgorithm(int array[], int start, int length, int count){

        //this.array = array;
        this.array = array;

        System.out.println("Starte Thread " + count);

        try {
                join();
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
        sort(start, length);

}

private static void merge(int start, int length, int breakpoint) {

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

private static void sort(int start, int length){

        if ( length > 1) {

                sort(start, (length/2));
                sort(start+(length/2), (length-(length/2)));

                merge(start, length, length/2);
        }

        else{
                // do nothing
        }
}

}
