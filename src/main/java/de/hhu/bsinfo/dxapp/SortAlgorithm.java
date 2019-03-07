package de.hhu.bsinfo        .dxapp;

class SortAlgorithm extends Thread {

private static int[] array;

/**
 * Sorts the array local on one thread based on John von Neumann mergesort
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
SortAlgorithm(int[] array, int start, int length){
        SortAlgorithm.array = array;
        try {
                join();
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
        sort(start, length);
}

/**
 * Merges two halfs stored next to each other in array
 * based on John von Neumann mergesort
 *
 * @param start
 * *              Startindex of the partial list
 * @param length
 *              Length of the two half's
 * @param breakpoint
 *              First index of the right half two separate both halfs
 */
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

/**
 * Sorts a partial list stored in the array
 * based on John von Neumann mergesort
 *
 * @param startIndex
 *              Startindex of the partial list
 * @param endIndex
 *              Endindex of the partial list
 */
private static void sort(int startIndex, int endIndex){
        if ( endIndex > 1) {
                sort(startIndex, (endIndex/2));
                sort(startIndex+(endIndex/2), (endIndex-(endIndex/2)));
                merge(startIndex, endIndex, endIndex/2);
        }

}
}
