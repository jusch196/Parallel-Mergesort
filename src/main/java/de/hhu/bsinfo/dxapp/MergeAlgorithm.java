package de.hhu.bsinfo.dxapp;

/**
 * Merges the adresseslists of the data local on one thread
 *
 * @author Julian Schacht, julian-morten.schacht@uni-duesseldorf.de, 15.03.2019
 */
class MergeAlgorithm extends Thread {
    /**
     * Merges the addresses of two partial lists
     * stored next to each other comparing their values
     * based on John von Neumann mergesortalgorithm
     *
     * @param start
     *          Startindex of the partial list
     * @param end
     *          Endindex of the partial list
     * @param breakpoint
     *          Breakpointindex to separate the two half's
     */
    MergeAlgorithm(int array[], int start, int end, int breakpoint){

        int[] finalArray = new int[end-start+1];

        int indexLeft = start;
        int indexRight = breakpoint;
        int finalIndex = 0;

        while (indexLeft < breakpoint && indexRight <= end) {

            if (array[indexLeft] < array[indexRight]) {
                finalArray[finalIndex] = array[indexLeft];
                indexLeft++;
            } else {
                finalArray[finalIndex] = array[indexRight];
                indexRight++;
            }

            finalIndex++;
        }

        while (indexLeft < breakpoint) {
            finalArray[finalIndex] = array[indexLeft];
            indexLeft++;
            finalIndex++;
        }

        while (indexRight <= end) {
            finalArray[finalIndex] = array[indexRight];
            indexRight++;
            finalIndex++;
        }

        System.arraycopy(finalArray, 0, array, start, finalIndex);
    }
}

