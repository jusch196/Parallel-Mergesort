package de.hhu.bsinfo.dxapp;

public class MergeAlgorithm extends Thread {

    public MergeAlgorithm (int array[], int start, int end, int breakpoint){

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

