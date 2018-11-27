package de.hhu.bsinfo.dxapp;

public class SuperMergeAlgorithm extends Thread {

        public SuperMergeAlgorithm(int array[], int pivotIndex) {

                int length = array.length;

                if (length != 1) {

                        int indexLeft = 0;
                        int indexRight = pivotIndex;
                        int finalIndex = 0;

                        int[] finalArray = new int[length];

                        while (indexLeft < pivotIndex && indexRight < length) {

                                if (array[indexLeft] < array[indexRight]) {
                                        finalArray[finalIndex] = array[indexLeft];
                                        indexLeft++;
                                } else {
                                        finalArray[finalIndex] = array[indexRight];
                                        indexRight++;
                                }
                                finalIndex++;
                        }

                        while (indexLeft < pivotIndex) {
                                finalArray[finalIndex] = array[indexLeft];
                                indexLeft++;
                                finalIndex++;
                        }

                        while (indexRight < length) {
                                finalArray[finalIndex] = array[indexRight];
                                indexRight++;
                                finalIndex++;
                        }

                        System.arraycopy(finalArray, 0, array, 0, finalArray.length);
                }
        }
}
