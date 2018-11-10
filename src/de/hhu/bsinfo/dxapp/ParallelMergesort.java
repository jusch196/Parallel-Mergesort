package de.hhu.bsinfo.dxapp;

import java.util.Arrays;

public class ParallelMergesort {

//Mergeebene
private static int level = 1;

private static int overhead = 0;
private static int split;

private static int array[] = {23,20,17,13,2,1,10,12,4,3,6,18,5,9,7,8,10};
private static int secondarray[] = new int[array.length];
private static int listlength[];


public static void main(String args[]) throws InterruptedException {

        // Assume integer
        Boolean unevenflag = false;

        //Evaluate number of available cores
        int cores = Runtime.getRuntime().availableProcessors();

        System.out.println("Anzahl der gefundenen CPU-Kerne: " + cores);
        System.out.println("Die Länge des Arrays beträgt: " + array.length);

        // Number of splits like for example available cores
        split = cores;

        // Length of one listpartition after split
        int length;

        // Check if integer and set overhead if necessary
        length = array.length/split;
        overhead = array.length%split;


        System.out.println("\nArray: " + Arrays.toString(array) + "\n");
        System.out.println("Der Overhead beträgt: " + overhead);
        System.out.println("Handelt es sich um eine Ganzzahl?: " + !unevenflag);
        System.out.println("Die Länge der Teillisten beträgt: " + length);
        System.out.println("Die Anzahl der Splittungen lautet: " + split);


        // Create one thread for every split
        Thread threads[] = new Thread[split];
        listlength = new int[split];

        // Run normal mergesort
        for (int i=0,j=0; i<split; i++) {
                if (j<overhead) {
                        threads[i] = new SortAlgorithm(array, i * length + j, length + 1, i);
                        listlength[i] = length+1;
                        j++;
                } else{
                        threads[i] = new SortAlgorithm(array,(i*length)+overhead, length,i);
                        listlength[i] = length;
                }
        }

        System.out.println("\nAktueller Status des Arrays:");
        System.out.println(Arrays.toString(array));

        // Run parallel Mergesort
        while(split>1) {
                split /= 2;
                for (int i=0; i<split; i++) {
                        // Double the value of i, because i represents one block (left AND right half)
                        merge(2*i);
                }

                // Update listlength
                int[] tmp = new int[(int) Math.ceil((double) listlength.length/2)];
                for (int i=0; i<tmp.length; i++) {
                        tmp[i] = listlength[2*i];

                        if (2*i+1 < listlength.length) {
                                tmp[i] += listlength[2*i+1];
                        }
                }
                listlength = tmp;
                level++;
        }
        System.out.println("Finales Array: " + Arrays.toString(array));
}

private static void merge(int splitindex) throws InterruptedException {

        // Ressources need to be an even number for creating threads!
        int ressources = level*2;
        Thread threads[] = new Thread[ressources];

        System.out.println("Threadslänge bzw. Ressourcen: " + threads.length);
        System.out.println("listlength" + Arrays.toString(listlength));

        // Startindeces of the left half
        int leftStartIndex[] = new int[ressources-1];
        leftStartIndex[0] = 0;
        for (int j=0; j<ressources-1; j++) {
                for (int i=0; i<j+splitindex; i++) {
                        leftStartIndex[j] +=  (int) Math.ceil((double)listlength[i]/(ressources-1));
                }
        }

        System.out.println("Splitindex:" + splitindex);
        System.out.println("Split:" + split);

        // Startindeces of the right half
        int rightStartIndex[] = new int[ressources -1];
        System.arraycopy(leftStartIndex,0,rightStartIndex,0,ressources-1);
        for (int j=0; j<ressources-1; j++) {
                rightStartIndex[j] += listlength[splitindex/2];
        }

        // Endindeces of the left half
        int leftEndIndex[] = new int[ressources-1];
        for (int j=0; j<ressources-2; j++) {
                leftEndIndex[j] = leftStartIndex[j+1]-1;
        }
        leftEndIndex[ressources-2] = rightStartIndex[0]-1;

        System.out.println("LeftStartIndex" + Arrays.toString(leftStartIndex));
        System.out.println("LeftEndIndex" + Arrays.toString(leftEndIndex));
        System.out.println("RightStartIndex" + Arrays.toString(rightStartIndex));

        // Endindeces of the right half
        int rightEndIndex[] = new int[ressources-1];
        for (int j=0; j<ressources-2; j++) {
                rightEndIndex[j] += rightStartIndex[j+1]-1;
        }
        rightEndIndex[ressources-2] = leftEndIndex[ressources-2] + listlength[splitindex/2]-1+splitindex/2;
        System.out.println("RightEndIndex: " + Arrays.toString(rightEndIndex));

        // Index of the Pivotelements in the left half
        int leftPivotIndex[] = new int[ressources-1];
        for (int i=0; i<ressources-1; i++) {
                leftPivotIndex[i] = leftStartIndex[i] + (listlength[splitindex/2]/((ressources-1)*2));
        }

        // Index of the Pivotelements in the right half
        int rightPivotIndex[] = new int[ressources-1];
        for (int i=0; i<ressources-1; i++) {
                rightPivotIndex[i] = rightStartIndex[i];
                while (array[rightPivotIndex[i]] < array[leftPivotIndex[i]] && rightPivotIndex[i] < rightEndIndex[i]) {
                        rightPivotIndex[i]++;
                }
        }
        for (int i=0; i<ressources-1; i++) {

                System.out.println("LEVEL: " + level);

                int[] tmpleft = new int[leftPivotIndex[i]-leftStartIndex[i] + rightPivotIndex[i] - rightStartIndex[i]+2];
                int[] tmpright = new int[leftEndIndex[i] - leftPivotIndex[i] + rightEndIndex[i]-rightPivotIndex[i]];

                if (leftPivotIndex[i]-leftStartIndex[i]+1 > 0)
                        System.arraycopy(array,leftStartIndex[i],tmpleft,0,leftPivotIndex[i]-leftStartIndex[i]+1);
                if (rightPivotIndex[i]-rightStartIndex[i]+1 > 0)
                        System.arraycopy(array,rightStartIndex[i],tmpleft,leftPivotIndex[i]-leftStartIndex[i]+1,rightPivotIndex[i]-rightStartIndex[i]+1);

                if (leftEndIndex[i]-leftPivotIndex[i] > 0)
                        System.arraycopy(array,leftPivotIndex[i]+1,tmpright,0,leftEndIndex[i]-leftPivotIndex[i]);
                if (rightEndIndex[i]-rightPivotIndex[i] > 0)
                        System.arraycopy(array,rightPivotIndex[i]+1,tmpright,leftEndIndex[i]-leftPivotIndex[i],rightEndIndex[i]-rightPivotIndex[i]);

                System.out.println("TMPLEFT: " + Arrays.toString(tmpleft));
                System.out.println("TMPRIGHT: " + Arrays.toString(tmpright));

                threads[2*i] = new SuperMergeAlgorithm(tmpleft, leftPivotIndex[i]-leftStartIndex[i]+1);
                threads[2*i+1] = new SuperMergeAlgorithm(tmpright,leftEndIndex[i] - leftPivotIndex[i]);

                threads[2*i].join();
                threads[2*i+1].join();

                if (level > 1) {
                        System.arraycopy(tmpleft,0,secondarray,leftStartIndex[i],tmpleft.length);
                        System.arraycopy(tmpright,0,secondarray,leftStartIndex[i]+tmpleft.length,tmpright.length);

                        System.out.println("Secondarray: \n" + Arrays.toString(secondarray));
                } else {
                        System.arraycopy(tmpleft,0,array,leftStartIndex[i],tmpleft.length);
                        System.arraycopy(tmpright,0,array,leftStartIndex[i]+tmpleft.length,tmpright.length);

                        System.out.println("Array: \n" + Arrays.toString(array));
                }
        }
}
}
