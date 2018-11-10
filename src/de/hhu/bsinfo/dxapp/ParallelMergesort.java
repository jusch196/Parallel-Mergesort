package de.hhu.bsinfo.dxapp;

import java.util.Arrays;

public class ParallelMergesort {

//Mergeebene
private static int level = 1;

private static int overhead = 0;
private static int split;

private static int array[] = {23,20,17,13,2,1,10,12,4,3,6,18,5,9,7,8,10};
private static int listlength[];

public static void main(String args[]) throws InterruptedException {

        // Gehe grundsätzlich von Ganzzahl aus
        Boolean unevenflag = false;

        //Ermittle Anzahl der Kerne
        int cores = Runtime.getRuntime().availableProcessors();

        System.out.println("Anzahl der gefundenen CPU-Kerne: " + cores);
        System.out.println("Die Länge des Arrays beträgt: " + array.length);

        // Splittung z.B. Anzahl der Kerne
        split = cores;

        // Länge einer Teilliste nach Split
        int length;

        // Überprüfe ob es sich um eine Ganzzahl handelt und setze gegebenenfalls overhead
        length = array.length/split;
        overhead = array.length%split;


        System.out.println("\nArray: " + Arrays.toString(array) + "\n");
        System.out.println("Der Overhead beträgt: " + overhead);
        System.out.println("Handelt es sich um eine Ganzzahl?: " + !unevenflag);
        System.out.println("Die Länge der Teillisten beträgt: " + length);
        System.out.println("Die Anzahl der Splittungen lautet: " + split);


        // Lege Array für split viele Threads an
        Thread threads[] = new Thread[split];
        listlength = new int[split];

        // Führe normalen Mergesort aus
        for (int i=0,j=0; i<split; i++) {
                if (j<overhead) {
                        threads[i] = new SortAlgorithm(array, i * length + j, length + 1, i);
                        listlength[i] = length+1;
                        j++;
                }
                else{
                        threads[i] = new SortAlgorithm(array,(i*length)+overhead, length,i);
                        listlength[i] = length;
                }
        }

        System.out.println("\nAktueller Status des Arrays:");
        System.out.println(Arrays.toString(array));

        // Führe parallelen Merge aus
        while(split>1) {
                split /= 2;
                for (int i=0; i<split; i++) {
                        // Double the value of i, because i represents one block (left AND right half)
                        merge(2*i);
                }

                // Aktualisiere listlength
                int[] tmp = new int[(int) Math.ceil((double) listlength.length/2)];
                for (int i=0; i<tmp.length; i++) {
                        tmp[i] = listlength[2*i];

                        if (2*i+1 < listlength.length)
                                tmp[i] += listlength[2*i+1];
                }
                listlength = tmp;
                level++;
        }
}

public static void merge(int splitindex) throws InterruptedException {

        // ressources need to be an even number for creating threads!
        int ressources = level*2;
        Thread threads[] = new Thread[ressources];

        System.out.println("Threadslänge bzw. Ressourcen: " + threads.length);
        System.out.println("listlength" + Arrays.toString(listlength));

        // Startindeces of the left half
        int leftStartIndex[] = new int[ressources-1];
        leftStartIndex[0] = 0;
        //leftStartIndex[0] = (splitindex/2)*listlength[splitindex];

        for (int j=0;j<ressources-1;j++){
            // Überlege splitindex zu ändern da bei 4 ressourcen und splitindex 0 die leftstarts nicht gesetzt werden
                for (int i=0; i<j+splitindex; i++) {
                    leftStartIndex[j] +=  (int) Math.ceil((double)listlength[i]/(ressources-1));
                    // leftStartIndex[j] = j*listlength[i]/(ressources-1);
                }
        }

        System.out.println("Splitindex:" + splitindex);
        System.out.println("Split:" + split);

        // Startindeces of the right half
        int rightStartIndex[] = new int[ressources -1];
        System.arraycopy(leftStartIndex,0,rightStartIndex,0,ressources-1);

        System.out.println("LeftStartIndex: "+Arrays.toString(leftStartIndex));
        System.out.println("RightStartIndex: "+Arrays.toString(rightStartIndex));

        for (int j=0; j<ressources-1; j++){
            rightStartIndex[j] += listlength[splitindex/2];
        }

        // Endindeces of the left half
        int leftEndIndex[] = new int[ressources-1];
        for (int j=0;j<ressources-2;j++){
                leftEndIndex[j] = leftStartIndex[j+1]-1;
        }
        leftEndIndex[ressources-2] = rightStartIndex[0]-1;

    System.out.println("LeftStartIndex" + Arrays.toString(leftStartIndex));
    System.out.println("LeftEndIndex" + Arrays.toString(leftEndIndex));
    System.out.println("RightStartIndex" + Arrays.toString(rightStartIndex));


    // Endindeces of the right half
        int rightEndIndex[] = new int[ressources-1];

        // Korrekturen????!!!
        /*for (int j=0;j<ressources-2;j++){
            rightEndIndex[j] = rightStartIndex[j+1]-1;
            //+ listlength [splitindex/2+j]-2 + splitindex/2
        }
        rightEndIndex[ressources-2] = rightStartIndex[ressources-2] + listlength[splitindex/2]-2 + splitindex/2;
        */
        System.arraycopy(leftEndIndex,0,rightEndIndex,0,ressources-1);
        for (int j=0; j<ressources-1; j++){
            rightEndIndex[j] += listlength[splitindex/2]-1;
        }
        System.out.println("RightEndIndex: " + Arrays.toString(rightEndIndex));

        // Index of the Pivotelements in the left half
        int leftPivotIndex[] = new int[ressources-1];
        for (int i=0; i<ressources-1;i++){
            leftPivotIndex[i] = leftStartIndex[i] + (listlength[splitindex]*(i+1))/ressources;
        }

        // Index of the Pivotelements in the right half
        int rightPivotIndex[] = new int[ressources-1];

        // Initialize first Element and set the others
        rightPivotIndex[0] = rightStartIndex[0];
        while (array[rightPivotIndex[0]] < array[leftPivotIndex[0]] && (rightPivotIndex[0]<(rightStartIndex[0]+listlength[1]-1))) {
            rightPivotIndex[0]++;
        }
        for (int i=1; i<ressources-1;i++) {
            rightPivotIndex[i] = rightPivotIndex[i-1];
            while (array[rightPivotIndex[i]] < array[leftPivotIndex[i]] && rightPivotIndex[i] < rightPivotIndex[i-1]+listlength[2*i-1]-1) {
                rightPivotIndex[i]++;
            }
        }

        for (int i=0; i<level; i++) {

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

            threads[2*i] = new SuperMergeAlgorithm(tmpleft, leftPivotIndex[i]-leftStartIndex[i]+1);
            threads[2*i+1] = new SuperMergeAlgorithm(tmpright,leftEndIndex[i] - leftPivotIndex[i]);

            threads[2*i].join();
            threads[2*i+1].join();

            System.arraycopy(tmpleft,0,array,leftStartIndex[i],tmpleft.length);
            System.arraycopy(tmpright,0,array,leftStartIndex[i]+tmpleft.length,tmpright.length);

            System.out.println("Sortiert: \n" + Arrays.toString(array));
        }
}
}
