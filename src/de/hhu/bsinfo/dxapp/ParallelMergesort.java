package de.hhu.bsinfo.dxapp;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

public class ParallelMergesort {

        // CSVfile options
        private static String filepath = "ressources/Data_ZM.csv";
        private static String seperator = ", ";

        // Parallel or partialparallel?
        private static Boolean parallel=false;
        final static int cores =10;
        private static boolean powtwo = true;

        //Mergeebene
        private static int level = 1;

        private static int overhead = 0;
        private static int split;

        private static int[] array;
        private static int secondarray[];
        private static int listlength[];

        private static Thread threads[];


        public static void main(String args[]) throws InterruptedException, IOException {

                // Scan File
                Scanner sc = new Scanner(new File(filepath));
                sc.useDelimiter(seperator);
                List<Integer> list = new ArrayList<Integer>();

                while (sc.hasNext()) {
                        list.add(sc.nextInt());
                }
                sc.close();

                array = new int[list.size()];
                secondarray = new int[list.size()];
                for (int i = 0; i < list.size(); i++) {
                        array[i] = (int) list.get(i);
                }

                // Assume integer
                Boolean unevenflag = false;

                //Evaluate number of available cores
                //int cores = Runtime.getRuntime().availableProcessors();

                // Number of splits like for example available cores
                split = cores;

                // Length of one listpartition after split
                int length;

                // Check if integer and set overhead if necessary
                length = array.length / split;
                overhead = array.length % split;

                //
                System.out.println("Anzahl der gefundenen CPU-Kerne: " + cores);
                System.out.println("Die Länge des Arrays beträgt: " + array.length);
                // System.out.println("\nArray: " + Arrays.toString(array) + "\n");
                System.out.println("Der Overhead beträgt: " + overhead);
                System.out.println("Handelt es sich um eine Ganzzahl?: " + !unevenflag);
                System.out.println("Die Länge der Teillisten beträgt: " + length);
                System.out.println("Die Anzahl der Splittungen lautet: " + split);

                // Create one thread for every split
                threads = new Thread[split];
                listlength = new int[split];

                // Run normal mergesort
                for (int i = 0, j = 0; i < split; i++) {
                        if (j < overhead) {
                                threads[i] = new SortAlgorithm(array, i * length + j, length + 1, i);
                                listlength[i] = length + 1;
                                j++;
                        } else {
                                threads[i] = new SortAlgorithm(array, (i * length) + overhead, length, i);
                                listlength[i] = length;
                        }
                }

                // Run parallel/partial parallel Mergesort
                while (split > 1) {

                        double splitcheck = (double) split/2;
                        if (splitcheck %1 != 0){
                                powtwo = false;
                        }
                        split /= 2;
                        threads = new Thread[split];

                        for (int i = 0; i < split; i++) {

                                //  i represents one block (left AND right half)
                                System.out.println("\nSortiere Block " + i);
                                if (parallel)
                                        mergeParallel(i);
                                else
                                        mergePartialParallel(i);
                        }

                        // Update listlength
                        int[] tmp;
                        if (!powtwo){
                                tmp = new int[split+1];

                                for (int i = 0; i < tmp.length-1; i++) {
                                        tmp[i] = listlength[2 * i];
                                        if (2 * i + 1 < listlength.length) {
                                                tmp[i] += listlength[2 * i + 1];
                                        }
                                }
                                tmp[tmp.length-1] = listlength[listlength.length-1];
                                powtwo = true;
                                split++;

                        } else{
                                tmp = new int[split];
                                for (int i = 0; i < tmp.length; i++) {
                                        tmp[i] = listlength[2 * i];
                                        if (2 * i + 1 < listlength.length) {
                                                tmp[i] += listlength[2 * i + 1];
                                        }
                                }
                        }
                        listlength = tmp;
                        level++;
                }

                // Print final array
                //System.out.println("Finales Array: " + Arrays.toString(secondarray));

                if (parallel){
                        boolean check = true;
                        int count = 0;
                        int countnull =0;
                        int seconcount =0, secondnull=0;
                        for (int i = 0; i < secondarray.length - 1; i++) {
                                if (secondarray[i] > secondarray[i+1]) {
                                        check = false;
                                        System.out.println("i: " + i);
                                        System.out.println("Vorgänger: " + secondarray[i]);
                                        System.out.println("Nachfolger:  " + secondarray[i + 1]);
                                        count++;
                                }

                                if (secondarray[i] == 0)
                                        countnull++;
                        }
                        System.out.println("Ist das Array sortiert: " + check);
                        System.out.println("Fehlstellungen: " + count);
                        //System.out.println("Fehlstellungen array: " + seconcount);
                        System.out.println("Nullen: " + countnull);
                        //System.out.println("Nullen array: " + secondnull);
                        //System.out.println("secondArray: \n" + Arrays.toString(secondarray));
                        //System.out.println("Array: \n " + Arrays.toString(array));
                }
                else {
                        boolean check = true;
                        int count = 0;
                        int countnull =0;
                        int seconcount =0, secondnull=0;
                        for (int i = 0; i < array.length - 1; i++) {
                                if (array[i] > array[i+1]) {
                                        check = false;
                                        System.out.println("i: " + i);
                                        System.out.println("Vorgänger: " + array[i]);
                                        System.out.println("Nachfolger:  " + array[i + 1]);
                                        count++;
                                }

                                if (array[i] == 0)
                                        countnull++;
                        }

                        System.out.println("Ist das Array sortiert: " + check);
                        System.out.println("Fehlstellungen: " + count);
                        //System.out.println("Fehlstellungen array: " + seconcount);
                        System.out.println("Nullen: " + countnull);
                        //System.out.println("Array: \n" + Arrays.toString(array));
                        //System.out.println("Nullen array: " + secondnull);
                        //System.out.println("secondArray: \n " + Arrays.toString(secondarray));
                        //System.out.println("Array: \n " + Arrays.toString(array));
                }

        }



        private static void mergePartialParallel(int blockIndex){

        int start=0, breakpoint, end;

        for (int i=0; i<2*blockIndex;i++){
                start += listlength[i];
        }
        breakpoint = start + listlength[2*blockIndex];
        end = breakpoint + listlength[2*blockIndex+1] -1;

        //System.out.println(Arrays.toString(listlength));
        threads[blockIndex] = new MergeAlgorithm(array,start,end,breakpoint);

}

        private static void mergeParallel(int blockIndex) throws InterruptedException {

                // Ressources need to be an even number for creating threads!
                int ressources = (int) Math.pow(2, level);
                Thread threads[] = new Thread[ressources];

                // Endindex of the whole Block
                int endIndex = 0;
                for (int i=0; i<2*blockIndex+2; i++){
                        endIndex += listlength[i];
                }
                endIndex--;

                // Startindeces of the left half
                int leftStartIndex[] = new int[ressources];

                int sum=0;
                int partitial = (int) Math.floor(listlength[2*blockIndex]/((double) ressources));

                for (int i=0; i<blockIndex*2; i++){
                        sum += listlength[i];
                }
                for (int j=0; j<ressources; j++) {
                        leftStartIndex[j] = sum + j*partitial;
                }

                // Startindeces of the right half
                int rightStartIndex[] = new int[ressources];

                sum=0;
                for (int i=0; i<=2*blockIndex; i++){
                        sum += listlength[i];
                }

                rightStartIndex[0] = sum;

                for (int i=1; i<ressources; i++) {
                        rightStartIndex[i] = rightStartIndex[i-1]+2;
                        while (array[rightStartIndex[i]] <array[leftStartIndex[i]] && rightStartIndex[i] < endIndex){
                                rightStartIndex[i]++;
                        }
                }

                // Endindeces of the left half
                int leftEndIndex[] = new int[ressources];
                for (int j=0; j<ressources-1; j++) {
                        leftEndIndex[j] = leftStartIndex[j+1]-1;
                }
                leftEndIndex[ressources-1] = rightStartIndex[0]-1;

                // Endindeces of the right half
                int rightEndIndex[] = new int[ressources];
                for (int j=0; j<ressources-1; j++) {
                        rightEndIndex[j] = rightStartIndex[j+1]-1;
                }
                rightEndIndex[ressources-1] = endIndex;

                boolean loop = true;
                while(loop) {

                        loop = false;

                        for (int i=1; i<ressources; i++) {
                                        while (array[leftEndIndex[i-1]] > array[rightStartIndex[i]] && rightStartIndex[i] < rightEndIndex[i]-1) {
                                                loop = true;
                                                rightEndIndex[i-1]++;
                                                rightStartIndex[i]++;
                                        }
                        }

                        for (int i=0; i<ressources-1; i++){
                                        while (array[leftStartIndex[i+1]] < array[rightStartIndex[i]] && leftStartIndex[i+1] < leftEndIndex[i+1]-1){
                                                loop = true;
                                                leftEndIndex[i]++;
                                                leftStartIndex[i+1]++;
                                        }
                        }

                }

                /*
                System.out.println(Arrays.toString(leftStartIndex));
                System.out.println(Arrays.toString(leftEndIndex));
                System.out.println(Arrays.toString(rightStartIndex));
                System.out.println(Arrays.toString(rightEndIndex));
                System.out.println(Arrays.toString(listlength));
                */

                System.out.println("BLOCK: " + blockIndex);
                System.out.println(Arrays.toString(listlength));
                System.out.println(Arrays.toString(leftStartIndex));
                System.out.println(Arrays.toString(leftEndIndex));
                System.out.println(Arrays.toString(rightStartIndex));
                System.out.println(Arrays.toString(rightEndIndex));
                System.out.println(Arrays.toString(secondarray));
                System.out.println(Arrays.toString(array));


                for (int j=0; j<leftStartIndex.length; j++){
                        Boolean check = true;
                        int count = 0;

                        for (int g=leftStartIndex[j]; g<leftEndIndex[j]-1; g++){
                                if (array[g] > array[g+1]) {
                                        check = false;
                                        count++;
                                }
                        }

                        System.out.println("links sortiert: " + check);
                        System.out.println("Fehlstellungen: " + count);
                        System.out.println("i: " + j);

                        for (int g=rightStartIndex[j]; g<rightEndIndex[j]-1; g++){
                                if (array[g] > array[g+1]) {
                                        check = false;
                                        count++;
                                }
                        }
                        System.out.println("rechts sortiert: " + check);
                        System.out.println("Fehlstellungen: " + count);
                        System.out.println("i: " + j);
                }



                int shift = 0;

                for (int i=0; i<ressources/2; i++) {

                        int[] tmpleft = new int[leftEndIndex[2*i]-leftStartIndex[2*i] + rightEndIndex[2*i] - rightStartIndex[2*i]+2];
                        int[] tmpright = new int[leftEndIndex[2*i+1] - leftStartIndex[2*i+1] + rightEndIndex[2*i+1] - rightStartIndex[2*i+1]+2];

                        System.arraycopy(array,leftStartIndex[2*i],tmpleft,0,leftEndIndex[2*i]-leftStartIndex[2*i]+1);
                        System.arraycopy(array,rightStartIndex[2*i],tmpleft,leftEndIndex[2*i]-leftStartIndex[2*i]+1,rightEndIndex[2*i]-rightStartIndex[2*i]+1);
                        System.arraycopy(array,leftStartIndex[2*i+1],tmpright,0,leftEndIndex[2*i+1]-leftStartIndex[2*i+1]+1);
                        System.arraycopy(array,rightStartIndex[2*i+1],tmpright,leftEndIndex[2*i+1]-leftStartIndex[2*i+1]+1,rightEndIndex[2*i+1]-rightStartIndex[2*i+1]+1);

            /*
            System.out.println("TMPLEFT: " + Arrays.toString(tmpleft));
            System.out.println("TMPRIGHT: " + Arrays.toString(tmpright));
            */

                        threads[2*i] = new SuperMergeAlgorithm(tmpleft, leftEndIndex[2*i]-leftStartIndex[2*i]+1);
                        threads[2*i+1] = new SuperMergeAlgorithm(tmpright,leftEndIndex[2*i+1]-leftStartIndex[2*i+1]+1);
                        threads[2*i].start();
                        threads[2*i+1].start();
                        threads[2*i].join();
                        threads[2*i+1].join();


                        System.out.println(i);
                        System.out.println("left: " + Arrays.toString(tmpleft));
                        System.out.println("right: " + Arrays.toString(tmpright));

                        if (level > 1) {
                                System.arraycopy(tmpleft,0,secondarray,shift,tmpleft.length);

                                /*System.out.println(shift);
                                System.out.println(tmpleft.length);
                                System.out.println(tmpright.length);
                                System.out.println(Arrays.toString(tmpleft));
                                System.out.println(Arrays.toString(tmpright));
                                */

                                System.arraycopy(tmpright,0,secondarray,shift+tmpleft.length,tmpright.length);

                                shift += tmpleft.length+tmpright.length;
                        } else {
                                System.arraycopy(tmpleft,0,array,leftStartIndex[2*i],tmpleft.length);
                                System.arraycopy(tmpright,0,array,leftStartIndex[2*i]+tmpleft.length,tmpright.length);
                        }
                }
        }

}
