# Parallel Mergesort DXApp 

This plugin provides an application to sort a dataset of integers stored in a cvs file running on [DXRAM](https://github.com/hhu-bsinfo/dxram/).

# Prerequisites

At the moment you need to install my own Fork of [DXRAM](https://github.com/jusch196/dxram) to guarantee that all "tasks" are avaiable and store it next to the Parallel-Mergesort folder.


# Compiling

To compile an application, simply run the build.sh script from the root of the repository:

```
./build.sh 
```

# Deployment and running an application

How to setup DXRAM and run an application is found [here](https://github.com/hhu-bsinfo/dxram).

# First steps 

After the setup you can run the mergesort-application with:
```
apprun de.hhu.bsinfo.dxapp.MergeSort −−path /dxapp/data/file.csv −−out 1 −−normal
```
The --path parameter gives the path to the file. The file can be located in the folder /dxram/dxapp/data.  
The --out  parameter sets the number of files to store the sorted data i.e. two files.
The --normal parameter instructs the application to sort the data localy with the JvN-Mergesort.
The --local parameter instructs the application to sort the data localy with the naive parallelization of the mergesortalgorithm.

The first two parameters has to be set, the other two are optional.

# License
Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems.
Licensed under the [GNU General Public License](LICENSE.md).
