# Parallel Mergesort DXApp 

This plugin provides an application to sort a dataset of integers stored in a cvs file running on [DXRAM](https://github.com/hhu-bsinfo/dxram/).

# Prerequisites

At the moment you need to install my own Fork of [DXRAM](https://github.com/jusch196/dxram) to guarantee that all "tasks" are avaiable.


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
apprun de.hhu.bsinfo.dxapp.MergeSort file.csv 1 4 2
```
The first parameter give the filname. The file has to be located in the folder /dxram/dxapp/data.  
The second and the third gives us the number of minimum used peers i.e. 1 and maximum i.e. 4 .  
The third number sets the number of files to store the sorted data i.e. two files.

All three parameters has to be set!

# License
Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems.
Licensed under the [GNU General Public License](LICENSE.md).
