# HDF5 Data Carving

A library interpositioning based HDF5 data carving system that carves out the exact subset of data accessed by a program, at the granularity of [HDF5 objects](https://docs.hdfgroup.org/hdf5/develop/group___h5_o.html), while preserving metadata.

The carving mechanism is based on interposing on three HDF5 C API calls, namely H5Fopen, H5Dread, and H5Oopen. When an application makes a call to these functions, the calls are first directed to the carving system instead of the HDF5 library. The carving system implements additional functionality around the original behavior of these functions, aimed towards the objective of carving.

<p align="center">
<img alt="HDF5 Data Carving" src="https://github.com/raffayatiq/hdf5-data-carving/assets/58357644/2b66fff0-c0a4-43a4-8355-8864e00e2333">
</p>

The carving mechanism also implements a fallback machinery in case a program decides to access data outside of the subset accessed in the original execution. In this case, the control flow of the HDF5 file access diverts to the original file (either locally stored or remotely stored e.g. on Amazon S3), querying the data in the original file instead of the carved file.

The system operates in two modes. The first mode is the execution mode where the application is run with the original HDF5 file and the second mode is the repeat mode where the application is run with the carved HDF5 file.

### Execution mode

   In this mode, the system creates the carved HDF5 file in two phases.

   In the first phase triggered by the H5Fopen call, the system builds a skeleton of the HDF5 file, copying the attributes, groups, and dataset objects (without the dataset contents and with a NULL dataspace implying an empty dataset).
   
   <p align="center">
   <img alt="H5Fopen" src="https://github.com/raffayatiq/hdf5-data-carving/assets/58357644/5b644e40-b1ab-4bbc-a0d2-822c76c3d480">
   </p>

   In the second phase, the system monitors H5Dread calls. As each H5Dread call is made, the contents of the dataset that is queried by an H5Dread call are copied to the carved file. The output is a carved version of the original HDF5 file, suffixed with ".carved", containing only the subset of data accessed by the program.
   
   <p align="center">
   <img alt="H5Dread" src="https://github.com/raffayatiq/hdf5-data-carving/assets/58357644/6a233928-d327-4ef1-bd17-c7eda3dca9a2">
   </p>

### Repeat mode

   The program now accesses the carved file in place of the original file.

   The carved file is accessed if the data queried is in the subset of data accessed in the original execution. For data outside this subset, the fallback machinery is triggered and the original file is accessed instead. This diversion of control flow to the original file is achieved by interposing on H5Oopen and detecting if the datasets to be accessed in the carved file are empty or not.
   
   <p align="center">
   <img alt="H5Oopen" src="https://github.com/raffayatiq/hdf5-data-carving/assets/58357644/d597461e-c840-4fa0-bbb6-3e87b2ed52c6">
   </p>

## Setup
Recommended OS: Ubuntu 22.04+.

1. Download and extract the [HDF5 source code](https://www.hdfgroup.org/downloads/hdf5/source-code/).
2. In the source code directory, type the following commands to build the HDF5 source code:
   ```
   export HDF5_CARVE_LIBRARY=~/.local/libhdf5-carve
   libtoolize --force
   aclocal
   autoheader
   automake --force-missing --add-missing
   autoconf
   ./configure --prefix=$HDF5_CARVE_LIBRARY --enable-ros3-vfd
   make
   make install
   make check-install
   ```
3. Remove any installed h5py package (skip this step if no h5py package is installed or you intend to use a python virtual environment):
   ```
   pip3 uninstall h5py
   ```
4. Install h5py based on the newly built HDF5 source code:
   ```
   HDF5_DIR=$HDF5_CARVE_LIBRARY pip3 install --no-binary=h5py h5py
   ```
5. Install HDF5 development files and helper tools:
   ```
   sudo apt install libhdf5-dev hdf5-helpers hdf5-tools
   ```
6. Clone this repository:
   ```
   git clone https://github.com/data-carving/hdf5-data-carving.git
   ``` 
7. In the cloned repository directory, compile the carving script using the [h5cc compile script](https://docs.hdfgroup.org/archive/support/HDF5/Tutor/compile.html):
   ```
   HDF5_CFLAGS="-fPIC" h5cc -shlib -shared H5carve_helper_functions.c H5carve.c -o h5carve.so
   ```
8. Move the shared library file to the newly built HDF5 source code directory:
    ```
    mv h5carve.so $HDF5_CARVE_LIBRARY/lib
    ```
    
## Usage

### Execution mode
Before running a program set LD_PRELOAD to the path of the shared libraries, for example:
```
LD_PRELOAD="$HDF5_CARVE_LIBRARY/lib/h5carve.so $HDF5_CARVE_LIBRARY/lib/libhdf5.so" <execution command>
```

### Repeat mode
In addition to setting up LD_PRELOAD, set the USE_CARVED environment variable to 1:
```
LD_PRELOAD="$HDF5_CARVE_LIBRARY/lib/h5carve.so $HDF5_CARVE_LIBRARY/lib/libhdf5.so" USE_CARVED=true <execution command>
```
