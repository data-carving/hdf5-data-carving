# HDF5 Data Carving

A library interpositioning based HDF5 data carving system that carves out the exact subset of data accessed by a program, at the granularity of [HDF5 objects](https://docs.hdfgroup.org/hdf5/develop/group___h5_o.html), while preserving metadata such as attributes. 

The carving mechanism is based on interposing on three HDF5 C API calls, namely H5Fopen, H5Dread, and H5Oopen. When an application makes a call to these functions, the calls are first directed to the carving system instead of the HDF5 library. The carving system implements additional functionality around the original behavior of these functions, aimed towards the objective of carving.

<p align="center">
<img alt="HDF5 Data Carving" src="https://lh3.googleusercontent.com/drive-viewer/AK7aPaDc4kGWh7ouN4KzqP4MKRkUEGAouKaPiYsxcycg6nTCxPUHPQtqVpviFV--j7MLyUpQLqPJVZsUrX42k6vj55NTnGh0mA=w1366-h664">
</p>

The carving mechanism also implements a fallback machinery in case a program decides to access data outside of the subset accessed in the original execution. In this case, the control flow of the HDF5 file access diverts to the original file (either locally stored or remotely stored e.g. on Amazon S3), querying the data in the original file instead of the carved file.

The system works in 2 modes:
1. Execution mode

   In this mode, the system creates the carved HDF5 file in two phases.
   <p align="center">
   <img alt="H5Fopen" src="https://lh3.googleusercontent.com/drive-viewer/AK7aPaAsrD7ZElEgi5Fi1A_bnPdu-SLljpZVafVDhxLlwYK14I0ZR9_ZPGJYV9BHCFPkPkFOwUL9ME_ddhHJDxBiM88vQqmENA=w1366-h664">
   </p>
   
   In the first phase triggered by the H5Fopen call, the system builds a skeleton of the HDF5 file, copying the attributes, groups, and dataset objects (without the dataset contents and with a NULL dataspace implying an empty dataset).
   <p align="center">
   <img alt="H5Dread" src="https://lh3.googleusercontent.com/drive-viewer/AK7aPaBuOTI_yVfmvItYn73lBhPUNo5HiuznBmrr5hurC212_-8U6SKuPY0WxbSAvdwpCdsopyw78085egPZXbWF7kjrP29G8A=w1366-h664">
   </p>

   In the second phase, the system monitors H5Dread calls. As each H5Dread call is made, the contents of the dataset that is queried by an H5Dread call are copied to the carved file. The output is a carved version of the original HDF5 file, suffixed with "_carved", containing only the subset of data accessed by the program.
3. Re-execution mode (set by the USE_CARVED environment variable)

   The program now accesses the carved file in place of the original file.
   <p align="center">
   <img alt="H5Oopen" src="https://lh3.googleusercontent.com/drive-viewer/AK7aPaCYXccmZ_fH0n7aP8J0sNm2NoM_q15HM0Wd9q6OJj0cIheGuLtXF54_p18D3MoXhgUwzedvzU5kOvxtqUs7EbgtqvdjSA=w1366-h664">
   </p>
   The carved file is accessed if the data queried is in the subset of data accessed in the original execution. For data outside this subset, the fallback machinery is triggered and the original file is accessed instead. This diversion of control flow to the original file is achieved by interposing on H5Oopen and detecting if the datasets to be accessed in the carved file are empty or not.

## Setup
1. Download and extract the [HDF5 source code](https://www.hdfgroup.org/downloads/hdf5/source-code/).
2. In the source code directory, type the following commands to build the HDF5 source code with shared libraries:
   ```
   libtoolize --force
   aclocal
   autoheader
   automake --force-missing --add-missing
   autoconf
   ./configure --enable-shared --with-pic --enable-ros3-vfd
   make
   sudo make install
   sudo make check-install
   ```
3. Delete any HDF5 folder in the /usr/local/ directory (optional):
   ```
   sudo rm -rf /usr/local/hdf5
   ```
4. Move newly built source code to /usr/local/:
   ```
   sudo mv hdf5 /usr/local/
   ```
5. Remove any installed h5py package (skip this step if no h5py package is installed or you intend to use a python virtual environment):
   ```
   pip uninstall h5py
   ```
6. Install h5py based on the newly built HDF5 source code:
   ```
   HDF5_DIR=/usr/local/hdf5 pip install --no-binary=h5py h5py
   ```
7. Clone this repository:
   ```
   git clone https://github.com/raffayatiq/hdf5-data-carving.git
   ```
8. In the cloned repository directory, compile the carving script using the [h5cc compile script](https://docs.hdfgroup.org/archive/support/HDF5/Tutor/compile.html):
   ```
   HDF5_CFLAGS="-fPIC" h5cc -shlib -shared H5custom_module.c H5carve.c -o h5carve.so
   ```
9. Move the shared library file to the HDF5 folder in /usr/local/ directory:
    ```
    sudo mv h5carve.so /usr/local/hdf5
    ```
    
## Usage

### Execution
Before running a program set LD_PRELOAD to the path of the shared libraries, for example:
```
LD_PRELOAD="/usr/local/hdf5/h5carve.so /usr/local/hdf5/lib/libhdf5.so" python script.py
```

### Re-execution
In addition to setting up LD_PRELOAD, set the USE_CARVED environment variable to 1:
```
LD_PRELOAD="/usr/local/hdf5/h5carve.so /usr/local/hdf5/lib/libhdf5.so" USE_CARVED=1 python script.py
```
