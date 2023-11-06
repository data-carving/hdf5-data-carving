# HDF5 Data Carving

A library interpositioning based HDF5 data carver that carves out the exact subset of data accessed by a program, at the granularity of [HDF5 objects](https://docs.hdfgroup.org/hdf5/develop/group___h5_o.html), while preserving metadata such as attributes. 
The carving mechanism also implements a fallback machinery in case a program decides to access data outside of the subset accessed in the original execution. In this case, the control flow of the HDF5 file access diverts to the original file (either locally stored or remotely stored e.g. on Amazon S3), querying the original data while the carved file acts as a cache.

## Setup
1. Download the [HDF5 source code](https://www.hdfgroup.org/downloads/hdf5/source-code/).
2. Extract the HDF5 source code.
3. In the source code directory, type the following commands to build the HDF5 source code with shared libraries:
   ```
   libtoolize --force
   aclocal
   autoheader
   automake --force-missing --add-missing
   autoconf
   ./configure --enable-shared --with-pic
   make
   sudo make install
   sudo make check-install
   ```
4. Delete any HDF5 folder in the /usr/local/ directory (optional):
   ```
   sudo rm -rf /usr/local/hdf5
   ```
5. Move newly built source code to /usr/local/:
   ```
   sudo mv hdf5 /usr/local/
   ```
6. Remove any installed h5py package (skip this step if no h5py package is installed or you intend to use a python virtual environment):
   ```
   pip uninstall h5py
   ```
7. Install h5py based on the newly built HDF5 source code:
   ```
   HDF5_DIR=/usr/local/hdf5 pip install --no-binary=h5py h5py
   ```
8. Clone this repository:
   ```
   git clone https://github.com/raffayatiq/hdf5-data-carving.git
   ```
9. In the cloned repository directory, compile the carving script using the h5cc compile script:
   ```
   HDF5_CFLAGS="-fPIC" h5cc -shlib -shared H5custom_module.c H5carve.c -o H5carve.so
   ```
10. Move the shared library file to the HDF5 folder in /usr/local/ directory:
    ```
    sudo mv H5carve.so /usr/local/hdf5
    ```
    
## Usage


### Execution

### Re-execution
   
