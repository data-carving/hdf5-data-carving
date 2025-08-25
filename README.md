# HDF5/netCDF4 Data Carving

This repository provides the source code and setup instructions for a data carving tool that interposes on [HDF5](https://www.hdfgroup.org/solutions/hdf5/) and [netCDF4](https://www.unidata.ucar.edu/software/netcdf/) library calls to extract only the data objects accessed by an application at runtime. The tool leverages [LD_PRELOAD](https://docs.oracle.com/cd/E19120-01/open.solaris/819-0690/chapter3-3/index.html) to intercept low-level functions such as file opens and dataset reads. It extends these functions with additional functionality to construct a minimal version of the original file by preserving only the required data objects, reducing storage footprint without sacrificing reproducibility or correctness. A fallback mechanism allows for dynamic retrieval of omitted data if subsequent executions access previously unaccessed data objects. This enables the creation of carved containers that maintain application correctness while significantly reducing file size and improving reproducibility in data-intensive workflows.

## Setup
Recommended OS: Ubuntu 22.04+.

1. Download and install the [zlib library](https://zlib.net/):
   ```
   export ZDIR=/usr/local
   tar -xzvf zlib-1.3.1.tar.gz && \
   cd zlib-1.3.1 && \
   ./configure && \
   make check && \
   make install
   ```
2. Download, extract, and build the [HDF5 source code](https://www.hdfgroup.org/downloads/hdf5/source-code/):
   ```
   export HDF5_CARVE_LIBRARY=/usr/local/hdf5
   libtoolize --force && \
   aclocal && \
   autoheader && \
   automake --force-missing --add-missing && \
   autoconf && \
   ./configure --prefix=$HDF5_CARVE_LIBRARY --enable-ros3-vfd --with-zlib=${ZDIR} --enable-hl && \
   make && \
   make install && \
   make check-install && \
   apt-get install -y libhdf5-dev hdf5-helpers hdf5-tools
   ```
3. Install the [h5py library](https://www.h5py.org/):
   ```
   HDF5_DIR=$HDF5_CARVE_LIBRARY pip3 install --no-binary=h5py git+https://github.com/h5py/h5py.git@master && \
   ```
   
4. Download, extract, and build the [netCDF4 source code](https://downloads.unidata.ucar.edu/netcdf/):
   ```
   libtoolize --force && \
   aclocal && \
   autoheader && \
   automake --force-missing --add-missing && \
   autoconf && \
   CPPFLAGS='-I/usr/local/hdf5/include -I/usr/local/include' LDFLAGS='-L/usr/local/hdf5/lib -L/usr/local/lib' ./configure --disable-dap --disable-nczarr && \
   make check && \
   make install
   ```
5. Install the [netCDF4 python library](https://unidata.github.io/netcdf4-python/):
   ```
   pip3 install netCDF4
   ```
6. Clone this repository:
   ```
   git clone https://github.com/data-carving/hdf5-data-carving.git
   ``` 
7. In the cloned repository directory, compile the carving script using the [h5cc compile script](https://docs.hdfgroup.org/archive/support/HDF5/Tutor/compile.html):
   ```
   HDF5_CFLAGS="-fPIC" h5cc -shlib -shared H5carve_helper_functions.c H5carve.c -o h5carve.so
   ```
8. Move the shared library file to the newly built HDF5 library:
    ```
    mv h5carve.so $HDF5_CARVE_LIBRARY/lib
    ```
    
## Usage

### Execution mode
Before running a program set LD_PRELOAD to the path of the shared libraries, for example:
```
LD_PRELOAD="$HDF5_CARVE_LIBRARY/lib/h5carve.so $HDF5_CARVE_LIBRARY/lib/libhdf5.so /usr/local/lib/libnetcdf.so" <execution command>
```

### Repeat mode
In addition to setting up LD_PRELOAD, set the USE_CARVED environment variable to true:
```
LD_PRELOAD="$HDF5_CARVE_LIBRARY/lib/h5carve.so $HDF5_CARVE_LIBRARY/lib/libhdf5.so /usr/local/lib/libnetcdf.so" USE_CARVED=true <execution command>
LD_PRELOAD="$HDF5_CARVE_LIBRARY/lib/h5carve.so $HDF5_CARVE_LIBRARY/lib/libhdf5.so /usr/local/lib/libnetcdf.so" USE_CARVED=true NETCDF4=true <execution command> (for netCDF4 files)
```
