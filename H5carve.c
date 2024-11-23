#define _GNU_SOURCE
#include "hdf5.h"
#include "netcdf.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include "H5carve_helper_functions.h"
#include <string.h>

// Functions being interposed on include H5Fopen, H5Dread, and H5Oopen.
herr_t (*original_H5Dread)(hid_t, hid_t, hid_t, hid_t, hid_t, void*);
hid_t (*original_H5Fopen)(const char *, unsigned, hid_t);
hid_t (*original_H5Oopen)(hid_t, const char *, hid_t);
int (*original_nc_open)(const char *path, int omode, int *ncidp);
void (*original_H5_term_library)(void);

// Global variables to be used across function calls
char *use_carved;
// File IDs are set to -1 to check if they have been set
hid_t src_file_id = -1;
hid_t dest_file_id = -1;
hid_t original_file_id = -1;
char *is_netcdf4; // TODO: replace with an robust automatic check i.e. some kind of byte encoding 
char **files_opened = NULL;
int files_opened_current_size = 0;
FILE *log_ptr = NULL;
char *DEBUG = NULL;

int nc_open(const char *path, int omode, int *ncidp) {
	DEBUG = getenv("DEBUG");

	if (DEBUG) {
		if (log_ptr == NULL) {
			log_ptr = fopen("log", "w");
		}

		fprintf(log_ptr, "nc_open called %s %d %ls\n", path, omode, ncidp);
	}

	original_nc_open = dlsym(RTLD_NEXT, "nc_open");

	char *filename = malloc(strlen(path) + 1);
	strcpy(filename, path);

	char *carved_filename = get_carved_filename(filename, NULL, NULL);

	free(filename);
	
	// Fetch USE_CARVED environment variable
	use_carved = getenv("USE_CARVED");

	// Check if USE_CARVED environment variable has been set
	if (use_carved != NULL && strcmp(use_carved, "true") == 0) {
		return original_nc_open(carved_filename, omode, ncidp);
	}

	free(carved_filename);
	return original_nc_open(path, omode, ncidp);
}

/* 
	The primary function for accessing existing HDF5 files in the HDF5 library.
	Additional functionality added includes making an identical skeleton copy of the existing HDF5 file.
	The copy includes all groups, datasets, and attributes but excludes the contents of the datasets.
*/
hid_t H5Fopen (const char *filename, unsigned flags, hid_t fapl_id) {
	DEBUG = getenv("DEBUG");

	if (DEBUG) {
		if (log_ptr == NULL) {
			log_ptr = fopen("log", "w");
		}

		fprintf(log_ptr, "H5Fopen called %s %d %ld\n", filename, flags, fapl_id);
	}

	// Fetch original function
	original_H5Fopen = dlsym(RTLD_NEXT, "H5Fopen");

	// Create name of carved file
	char *carved_directory = getenv("CARVED_DIRECTORY");
	is_netcdf4 = getenv("NETCDF4");
	// Fetch USE_CARVED environment variable
	use_carved = getenv("USE_CARVED");

	char *carved_filename = get_carved_filename(filename, is_netcdf4, use_carved);

	// Check if USE_CARVED environment variable has been set
	if (use_carved != NULL && strcmp(use_carved, "true") == 0) {
		// Open original file for fallback machinery
		original_file_id = original_H5Fopen(filename, flags, fapl_id);

		if (original_file_id == H5I_INVALID_HID) {
			if (DEBUG)
				fprintf(log_ptr, "Error opening original file to be used as fallback %s %d %ld\n", filename, flags, fapl_id);
			return original_file_id;
		}

		// Open carved file for re-execution mode
		src_file_id = original_H5Fopen(carved_filename, flags, H5P_DEFAULT);

		if (src_file_id == H5I_INVALID_HID) {
			if (DEBUG)
				fprintf(log_ptr, "Error opening carved file %s %d\n", carved_filename, flags);
			return src_file_id;
		}

		if (DEBUG)
			fprintf(log_ptr, "CARVING DATASETS ACCESSED\n");

		return src_file_id;
	}

	// Open source file
	src_file_id = original_H5Fopen(filename, flags, fapl_id);

	if (src_file_id == H5I_INVALID_HID) {
		if (DEBUG)
			fprintf(log_ptr, "Error calling original H5Fopen function %s %d %ld\n", filename, flags, fapl_id);
		return H5I_INVALID_HID;
	}

	// Record files that have been opened for copying attributes if not already recorded
	if (!is_already_recorded(filename)) {
		if (files_opened == NULL) {
	        files_opened = malloc((files_opened_current_size + 1) * sizeof(char*));
	    } else {
	        files_opened = realloc(files_opened, (files_opened_current_size + 1) * sizeof(char*));
	    }
	    
	    files_opened[files_opened_current_size] = malloc(sizeof(char) * (strlen(filename) + 1));
	    strcpy(files_opened[files_opened_current_size], filename);

	    files_opened_current_size += 1;
	}

	// If carved file already exists or file was opened previously, skeleton file has already been created. Skip first phase.
	if (access(carved_filename, F_OK) == 0) {
		if (dest_file_id == -1) {
			dest_file_id = original_H5Fopen(carved_filename, H5F_ACC_RDWR, H5P_DEFAULT);

			if (dest_file_id == H5I_INVALID_HID) {
				if (DEBUG)
					fprintf(log_ptr, "Error calling original H5Fopen function when carved file already exists or file was opened previously %s\n", carved_filename);
				return H5I_INVALID_HID;
			}
		}

    	return src_file_id;
	}

	// Open root group of source file
	hid_t group_location_id = H5Gopen(src_file_id, "/", H5P_DEFAULT);

	if (group_location_id == H5I_INVALID_HID) {
		if (DEBUG)
			fprintf(log_ptr, "Error opening source file root group %ld\n", src_file_id);
		return H5I_INVALID_HID;
	}

	// Create destination (to-be carved) file and open the root group to duplicate the general structure of source file
	dest_file_id = H5Fcreate(carved_filename, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

	if (dest_file_id == H5I_INVALID_HID) {
		if (DEBUG)
			fprintf(log_ptr, "Error creating destination file %s\n", carved_filename);
		return H5I_INVALID_HID;
	}

	// Open root group of destination file
	hid_t destination_group_location_id = H5Gopen(dest_file_id, "/", H5P_DEFAULT);

	if (destination_group_location_id == H5I_INVALID_HID) {
		if (DEBUG)
			fprintf(log_ptr, "Error opening destination file root group %ld\n", dest_file_id);
		return H5I_INVALID_HID;
	}

	herr_t fallback_metadata_ret_val = create_fallback_metadata(filename, destination_group_location_id);

	if (fallback_metadata_ret_val < 0) {
		if (DEBUG)
			fprintf(log_ptr, "Error creating fallback metadata");
		return fallback_metadata_ret_val;
	}

    hid_t dataset_copy_check_attr_dataspace_id = H5Screate(H5S_SCALAR);

    hid_t dataset_copy_check_attr_id = H5Acreate2(destination_group_location_id, "WAS_DATASET_COPIED", H5T_NATIVE_HBOOL, dataset_copy_check_attr_dataspace_id, 
                               H5P_DEFAULT, H5P_DEFAULT);

    hbool_t is_empty = false;
    H5Awrite(dataset_copy_check_attr_id, H5T_NATIVE_HBOOL, &is_empty);

	if (DEBUG)
		fprintf(log_ptr, "CARVING GROUPS AND EMPTY DATASETS\n");

	// Start DFS to make a copy of the HDF5 file structure without populating contents i.e a "skeleton" 
	herr_t link_iterate_return_val = H5Literate2(group_location_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &destination_group_location_id);

	if (link_iterate_return_val < 0) {
		if (DEBUG)
			fprintf(log_ptr, "Link iteration failed %ld %ld\n", group_location_id, destination_group_location_id);
		return H5I_INVALID_HID;
	}
	
	if (DEBUG)
		fprintf(log_ptr, "CARVING DATASETS ACCESSED\n");

	return src_file_id;
}
/* 
	Reads a dataset from the HDF5 file into application memory.
    Additional functionality added includes monitoring which datasets have
    been accessed and populating the empty datasets in the carved file
    with the contents of the datasets accessed in the original file.
*/
herr_t H5Dread(hid_t dataset_id, hid_t	mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t	dxpl_id, void *buf)	{
	if (DEBUG)
		fprintf(log_ptr, "H5Dread called %ld %ld %ld %ld %ld\n", dataset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id);

    // Original function call
	original_H5Dread = dlsym(RTLD_NEXT, "H5Dread");
	herr_t return_val = original_H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf);

	// Fetch USE_CARVED environment variable
	use_carved = getenv("USE_CARVED");

	// Check if USE_CARVED environment variable has been set and return if it has (if it has been set, the carved file is queried by the above H5Dread call)
	if (use_carved != NULL && strcmp(use_carved, "true") == 0) {	
		return return_val;
	}

	// Fetch length of name of dataset
    int size_of_name_buffer = H5Iget_name(dataset_id, NULL, 0) + 1; // Preliminary call to fetch length of dataset name

    if (size_of_name_buffer == 0) {
    	if (DEBUG)
			fprintf(log_ptr, "Error fetching size of dataset name buffer %ld\n", dataset_id);
    	return -1;
    }

   	// Create and populate buffer for dataset name
    char *dataset_name = (char *)malloc(size_of_name_buffer);
    H5Iget_name(dataset_id, dataset_name, size_of_name_buffer); // Fill dataset_name buffer with the dataset name
    
    if (DEBUG)
		fprintf(log_ptr, "H5Dread called on %s dataset\n", dataset_name);

	hid_t dataset_file_id = H5Iget_file_id(dataset_id);
	int dataset_filename_len = H5Fget_name(dataset_file_id, NULL, 0) + 1;
	char *dataset_filename = (char *)malloc(dataset_filename_len);
	H5Fget_name(dataset_file_id, dataset_filename, dataset_filename_len);

	// Create name of carved file
	char *carved_directory = getenv("CARVED_DIRECTORY");
	is_netcdf4 = getenv("NETCDF4");
	// Fetch USE_CARVED environment variable
	use_carved = getenv("USE_CARVED");

	char *carved_filename = get_carved_filename(dataset_filename, is_netcdf4, use_carved);
	
	original_H5Fopen = dlsym(RTLD_NEXT, "H5Fopen");
	hid_t dataset_src_file = original_H5Fopen(dataset_filename, H5F_ACC_RDONLY, H5P_DEFAULT);
	hid_t dataset_carved_file = original_H5Fopen(carved_filename, H5F_ACC_RDWR, H5P_DEFAULT);
	free(dataset_filename);

	hid_t carved_empty_dataset = H5Dopen(dataset_carved_file, dataset_name, H5P_DEFAULT);

	// If the dataset being read does not exist in the carved file, copy the datatset object to the carved file
    if (!does_dataset_exist(carved_empty_dataset)) {
    	if (DEBUG)
			fprintf(log_ptr, "Deleting empty dataset from carved file %s\n", dataset_name);
    	hid_t link_deletion_ret_value = H5Ldelete(dataset_carved_file, dataset_name, H5P_DEFAULT);

    	if (link_deletion_ret_value < 0) {
    		if (DEBUG)
				fprintf(log_ptr, "Error deleting empty dataset object %ld %s\n", dataset_carved_file, dataset_name);
    		return link_deletion_ret_value;
    	}

    	if (DEBUG)
			fprintf(log_ptr, "Copying complete dataset to carved file %s\n", dataset_name);


		// Make copy of dataset in the destination file
		herr_t object_copy_return_val = H5Ocopy(dataset_src_file, dataset_name, dataset_carved_file, dataset_name, H5P_DEFAULT, H5P_DEFAULT);

		if (object_copy_return_val < 0) {
			if (DEBUG)
				fprintf(log_ptr, "Error copying object %ld %s %ld %s\n", dataset_src_file, dataset_name, dataset_carved_file, dataset_name);
			return object_copy_return_val;
		}

		hid_t dataset_copy_check_attr_id = H5Aopen(dataset_carved_file, "WAS_DATASET_COPIED", H5P_DEFAULT);

		if (dataset_copy_check_attr_id < 0) {
            if (DEBUG)
				fprintf(log_ptr, "Error opening dataset copy check attribute %ld\n", dataset_carved_file);
            return -1;
        }

		hbool_t dataset_copy_check_attr_val = true;

        herr_t dataset_copy_check_attr_write_status = H5Awrite(dataset_copy_check_attr_id, H5T_NATIVE_HBOOL, &dataset_copy_check_attr_val);
        
        if (dataset_copy_check_attr_write_status < 0) {
            if (DEBUG)
				fprintf(log_ptr, "Error writing value to dataset copy check ttribute %ld\n", dataset_copy_check_attr_id);
            return -1;
        }

		original_H5Oopen = dlsym(RTLD_NEXT, "H5Oopen");
		hid_t recent = original_H5Oopen(dataset_carved_file, dataset_name, H5P_DEFAULT);

		if (recent < 0) {
			if (DEBUG)
				fprintf(log_ptr, "Error opening object after H5Ocopy %ld %s\n", dataset_carved_file, dataset_name);
			return recent;
		}

		// Delete copied attributes (attributes may contain references to objects which would be invalid in carved file)
		herr_t attribute_iterate_return_val = H5Aiterate2(recent, H5_INDEX_NAME, H5_ITER_INC, NULL, delete_attributes, NULL);

		if (attribute_iterate_return_val < 0) {
			if (DEBUG)
				fprintf(log_ptr, "Attribute iteration failed\n");
			return attribute_iterate_return_val;
		}
	}

	H5Fclose(dataset_src_file);
	H5Fclose(dataset_carved_file);
	free(dataset_name);
	
	return return_val;
}

/*
	Opens an object within an HDF5 file.
	Additional functionality added includes monitoring if the datasets accessed in re-execution mode
	are present in the carved file or not. If not, diverts the control flow to access the dataset in 
	the original file instead of the carved file.
*/
hid_t H5Oopen(hid_t loc_id, const char *name, hid_t lapl_id) {
	if (DEBUG)
		fprintf(log_ptr, "H5Oopen called %ld %s %ld\n", loc_id, name, lapl_id);

	// Original function call
	original_H5Oopen = dlsym(RTLD_NEXT, "H5Oopen");
	hid_t return_val;
	// Fetch USE_CARVED environment variable
	use_carved = getenv("USE_CARVED");
	
	// If in repeat mode and object does not exist in carved file, bifurcate access to original file
	if ((use_carved != NULL && strcmp(use_carved, "true") == 0) && (!does_dataset_exist(H5Dopen(src_file_id, name, H5P_DEFAULT)))) {
		// Fetch length of name of dataset
	    int size_of_name_buffer = H5Iget_name(loc_id, NULL, 0) + 1; // Preliminary call to fetch length of dataset name

	    if (size_of_name_buffer == 0) {
	    	if (DEBUG)
				fprintf(log_ptr, "Error fetching size of dataset name buffer %ld\n", loc_id);
	    	return -1;
	    }

	   	// Create and populate buffer for dataset name
	    char *parent_object_name = (char *)malloc(size_of_name_buffer);
	    H5Iget_name(loc_id, parent_object_name, size_of_name_buffer); // Fill parent_object_name buffer with the dataset name

	    hid_t original_file_loc_id = original_H5Oopen(original_file_id, parent_object_name, lapl_id);
	    free(parent_object_name);

	    return_val = original_H5Oopen(original_file_loc_id, name, lapl_id);
	} else {
		return_val = original_H5Oopen(loc_id, name, lapl_id);

		if (return_val == H5I_INVALID_HID) {
			if (DEBUG)
				fprintf(log_ptr, "Error opening object %ld %s %ld\n", loc_id, name, lapl_id);
			return return_val;
		}
	}

	return return_val;
}

void H5_term_library(void) {
	if (DEBUG)
		fprintf(log_ptr, "H5_term_library called\n");

	// Fetch USE_CARVED environment variable
	use_carved = getenv("USE_CARVED");

	// Check if USE_CARVED environment variable has been set
	if (use_carved == NULL) {
		for (int i = 0; i < files_opened_current_size; i++) {
			src_file_id = original_H5Fopen(files_opened[i], H5F_ACC_RDONLY, H5P_DEFAULT);

			if (src_file_id == H5I_INVALID_HID) {
				if (DEBUG)
					fprintf(log_ptr, "Error reopening source file for copying attributes %s\n", files_opened[i]);
			}

			// Create name of carved file
			char *carved_directory = getenv("CARVED_DIRECTORY");
			is_netcdf4 = getenv("NETCDF4");
			// Fetch USE_CARVED environment variable
			use_carved = getenv("USE_CARVED");

			char *carved_filename = get_carved_filename(files_opened[i], is_netcdf4, use_carved);
	
			dest_file_id = original_H5Fopen(carved_filename, H5F_ACC_RDWR, H5P_DEFAULT);

			if (dest_file_id == H5I_INVALID_HID) {
				if (DEBUG)
					fprintf(log_ptr, "Error reopening dest file for copying attributes %s\n", carved_filename);
			}

			hid_t dataset_copy_check_attr_id = H5Aopen(dest_file_id, "WAS_DATASET_COPIED", H5P_DEFAULT);

			if (dataset_copy_check_attr_id < 0) {
	            if (DEBUG)
					fprintf(log_ptr, "Error opening dataset copy check attribute %ld\n", dest_file_id);
	            return -1;
	        }

	        hbool_t dataset_copy_check_attr_val;
	        
			herr_t dataset_copy_check_attr_return_val = H5Aread(dataset_copy_check_attr_id, H5T_NATIVE_HBOOL, &dataset_copy_check_attr_val);

			if (dataset_copy_check_attr_return_val < 0) {
				if (DEBUG)
	    			fprintf(log_ptr, "Error reading dataset copy check attribute data %ld\n", dataset_copy_check_attr_id);
				return dataset_copy_check_attr_return_val;
			}

			if (dataset_copy_check_attr_val == true) {
				printf("HIT\n");
				hid_t original_file_group_location_id = H5Gopen(src_file_id, "/", H5P_DEFAULT);	

				if (original_file_group_location_id == H5I_INVALID_HID) {
					if (DEBUG)
						fprintf(log_ptr, "Error opening source file root group %ld\n", src_file_id);
				}

				hid_t carved_file_group_location_id = H5Gopen(dest_file_id, "/", H5P_DEFAULT);

				if (carved_file_group_location_id == H5I_INVALID_HID) {
					if (DEBUG)
						fprintf(log_ptr, "Error opening carved file root group %ld\n", dest_file_id);
				}

				if (DEBUG)
					fprintf(log_ptr, "CARVING ATTRIBUTES\n");

				// Iterate over attributes at this level in the source file and make non-shallow copies in the destination file
				herr_t attribute_iterate_return_val = H5Aiterate2(original_file_group_location_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_object_attributes, &carved_file_group_location_id); // Iterate through each attribute and create a copy
				
				if (attribute_iterate_return_val < 0) {
					if (DEBUG)
						fprintf(log_ptr, "Attribute iteration failed\n");
				}

				// Start DFS to make a copy of attributes
				herr_t link_iterate_return_val = H5Literate2(original_file_group_location_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &carved_file_group_location_id);

				if (link_iterate_return_val < 0) {
					if (DEBUG)
						fprintf(log_ptr, "Link iteration failed\n");
				}

				dataset_copy_check_attr_val = false;

		        herr_t dataset_copy_check_attr_write_status = H5Awrite(dataset_copy_check_attr_id, H5T_NATIVE_HBOOL, &dataset_copy_check_attr_val);
		        if (dataset_copy_check_attr_write_status < 0) {
		            if (DEBUG)
						fprintf(log_ptr, "Error writing value to dataset copy check ttribute %ld\n", dataset_copy_check_attr_id);
		            return -1;
		        }
			}

			H5Fclose(src_file_id);
			H5Fclose(dest_file_id);
			free(files_opened[i]);
		}

		free(files_opened);
	}

	original_H5_term_library = dlsym(RTLD_NEXT, "H5_term_library");
	original_H5_term_library();
}