#define _GNU_SOURCE
#include "hdf5.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include "H5carve_helper_functions.h"

// Functions being interposed on include H5Fopen, H5Dread, and H5Oopen.
herr_t (*original_H5Dread)(hid_t, hid_t, hid_t, hid_t, hid_t, void*);
hid_t (*original_H5Fopen)(const char *, unsigned, hid_t);
hid_t (*original_H5Oopen)(hid_t, const char *, hid_t);

// Global variables to be used across function calls
char *use_carved;
// File IDs are set to -1 to check if they have been set
hid_t src_file_id = -1;
hid_t dest_file_id = -1;
hid_t original_file_id = -1;

/* 
	The primary function for accessing existing HDF5 files in the HDF5 library.
	Additional functionality added includes making an identical skeleton copy of the existing HDF5 file.
	The copy includes all groups, datasets, and attributes but excludes the contents of the datasets.
*/
hid_t H5Fopen (const char *filename, unsigned flags, hid_t fapl_id) {
	// Fetch original function
	original_H5Fopen = dlsym(RTLD_NEXT, "H5Fopen");

	// Create name of carved file
	char *carved_directory = getenv("CARVED_DIRECTORY");
	
	char *filename_without_directory_separators = strrchr(filename, '/');
    
    if (filename_without_directory_separators != NULL) {
            filename_without_directory_separators = filename_without_directory_separators + 1;
    } else {
            filename_without_directory_separators = filename;
    }

	char carved_filename[(carved_directory == NULL ? strlen(filename) : strlen(carved_directory) + strlen(filename_without_directory_separators)) + 7 + 1];

	if (carved_directory == NULL) {
	    carved_filename[0] = '\0';
		strcat(carved_filename, filename);
		strcat(carved_filename, ".carved");	
	} else {
		carved_filename[0] = '\0';
		strcat(carved_filename, carved_directory);
		strcat(carved_filename, filename_without_directory_separators);
		strcat(carved_filename, ".carved");
	}
	
	// Fetch USE_CARVED environment variable
	use_carved = getenv("USE_CARVED");

	// Check if USE_CARVED environment variable has been set
	if (use_carved != NULL && strcmp(use_carved, "true") == 0) {
		// Open original file for fallback machinery
		original_file_id = original_H5Fopen(filename, flags, fapl_id);

		if (original_file_id == H5I_INVALID_HID) {
			// printf("Error opening original file to be used as fallback\n");
		}

		// Open carved file for re-execution mode
		src_file_id = original_H5Fopen(carved_filename, flags, H5P_DEFAULT);

		if (src_file_id == H5I_INVALID_HID) {
			printf("Error opening carved file\n");
		}

		return src_file_id;
	}

	// Open source file
	src_file_id = original_H5Fopen(filename, flags, fapl_id);

	if (src_file_id == H5I_INVALID_HID) {
		printf("Error calling original H5Fopen function\n");
		return H5I_INVALID_HID;
	}

	// If carved file already exists or file was opened previously, skeleton file has already been created. Skip first phase.
	if (access(carved_filename, F_OK) == 0) {
		if (dest_file_id == -1) {
			dest_file_id = original_H5Fopen(carved_filename, H5F_ACC_RDWR, H5P_DEFAULT);

			if (dest_file_id == H5I_INVALID_HID) {
				printf("Error calling original H5Fopen function when carved file already exists or file was opened previously\n");
				return H5I_INVALID_HID;
			}
		}

    	return src_file_id;
	}

	// Open root group of source file
	hid_t group_location_id = H5Gopen(src_file_id, "/", H5P_DEFAULT);

	if (group_location_id == H5I_INVALID_HID) {
		printf("Error opening source file root group\n");
		return H5I_INVALID_HID;
	}

	// Create destination (to-be carved) file and open the root group to duplicate the general structure of source file
	dest_file_id = H5Fcreate(carved_filename, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

	if (dest_file_id == H5I_INVALID_HID) {
		printf("Error creating destination file\n");
		return H5I_INVALID_HID;
	}

	// Open root group of destination file
	hid_t destination_group_location_id = H5Gopen(dest_file_id, "/", H5P_DEFAULT);

	if (destination_group_location_id == H5I_INVALID_HID) {
		printf("Error opening destination file root group\n");
		return H5I_INVALID_HID;
	}

	// Start DFS to make a copy of the HDF5 file structure without populating contents i.e a "skeleton" 
	herr_t link_iterate_return_val = H5Literate2(group_location_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &destination_group_location_id);

	if (link_iterate_return_val < 0) {
		printf("Link iteration failed\n");
		return H5I_INVALID_HID;
	}

	// Iterate over attributes at root level in the source file and make non-shallow copies in the destination file
	herr_t attribute_iterate_return_val = H5Aiterate2(group_location_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &destination_group_location_id);

	if (attribute_iterate_return_val < 0) {
		printf("Attribute iteration failed\n");
		return H5I_INVALID_HID;
	}
	
	return src_file_id;
}
/* 
	Reads a dataset from the HDF5 file into application memory.
    Additional functionality added includes monitoring which datasets have
    been accessed and populating the empty datasets in the carved file
    with the contents of the datasets accessed in the original file.
*/
herr_t H5Dread(hid_t dataset_id, hid_t	mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t	dxpl_id, void *buf)	{
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
    	printf("Error fetching size of dataset name buffer\n");
    	return -1;
    }

   	// Create and populate buffer for dataset name
    char *dataset_name = (char *)malloc(size_of_name_buffer);
    H5Iget_name(dataset_id, dataset_name, size_of_name_buffer); // Fill dataset_name buffer with the dataset name

	// Open dataset in the destination file
	hid_t destination_dataset_id;
	destination_dataset_id = H5Dopen(dest_file_id, dataset_name, H5P_DEFAULT);

	if (destination_dataset_id == H5I_INVALID_HID) {
		printf("Error opening dataset\n");
		return destination_dataset_id;
	}

	// Check if dataset is NULL in carved file. If it is, remove associated link and copy original dataset to carved file
	if (is_dataset_null(destination_dataset_id)) {
		// Delete empty copy so that we are able to make a new copy with contents populated
		herr_t link_deletion = H5Ldelete(dest_file_id, dataset_name, H5P_DEFAULT);

		if (link_deletion < 0) {
			printf("Link deletion failed");
		}

		// Make copy of dataset in the destination file
		herr_t object_copy_return_val = H5Ocopy(src_file_id, dataset_name, dest_file_id, dataset_name, H5P_DEFAULT, H5P_DEFAULT);

		if (object_copy_return_val < 0) {
			printf("Copying %s failed.\n", dataset_name);
			return object_copy_return_val;
		}
	}
	
	return return_val;
}

/*
	Opens an object within an HDF5 file.
	Additional functionality added includes monitoring if the datasets accessed in re-execution mode
	are present in the carved file or not. If not, diverts the control flow to access the dataset in 
	the original file instead of the carved file.
*/
hid_t H5Oopen(hid_t loc_id, const char *name, hid_t lapl_id)	 {
	// Original function call
	original_H5Oopen = dlsym(RTLD_NEXT, "H5Oopen");
	hid_t return_val = original_H5Oopen(loc_id, name, lapl_id);

	if (return_val == H5I_INVALID_HID) {
		printf("Error opening object\n");
		return return_val;
	}

	// Fetch type of object
	H5I_type_t object_type = H5Iget_type(return_val);

	if (object_type == H5I_BADID) {
		printf("Error fetching object type in interposed H5Oopen\n");
		return return_val;
	}

	// Only consider triggering fallback if the object is a dataset (since H5Oopen can also be used to open files, groups, and datatypes)
	if (object_type == H5I_DATASET) {
		// Trigger fallback if the dataset is NULL
		if (is_dataset_null(return_val)) {
			return_val = original_H5Oopen(original_file_id, name, lapl_id);
		}
	}

	return return_val;
}
