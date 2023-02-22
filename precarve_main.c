#define _GNU_SOURCE
#include "hdf5.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>

herr_t (*orig_H5Dread)(hid_t, hid_t, hid_t, hid_t, hid_t, void*);
hid_t (*orig_H5Fopen)(const char *, unsigned, hid_t);

int num_of_datasets_and_groups;
char **datasets_groups_accessed = NULL;
hid_t src_file_id;
hid_t dest_file_id;

herr_t copy_attributes(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	hid_t dest_attribute_id, attribute_data_type, attribute_data_space;
	hid_t *dest_object_id = (hid_t *)opdata;

	if (dest_object_id == NULL) {
		return 0;
	}

	// printf("%d\n", *dest_object_id);
	// Open the object
	hid_t src_attribute_id = H5Aopen(loc_id, name, H5P_DEFAULT);

	int size_of_name_buffer = H5Aget_name(src_attribute_id, NULL, 0) + 1;
	char *name_of_attribute = (char *)malloc(size_of_name_buffer);
	H5Aget_name(src_attribute_id, size_of_name_buffer, name_of_attribute);
	attribute_data_type = H5Aget_type(src_attribute_id);
	attribute_data_space = H5Aget_space(src_attribute_id);
	hsize_t attribute_data_size = H5Aget_storage_size(src_attribute_id);
	void* attribute_data_buffer = malloc(attribute_data_size);
	H5Aread(src_attribute_id, attribute_data_type, attribute_data_buffer);

	dest_attribute_id = H5Acreate1(*dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space, H5P_DEFAULT);
	// printf("Creating attribute: %s\n", name_of_attribute);
	H5Awrite(dest_attribute_id, attribute_data_type, attribute_data_buffer);

	H5Aclose(src_attribute_id);
	free(name_of_attribute);
	return 0;
}

// Copy structure of the HDF5 without copying contents. Essentially a DFS into the directed graph structure of an HDF5 file.
// In the directed graph structure, datasets are leaf nodes and groups are sub-trees
herr_t shallow_copy_object(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	// Open the object
	hid_t object_id = H5Oopen(loc_id, name, H5P_DEFAULT);

	hid_t *dest_parent_object_id = (hid_t *)opdata;
	
	if (dest_parent_object_id == NULL) {
		return 0;
	}

	// hid_t dest_object = H5Gopen(dest_file_id, "/foo", H5P_DEFAULT);

	// Fetch object type
	H5I_type_t object_type = H5Iget_type(object_id);
	int size_of_name_buffer = H5Iget_name(object_id, NULL, 0) + 1; // Preliminary call to fetch length of object name
    const char *object_name = (char *)malloc(size_of_name_buffer);
    H5Iget_name(object_id, object_name, size_of_name_buffer); // Fill dataset_name buffer with the object name
	
    // printf("Creating shallow copy of (full name) %s\n", object_name);
	
	// If object is a dataset, make shallow copy
	if (object_type == H5I_DATASET) {
		printf("Creating shallow copy of dataset %s\n", name);

		hid_t dset_id, data_type, data_space;
		dset_id = H5Dopen(src_file_id, object_name, H5P_DEFAULT);
		data_type = H5Dget_type(dset_id);
		data_space = H5Dget_space(dset_id);
		hid_t create_plist = H5Dget_create_plist(dset_id);	

		if (strcmp(name, "axis0") == 0 || strcmp(name, "axis1") == 0 || strcmp(name, "block0_items") == 0) {
			herr_t is_success = H5Ocopy(src_file_id, object_name, dest_file_id, object_name, H5P_DEFAULT, H5P_DEFAULT);

			if (is_success < 0) {
	    		printf("Copying %s failed in shallow_copy_object.\n", object_name);
	    	}
		} else {
			hid_t dest_dset_id = H5Dcreate(*dest_parent_object_id, object_name, data_type, data_space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

			if (dest_dset_id < 0) {
				printf("Error creating shallow copy of dataset %s. dest_parent_object_id is %d\n", name, *dest_parent_object_id);
			}

			H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &dest_dset_id); // Iterate through each attribute and create a copy
		}

		H5Dclose(dset_id);
	// If object is a group, recursively make shallow copy of the group
	} else if (object_type == H5I_GROUP) {
		printf("Creating shallow copy of group %s\n", name);
		// printf("Dest Root group %d\n", *dest_parent_object_id);
		// hid_t dest_group_id = H5Gcreate(*dest_parent_object_id, object_name, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
		hid_t dest_group_id = H5Gcreate1(*dest_parent_object_id, name, size_of_name_buffer);

		if (dest_group_id < 0) {
			printf("Error creating shallow copy of group %s. dest_parent_object_id is %d\n", name, *dest_parent_object_id);
		} else {
			printf("Success!\n");
		}		

		// dest_group_id = H5Gopen(dest_file_id, name, H5P_DEFAULT); // Opening the 
		// printf("Opened created group id: %d\n", opened_dest_group_id);

		// H5Literate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &dest_group_id);
		H5Literate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &dest_group_id);
		H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &dest_group_id); // Iterate through each attribute and create a copy
	}
	
	H5Oclose(object_id);
	return 0;
}

void count_objects_in_group(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	// Open the object
	hid_t group_id = H5Oopen(loc_id, name, H5P_DEFAULT);

	// Get count
	int count;
	H5Gget_num_objs(group_id, &count);

	// Sum
	num_of_datasets_and_groups += count;

	H5Oclose(group_id);
}

hid_t H5Fopen (const char * filename, unsigned flags, hid_t fapl_id) {
	// Original function call
	orig_H5Fopen = dlsym(RTLD_NEXT, "H5Fopen");
	src_file_id = orig_H5Fopen(filename, flags, fapl_id);
	hid_t g_loc_id = H5Gopen(src_file_id, "/", H5P_DEFAULT);

	// Get total object (datasets, groups etc.) count 
	H5Gget_num_objs(g_loc_id, &num_of_datasets_and_groups);
	H5Giterate(g_loc_id, "/", NULL, count_objects_in_group, NULL);

	// Initialize the buffer to record objects accessed
	datasets_groups_accessed = malloc(num_of_datasets_and_groups * sizeof(char*));
	for (int i = 0; i < num_of_datasets_and_groups; i++) {
		datasets_groups_accessed[i] = NULL;
	}

	// Create precarved file and open the root group so we can duplicate the general structure of our source file, excluding the contents
	dest_file_id = H5Fcreate("precarved.hdf5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
	hid_t dest_g_loc_id = H5Gopen(dest_file_id, "/", H5P_DEFAULT);

	// Start DFS to make a copy of the HDF5 structure without populating contents i.e a "skeleton" 
	// This is needed so that the Python script doesn't error out (because due to lazy loading, only the datasets whose elements the Python script accesses are called via H5Dread, and hence only those datasets are copied. Datasets that are accessed, but whose elements are not accessed in the Python script, will not be copied but will give error as they won't exist in the precarved file).
	H5Literate2(g_loc_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &dest_g_loc_id); // Iterate through each object and create shallow copy
	H5Aiterate2(g_loc_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &dest_g_loc_id); // Iterate through each object and create shallow copy
	
	H5Gclose(g_loc_id);
	H5Gclose(dest_g_loc_id);
	free(datasets_groups_accessed);
	return src_file_id;
}

herr_t H5Dread(hid_t dset_id, hid_t	mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t	dxpl_id, void *buf)	{
    int size_of_name_buffer = H5Iget_name(dset_id, NULL, 0) + 1; // Preliminary call to fetch length of dataset name
    char *dataset_name = (char *)malloc(size_of_name_buffer);
    H5Iget_name(dset_id, dataset_name, size_of_name_buffer); // Fill dataset_name buffer with the dataset name

    // printf("H5Dread HIT!\n");
    printf("Reading object %s\n", dataset_name);
    printf("num_of_datasets_and_groups %d\n", num_of_datasets_and_groups);

    for (int j = 0; j < num_of_datasets_and_groups; j++) {
    	// If dataset access has already been recorded, ignore and break
    	if (datasets_groups_accessed[j] != NULL && strcmp(datasets_groups_accessed[j], dataset_name) == 0) {
    		break;
    	}

    	// If dataset access has not been recorded, record and make copy of the dataset in the precarved file
    	if (datasets_groups_accessed[j] == NULL) {
    		datasets_groups_accessed[j] = malloc(size_of_name_buffer);
    		datasets_groups_accessed[j] = dataset_name;
    		printf("Dataset Name: %s\n", datasets_groups_accessed[j]);
    		
    		hid_t destination_dset_id, dest_data_space;
    		destination_dset_id = H5Dopen(dest_file_id, dataset_name, H5P_DEFAULT);
    		int rank = H5Sget_simple_extent_ndims(dest_data_space); // Retrives the number of dimensions of dataset. Returns -1 if dataset is empty, which in this case it is
			
			// If dataset is empty, delete empty copy so that we are able to make a new copy with contents populated, otherwise it errors out (citing the dataset already exists)
			if (rank < 0) {
				H5Ldelete(dest_file_id, dataset_name, H5P_DEFAULT);
			}

			// Make copy of dataset in precarved file
    		herr_t is_success = H5Ocopy(src_file_id, dataset_name, dest_file_id, dataset_name, H5P_DEFAULT, H5P_DEFAULT);

    		if (is_success < 0) {
    			printf("Copying %s failed.\n", dataset_name);
    		}

    		H5Dclose(destination_dset_id);
    		break;
    	}
    }

    // Original function call
	orig_H5Dread = dlsym(RTLD_NEXT, "H5Dread");
	herr_t return_val = orig_H5Dread(dset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf);
	

	return return_val;
}