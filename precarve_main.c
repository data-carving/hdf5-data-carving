#define _GNU_SOURCE
#include "hdf5.h"
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <string.h>

herr_t (*original_H5Dread)(hid_t, hid_t, hid_t, hid_t, hid_t, void*);
hid_t (*original_H5Fopen)(const char *, unsigned, hid_t);

int number_of_objects;
char **datasets_accessed = NULL;
hid_t src_file_id;
hid_t dest_file_id;
char *use_precarved;

herr_t copy_attributes(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	hid_t dest_attribute_id, attribute_data_type, attribute_data_space;
	hid_t *dest_object_id = (hid_t *)opdata;

	if (dest_object_id == NULL) {
		return -1;
	}

	// Open the attribute
	hid_t src_attribute_id = H5Aopen(loc_id, name, H5P_DEFAULT);

	if (src_attribute_id < 0) {
		printf("Error opening attribute\n");
		return src_attribute_id ;
	}

	// Fetch length of name of attribute
	int size_of_name_buffer = H5Aget_name(src_attribute_id, NULL, 0) + 1;

	if (size_of_name_buffer < 0) {
		printf("Error fetching attribute name\n");
		return size_of_name_buffer;
	}

	// Create and populate buffer for attribute name
	char *name_of_attribute = (char *)malloc(size_of_name_buffer);
	H5Aget_name(src_attribute_id, size_of_name_buffer, name_of_attribute);

	// Fetch data type of attribute
	attribute_data_type = H5Aget_type(src_attribute_id);

	if (attribute_data_type == H5I_INVALID_HID) {
		printf("Error fetching attribute data type\n");
		return attribute_data_type;
	}

	// Fetch data space of attribute
	attribute_data_space = H5Aget_space(src_attribute_id);

	if (attribute_data_space < 0) {
		printf("Error fetching attribute data space\n");
		return attribute_data_space;
	}

	// Fetch data size of attribute
	hsize_t attribute_data_size = H5Aget_storage_size(src_attribute_id);

	// Create and populate buffer for attribute data
	void* attribute_data_buffer = malloc(attribute_data_size);
	herr_t read_return_val = H5Aread(src_attribute_id, attribute_data_type, attribute_data_buffer);

	if (read_return_val < 0) {
		printf("Error reading attribute data\n");
		return read_return_val;
	}

	// Create attribute in destination file
	dest_attribute_id = H5Acreate1(*dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space, H5P_DEFAULT);
	
	if (dest_attribute_id < 0) {
		printf("Error creating attribute\n");
		return dest_attribute_id;
	}

	// Write attribute data to the newly created attribute in destination file
	herr_t write_return_val = H5Awrite(dest_attribute_id, attribute_data_type, attribute_data_buffer);

	if (write_return_val < 0) {
		printf("Error writing attribute\n");
		return write_return_val;
	}

	free(name_of_attribute);
	return 0;
}

// Copy structure of the HDF5 without copying contents. Essentially a DFS into the directed graph structure of an HDF5 file.
// In the directed graph structure, datasets are leaf nodes and groups are sub-trees
herr_t shallow_copy_object(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	// Open the object
	hid_t object_id = H5Oopen(loc_id, name, H5P_DEFAULT);

	if (object_id < 0) {
		printf("Error opening object\n");
		return object_id;
	}

	// Fetch id of parent object
	hid_t *dest_parent_object_id = (hid_t *)opdata;
	
	if (dest_parent_object_id == NULL) {
		printf("Error due to NULL destination file parent object\n");
		return -1;
	}

	// Fetch object type
	H5I_type_t object_type = H5Iget_type(object_id);

	if (object_type == H5I_BADID) {
		printf("Error fetching type of identifier\n");
		return object_type;
	}

	// Fetch length of name of object
	int size_of_name_buffer = H5Iget_name(object_id, NULL, 0) + 1; // Preliminary call to fetch length of object name

	if (size_of_name_buffer == 0) {
		printf("Error fetching size of name\n");
		return -1;
	}

	// Create and populate buffer for name of object
    const char *object_name = (char *)malloc(size_of_name_buffer);
    H5Iget_name(object_id, object_name, size_of_name_buffer); // Fill dataset_name buffer with the object name
	
	// If object is a dataset, make shallow copy of dataset and terminate
	if (object_type == H5I_DATASET) {
		hid_t dataset_id, data_type, data_space;

		// Open the dataset
		dataset_id = H5Dopen(src_file_id, object_name, H5P_DEFAULT);

		if (dataset_id == H5I_INVALID_HID) {
			printf("Error opening dataset\n");
			return dataset_id;
		}

		// Fetch data type of dataset
		data_type = H5Dget_type(dataset_id);

		if (data_type == H5I_INVALID_HID) {
			printf("Error fetching type of dataset\n");
			return data_type;
		}

		// Create null dataspace for shallow copy
		data_space = H5Screate(H5S_NULL);

		if (data_space == H5I_INVALID_HID) {
			printf("Error fetching data space of dataset\n");
			return data_space;
		}

		// If dataset is "axis0", "axis1", or "block0_items", make deep copy
		if (strcmp(name, "axis0") == 0 || strcmp(name, "axis1") == 0 || strcmp(name, "block0_items") == 0) {
			herr_t object_copy_return_val = H5Ocopy(src_file_id, object_name, dest_file_id, object_name, H5P_DEFAULT, H5P_DEFAULT);

			if (object_copy_return_val < 0) {
	    		printf("Copying %s failed in shallow_copy_object.\n", object_name);
	    		return object_copy_return_val;
	    	}
	    // Otherwise, make shallow copy of the dataset
		} else {
			// Create dataset in destination file
			hid_t dest_dataset_id = H5Dcreate(*dest_parent_object_id, object_name, data_type, data_space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

			if (dest_dataset_id < 0) {
				printf("Error creating shallow copy of dataset %s. dest_parent_object_id is %d\n", name, *dest_parent_object_id);
				return dest_dataset_id;
			}

			// Iterate over attributes at this level in the source file and make non-shallow copies in the destination file
			herr_t attribute_iterate_return_val = H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &dest_dataset_id); // Iterate through each attribute and create a copy

			if (attribute_iterate_return_val < 0) {
				printf("Attribute iteration failed\n");
				return attribute_iterate_return_val;
			}
		}
	// If object is a group, make shallow copy of the group and recursively go down the tree
	} else if (object_type == H5I_GROUP) {
		// Create group in destination file
		hid_t dest_group_id = H5Gcreate1(*dest_parent_object_id, name, size_of_name_buffer);

		if (dest_group_id < 0) {
			printf("Error creating shallow copy of group %s. dest_parent_object_id is %d\n", name, *dest_parent_object_id);
			return dest_group_id;
		}

		// Iterate over objects at this level in the source file, and make shallow copes in the destination file
		herr_t link_iterate_return_val = H5Literate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &dest_group_id);

		if (link_iterate_return_val < 0) {
			printf("Link iteration failed\n");
			return link_iterate_return_val;
		}

		// Iterate over attributes at this level in the source file and make non-shallow copies in the destination file
		herr_t attribute_iterate_return_val = H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &dest_group_id); // Iterate through each attribute and create a copy
		
		if (attribute_iterate_return_val < 0) {
			printf("Attribute iteration failed\n");
			return attribute_iterate_return_val;
		}		
	}
	
	return 0;
}

void count_objects_in_group(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	// Open the object
	hid_t group_id = H5Oopen(loc_id, name, H5P_DEFAULT);

	if (group_id < 0) {
		printf("Error opening object\n");
		return group_id;
	}

	// Get count of objects at this level in the source file
	int count;
	herr_t get_num_objs_return_val = H5Gget_num_objs(group_id, &count);

	if (get_num_objs_return_val < 0) {
		printf("Error fetching object count\n");
		return get_num_objs_return_val;
	}

	// Add to the total count
	number_of_objects += count;
}

hid_t H5Fopen (const char * filename, unsigned flags, hid_t fapl_id) {
	// Fetch original function
	original_H5Fopen = dlsym(RTLD_NEXT, "H5Fopen");

	// Create name of the precarved file
	int filename_absolute_path_length = strlen(filename);
	char filename_copy[filename_absolute_path_length];
	strcpy(filename_copy, filename);

	char *filename_without_directory_separators = strrchr(filename_copy, '/');
	
	if (filename_without_directory_separators == NULL) {
		filename_without_directory_separators = filename_copy;
	} else {
		filename_without_directory_separators += 1; // add 1 to remove the starting '/'
	}

	char *filename_without_extension = strtok(filename_without_directory_separators, ".");
	char *filename_extension = strtok(NULL, ".");
	char precarved_suffix[] = "_carved.";
	int filename_length = strlen(filename_without_extension);
	int suffix_length = strlen(precarved_suffix);
	char *precarved_filename = malloc(sizeof(char) * (filename_length + suffix_length + 1));
	strcpy(precarved_filename, filename_without_extension);
	strcat(precarved_filename, precarved_suffix);
	strcat(precarved_filename, filename_extension);

	// Fetch USE_PRECARVED environment variable
	use_precarved = getenv("USE_PRECARVED");

	// Check if USE_PRECARVED environment variable has been set
	if (use_precarved != NULL && strcmp(use_precarved, "1") == 0) {
		// Open precarved file
		src_file_id = original_H5Fopen(precarved_filename, flags, fapl_id);

		if (src_file_id == H5I_INVALID_HID) {
			printf("Error opening precarved file\n");
		}

		return src_file_id;
	}

	// Open source file
	src_file_id = original_H5Fopen(filename, flags, fapl_id);

	if (src_file_id == H5I_INVALID_HID) {
		printf("Error calling original H5Fopen function\n");
		return H5I_INVALID_HID;
	}

	// Open root group of source file
	hid_t group_location_id = H5Gopen(src_file_id, "/", H5P_DEFAULT);

	if (group_location_id == H5I_INVALID_HID) {
		printf("Error opening source file root group\n");
		return H5I_INVALID_HID;
	}

	// Get total object count in source file
	herr_t get_num_objs_return_val = H5Gget_num_objs(group_location_id, &number_of_objects);

	if (get_num_objs_return_val < 0) {
		printf("Error fetching object count\n");
		return H5I_INVALID_HID;
	}	

	// Iterate over group at root level in the source file, and count objects under that group
	herr_t group_iterate_return_val = H5Giterate(group_location_id, "/", NULL, count_objects_in_group, NULL);

	if (group_iterate_return_val < 0) {
		printf("Group iteration failed\n");
		return H5I_INVALID_HID;
	}

	// Initialize the buffer to record datasets accessed
	datasets_accessed = malloc(number_of_objects * sizeof(char*));
	for (int i = 0; i < number_of_objects; i++) {
		datasets_accessed[i] = NULL;
	}

	// Fetch file creation property list identifier of source file
	hid_t file_creation_property_list_id = H5Fget_create_plist(src_file_id);

	if (file_creation_property_list_id == H5I_INVALID_HID) {
		printf("Error fetching file creation property list identifier\n");
		return file_creation_property_list_id;
	}

	// Fetch file access property list identifier of source file
	hid_t file_access_property_list_id = H5Fget_access_plist(src_file_id);

	if (file_access_property_list_id == H5I_INVALID_HID) {
		printf("Error fetching file access property list identifier\n");
		return file_access_property_list_id;
	}

	// Create destination (to-be precarved) file and open the root group to duplicate the general structure of source file
	dest_file_id = H5Fcreate(precarved_filename, H5F_ACC_TRUNC, file_creation_property_list_id, file_access_property_list_id);

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
	herr_t link_iterate_return_val = H5Literate2(group_location_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &destination_group_location_id); // Iterate through each object and create shallow copy

	if (link_iterate_return_val < 0) {
		printf("Link iteration failed\n");
		return H5I_INVALID_HID;
	}

	// Iterate over attributes at root level in the source file and make non-shallow copies in the destination file
	herr_t attribute_iterate_return_val = H5Aiterate2(group_location_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &destination_group_location_id); // Iterate through each object and create shallow copy

	if (attribute_iterate_return_val < 0) {
		printf("Attribute iteration failed\n");
		return H5I_INVALID_HID;
	}
	
	return src_file_id;
}

herr_t H5Dread(hid_t dataset_id, hid_t	mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t	dxpl_id, void *buf)	{
    // Original function call
	original_H5Dread = dlsym(RTLD_NEXT, "H5Dread");
	herr_t return_val = original_H5Dread(dataset_id, mem_type_id, mem_space_id, file_space_id, dxpl_id, buf);

	// Fetch USE_PRECARVED environment variable
	use_precarved = getenv("USE_PRECARVED");

	// Check if USE_PRECARVED environment variable has been set and return if it has
	if (use_precarved != NULL && strcmp(use_precarved, "1") == 0) {	
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

    // Iterate over datasets accessed and create copies in the destination file, including contents
    for (int i = 0; i < number_of_objects; i++) {
    	// If dataset access has already been recorded, ignore and break
    	if (datasets_accessed[i] != NULL && strcmp(datasets_accessed[i], dataset_name) == 0) {
    		break;
    	}

    	// If dataset access has not been recorded, record and make copy of the dataset in the destination file
    	if (datasets_accessed[i] == NULL) {
    		// Populate ith element of array with the name of dataset
    		datasets_accessed[i] = malloc(size_of_name_buffer);
    		datasets_accessed[i] = dataset_name;
    		
    		// Open skeleton dataset in the destination file
    		hid_t destination_dataset_id;
    		destination_dataset_id = H5Dopen(dest_file_id, dataset_name, H5P_DEFAULT);

    		if (destination_dataset_id == H5I_INVALID_HID) {
    			printf("Error opening dataset\n");
    			return destination_dataset_id;
    		}

    		// Retrives the dataspace of dataset
    		hid_t dest_dataspace = H5Dget_space(destination_dataset_id);

    		if (dest_dataspace == H5I_INVALID_HID) {
    			printf("Error getting destination dataspace\n");
    			return dest_dataspace;
    		}

			H5S_class_t dest_dataspace_class = H5Sget_simple_extent_type(dest_dataspace);

			if (dest_dataspace_class == H5S_NO_CLASS) {
				printf("Error getting destination dataspace class\n");
				return dest_dataspace_class;
			}

			// If dataspace class is NULL, it means the dataset is empty. 
			if (dest_dataspace_class == H5S_NULL) {
				H5Ldelete(dest_file_id, dataset_name, H5P_DEFAULT); // Delete empty copy so that we are able to make a new copy with contents populated
			}

			// Make copy of dataset in the destination file
    		herr_t object_copy_return_val = H5Ocopy(src_file_id, dataset_name, dest_file_id, dataset_name, H5P_DEFAULT, H5P_DEFAULT);

    		if (object_copy_return_val < 0) {
    			printf("Copying %s failed.\n", dataset_name);
    			return object_copy_return_val;
    		}

    		break;
    	}
    }
	
	return return_val;
}