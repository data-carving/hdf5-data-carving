#include "hdf5.h"
#include "H5carve_helper_functions.h"
#include <stdlib.h>
#include <string.h>

extern hid_t src_file_id;
extern hid_t dest_file_id;
extern char *use_precarved;
extern hid_t original_file_id;

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

char *get_carved_filename(const char *filename) {
	// Create name of the carved file
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
	char carved_suffix[] = "_carved.";
	int filename_length = strlen(filename_without_extension);
	int suffix_length = strlen(carved_suffix);
	char *carved_filename = malloc(sizeof(char) * (filename_length + suffix_length + 1));
	strcpy(carved_filename, filename_without_extension);
	strcat(carved_filename, carved_suffix);
	strcat(carved_filename, filename_extension);
	
	return carved_filename;
}


bool is_dataset_null(hid_t dataset_id) {
	// Retrives the dataspace of dataset
	hid_t dataspace = H5Dget_space(dataset_id);

	if (dataspace == H5I_INVALID_HID) {
		printf("Error getting destination dataspace\n");
		return dataspace;
	}

	H5S_class_t dataspace_class = H5Sget_simple_extent_type(dataspace);

	if (dataspace_class == H5S_NO_CLASS) {
		printf("Error getting destination dataspace class\n");
		return dataspace_class;
	}

	// If dataspace class is NULL, it means the dataset is empty. 
	return dataspace_class == H5S_NULL;
}