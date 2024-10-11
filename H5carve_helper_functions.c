#define _GNU_SOURCE
#include "hdf5.h"
#include "H5carve_helper_functions.h"
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

herr_t (*original_H5Dread)(hid_t, hid_t, hid_t, hid_t, hid_t, void*);
hid_t (*original_H5Oopen)(hid_t, const char *, hid_t);

extern hid_t src_file_id;
extern hid_t dest_file_id;
extern char *use_precarved;
extern hid_t original_file_id;
extern char **files_opened;
extern int files_opened_current_size;
extern FILE *log_ptr;
extern char *DEBUG;

hobj_ref_t *copy_reference_object(hobj_ref_t *source_ref, int num_elements, hid_t src_attribute_id) {
	hobj_ref_t *dest_ref = malloc(num_elements * sizeof(hobj_ref_t));

	// Iterate over all elements in the reference attribute
	for (int i = 0; i < num_elements; i++) {
		// Process the reference data
	    hid_t referenced_obj = H5Rdereference1(src_attribute_id, H5R_OBJECT, (source_ref + i)); // Should this be replaced by H5Rdereference? Attempting to replace it leads to errors

	    if (referenced_obj < 0) {
	    	if (DEBUG)
	        	fprintf(log_ptr, "Error dereferencing object %ld\n", src_attribute_id);
	        return NULL;
	    }

	    // Fetch length of name of referenced object
	    int size_of_name_buffer = H5Iget_name(referenced_obj, NULL, 0) + 1; // Preliminary call to fetch length of dataset name

	    if (size_of_name_buffer == 0) {
	        if (DEBUG)
	        	fprintf(log_ptr, "Error fetching size of dataset name buffer %ld\n", referenced_obj);
	        return NULL;
	    }

	    // Create and populate buffer for object name
	    char *referenced_obj_name = (char *)malloc(size_of_name_buffer);
	    H5Iget_name(referenced_obj, referenced_obj_name, size_of_name_buffer); // Fill referenced_obj_name buffer with the dataset name

	    if (DEBUG)
	        fprintf(log_ptr, "Creating reference to object %s\n", referenced_obj_name);

	    // Create reference in the carved file
	    herr_t ref_dest_creation_return_value = H5Rcreate((dest_ref + i), dest_file_id, referenced_obj_name, H5R_OBJECT, -1);

	    if (ref_dest_creation_return_value < 0) {
	        if (DEBUG)
	        	fprintf(log_ptr, "Error creating destination file reference %ld %s\n", dest_file_id, referenced_obj_name);
	        return NULL;
	    }

	    free(referenced_obj_name);
	}
    
	return dest_ref;
}

herr_t copy_compound_type(hid_t src_id, void *src_buffer, void *dest_buffer, hid_t data_type, int num_elements, int num_members, size_t starting_offset) {
	// Iterate over all elements in the compound attribute
    for (hsize_t j = 0; j < num_elements; j++) {
    	if (DEBUG)
    		fprintf(log_ptr, "Copying element %ld\n", j);

    	// Iterate over all members in an element
		for (int i = 0; i < num_members; i++) {
			if (DEBUG)
    			fprintf(log_ptr, "Copying member %d\n", i);

			char *field_name = H5Tget_member_name(data_type, i);
    		size_t field_offset = H5Tget_member_offset(data_type, i);
    		hid_t field_type = H5Tget_member_type(data_type, i);

    		// Get the precise offset of the member, depending on the starting offset of the compound type. 
    		// Starting offset is 0 for the root compound type. For nested compound types, it is the field_offset of the parent compound type.
    		size_t offset = starting_offset + field_offset;

    		if (DEBUG)
    			fprintf(log_ptr, "Field name %s Field offset %ld Field type %d\n", field_name, field_offset, H5Tget_class(field_type));

    		// Check datatype class of each datatype
    		if (H5Tget_class(field_type) == H5T_REFERENCE) {
    			// TODO add check for REF_OBJ and DSET_REGION
        		hobj_ref_t *ref_data_dest = copy_reference_object(src_buffer + offset, 1, src_id);

        		if (ref_data_dest == NULL) {
        			return -1;
        		}

        		// Add member to the compound datatype in carved file
		        H5Tinsert(data_type, field_name, offset, field_type);

		        // Copy data to the offset of the member in destination buffer
				memcpy(dest_buffer + offset, ref_data_dest, H5Tget_size(field_type));
				free(ref_data_dest);
    		} else if (H5Tget_class(field_type) == H5T_COMPOUND) {
    			// Get number of members for the nested compound datatype member
				int nested_num_members = H5Tget_nmembers(field_type);

				// Copy nested compound datatype member recursively
    			herr_t copy_compound_type_return_value = copy_compound_type(src_id, src_buffer, dest_buffer, field_type, 1, nested_num_members, offset);

    			if (copy_compound_type_return_value == -1) {
    				return -1;
    			}
    		} else if (H5Tget_class(field_type) == H5T_VLEN) {
				hvl_t *dest_data = copy_vlen_type(src_id, field_type, (hvl_t *)(src_buffer + offset), 1);
				
				if (dest_data == NULL) {
					return -1;
				}

				// Copy data to the offset of the member in destination buffer
				memcpy(dest_buffer + offset, dest_data, H5Tget_size(field_type));
				free(dest_data);
    		} else {
    			H5Tinsert(data_type, field_name, offset, field_type);

    			// Traverse to the appropriate offset in the source buffer 
    			char *value = (char *)src_buffer + offset;

    			// Copy data to the offset of the member in destination buffer
				memcpy(dest_buffer + offset, value, H5Tget_size(field_type));
    		}
		}

		// Increment the starting offset by the size of each element of the compound datatype. 
		// Each element of a compound datatype is positioned at an offset that is a multiple of its position in the compound datatype. 
		// The offset of each element is the size of the compound datatype.
    	starting_offset += (H5Tget_size(data_type));
	}
}

hvl_t *copy_vlen_type(hid_t src_attribute_id, hid_t data_type, hvl_t *src_data, int num_elements) {
	hvl_t *dest_data = malloc(num_elements * sizeof(hvl_t));

	// Check datatype class of each datatype
	if (H5Tget_class(H5Tget_super(data_type)) == H5T_REFERENCE) {
		if (H5Tequal(H5Tget_super(data_type), H5T_STD_REF_OBJ)) {
			// Process the VLEN data
		    for (int i = 0; i < num_elements; i++) {
		    	if (DEBUG)
    				fprintf(log_ptr, "Copying REFERENCE element %d len %ld\n", i, dest_data[i].len);

    			// Create carved version of the ith element of the hvl_t struct
			    dest_data[i].len = src_data[i].len;
			 	dest_data[i].p = copy_reference_object(src_data[i].p, src_data[i].len, src_attribute_id);
		    }
		} else if (H5Tequal(H5Tget_super(data_type), H5T_STD_REF_DSETREG)) {
			// TODO: Add support for dataset region references
        	printf("Dataset region references not supported yet.\n");
        	return NULL;
		}
	} else if (H5Tget_class(H5Tget_super(data_type)) == H5T_COMPOUND) {
		for (int i = 0; i < num_elements; i++) {
			// Get the size of the compound datatype
	    	size_t size = H5Tget_size(H5Tget_super(data_type));

	    	// Number of elements of the compound datatype is equal to the length of each hvl_t struct
	    	hsize_t num_elements = src_data[i].len;

	    	// Create carved version of the ith element of the hvl_t struct
	    	dest_data[i].len = src_data[i].len;
	    	dest_data[i].p = malloc(size * num_elements);

	    	// Get number of members in the compound datatype
	    	hid_t vlen_element_data_type = H5Tget_super(data_type);
    		int num_members = H5Tget_nmembers(vlen_element_data_type);
    		
    		if (DEBUG)
    			fprintf(log_ptr, "Copying COMPOUND element %d len %ld members %d\n", i, dest_data[i].len, num_members);
    		
    		herr_t copy_compound_type_return_value = copy_compound_type(src_attribute_id, src_data[i].p, dest_data[i].p, vlen_element_data_type, num_elements, num_members, 0);

    		if (copy_compound_type_return_value == -1) {
				return NULL;
			}
		}
	} else {
		// Process the VLEN data
	    for (int i = 0; i < num_elements; i++) {
	    	if (DEBUG)
    				fprintf(log_ptr, "Copying OTHER element %d len %ld\n", i, dest_data[i].len);

    		// Iterate over each element of the hvl_t struct
	        for (int j = 0; j < src_data[i].len; j++) {
	    		// Create carved version of the ith element of the hvl_t struct
	            dest_data[i].len = src_data[i].len;
	            dest_data[i].p = src_data[i].p;
	        }
	    }
	}

	return dest_data;
}	

herr_t copy_attributes(hid_t loc_id, const char *name, const H5L_info_t *ainfo, void *opdata) {
	if (DEBUG)
		fprintf(log_ptr, "Copying attributes of object %s\n", name);
	// Open the object
	original_H5Oopen = dlsym(RTLD_NEXT, "H5Oopen");
	hid_t object_id = original_H5Oopen(loc_id, name, H5P_DEFAULT);

	if (object_id < 0) {
		printf("Error opening object %ld %s\n", loc_id, name);
		return object_id;
	}

	// Fetch id of parent object
	hid_t dest_parent_object_id = *(hid_t *)opdata;
	
	if (dest_parent_object_id < 0) {
		printf("Error due to NULL destination file parent object\n");
		return -1;
	}

	// Fetch object type
	H5I_type_t object_type = H5Iget_type(object_id);

	if (object_type == H5I_BADID) {
		printf("Error fetching type of identifier %ld\n", object_id);
		return object_type;
	}

	// Check object type. 
	// Groups can be leaf nodes as well as subtrees whereas datasets can only be leaf nodes. 
	// Continue traversing the graph in case of groups.
	if (object_type == H5I_DATASET) {
		hid_t dest_object_id = H5Dopen(dest_parent_object_id, name, H5P_DEFAULT);

		if (dest_object_id < 0) {
			printf("Error opening dest object %ld %s\n", dest_parent_object_id, name);
			return object_id;
		}

		// Iterate over attributes at this level in the source file and make non-shallow copies in the destination file
		herr_t attribute_iterate_return_val = H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_object_attributes, &dest_object_id); // Iterate through each attribute and create a copy
		
		if (attribute_iterate_return_val < 0) {
			printf("Attribute iteration failed\n");
			return attribute_iterate_return_val;
		}
	} else if (object_type == H5I_GROUP) {
		hid_t dest_object_id = original_H5Oopen(dest_parent_object_id, name, H5P_DEFAULT);

		if (dest_object_id < 0) {
			printf("Error opening dest object %ld %s\n", dest_parent_object_id, name);
			return object_id;
		}

		// Iterate over attributes at this level in the source file and make non-shallow copies in the destination file
		herr_t attribute_iterate_return_val = H5Aiterate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_object_attributes, &dest_object_id); // Iterate through each attribute and create a copy
		
		if (attribute_iterate_return_val < 0) {
			printf("Attribute iteration failed\n");
			return attribute_iterate_return_val;
		}

		// Iterate over objects at this level in the source file, and make shallow copes in the destination file
		herr_t link_iterate_return_val = H5Literate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, copy_attributes, &dest_object_id);

		if (link_iterate_return_val < 0) {
			printf("Link iteration failed\n");
			return link_iterate_return_val;
		}
	}

	return 0;
}

int copy_object_attributes(hid_t loc_id, const char *name, const H5A_info_t *linfo, void *opdata) {
	hid_t dest_attribute_id, attribute_data_type, attribute_data_space;
	hid_t dest_object_id = *(hid_t *)opdata;

	if (dest_object_id < 0) {
		return -1;
	}

	// Open the attribute
	hid_t src_attribute_id = H5Aopen(loc_id, name, H5P_DEFAULT);

	if (src_attribute_id < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error opening attribute %ld %s\n", loc_id, name);
		return src_attribute_id ;
	}

	// Fetch length of name of attribute
	int size_of_name_buffer = H5Aget_name(src_attribute_id, 0, 0) + 1;

	if (size_of_name_buffer < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error fetching attribute name %ld\n", src_attribute_id);
		return size_of_name_buffer;
	}

	// Create and populate buffer for attribute name
	char *name_of_attribute = (char *)malloc(size_of_name_buffer);
	H5Aget_name(src_attribute_id, size_of_name_buffer, name_of_attribute);

	// Fetch data type of attribute
	attribute_data_type = H5Aget_type(src_attribute_id);

	if (attribute_data_type == H5I_INVALID_HID) {
		if (DEBUG)
    		fprintf(log_ptr, "Error fetching attribute data type %ld\n", src_attribute_id);
		return attribute_data_type;
	}

	// Check if attribute_data_type is a REFERENCE datatype
    if (H5Tget_class(attribute_data_type) == H5T_REFERENCE) {
	    // Get the number of elements in the reference attribute
	    hsize_t num_elements = H5Aget_storage_size(src_attribute_id) / sizeof(hobj_ref_t);

	    if (num_elements == 0) {
	        if (DEBUG)
    			fprintf(log_ptr, "Error getting attribute storage size %ld\n", src_attribute_id);
	        return -1;
	    }
	    
	    // Allocate memory to store the reference data
	    hobj_ref_t *ref_data_src_file = malloc(num_elements * sizeof(hobj_ref_t));

	    // Read the reference attribute into the allocated memory
	    herr_t read_return_val = H5Aread(src_attribute_id, H5T_STD_REF_OBJ, ref_data_src_file);

	    if (read_return_val < 0) {
	        if (DEBUG)
    			fprintf(log_ptr, "Error reading attribute %ld\n", src_attribute_id);
	        return read_return_val;
	    }

	    // Currently, supports only object references and not dataset region references
	    if (H5Tequal(attribute_data_type, H5T_STD_REF_OBJ)) {
	    	if (DEBUG)
    			fprintf(log_ptr, "Copying REFERENCE attribute %s type %ld %ld elements\n", name_of_attribute, H5T_STD_REF_OBJ, num_elements);

	        hobj_ref_t *ref_data_dest = copy_reference_object(ref_data_src_file, num_elements, src_attribute_id);

	        free(ref_data_src_file);

	        // Copy the dataspace
	        hid_t ref_data_dest_dataspace = H5Aget_space(src_attribute_id);   
	        if (ref_data_dest_dataspace < 0) {
	            if (DEBUG)
    				fprintf(log_ptr, "Error copying dataspace %ld\n", src_attribute_id);
	            return -1;
	        }

	        // If attribute already exists, open the existing attribute. Otherwise, create the attribute.
	        if (H5Aexists(dest_object_id, name_of_attribute)) {
	        	dest_attribute_id = H5Aopen(dest_object_id, name_of_attribute, H5P_DEFAULT);
	        } else {
	        	dest_attribute_id = H5Acreate2(dest_object_id, name_of_attribute, H5T_STD_REF_OBJ, ref_data_dest_dataspace, H5P_DEFAULT, H5P_DEFAULT);
	        }

	        if (dest_attribute_id < 0) {
	            if (DEBUG)
    				fprintf(log_ptr, "Error creating destination file attribute %ld %s %ld\n", dest_object_id, name_of_attribute, ref_data_dest_dataspace);
	            return -1;
	        }

	        herr_t status = H5Awrite(dest_attribute_id, H5T_STD_REF_OBJ, ref_data_dest);
	        if (status < 0) {
	            if (DEBUG)
    				fprintf(log_ptr, "Error writing reference to attribute %ld\n", dest_attribute_id);
	            return -1;
	        }

	        free(ref_data_dest);
	    } else if (H5Tequal(attribute_data_type, H5T_STD_REF_DSETREG)) {
	        // TODO: Add support for dataset region references
	        if (DEBUG)
    			fprintf(log_ptr, "Dataset region references not supported yet.\n");
	        return -1;
	    }
    } else if (H5Tget_class(attribute_data_type) == H5T_COMPOUND) {
    	// Fetch data space of attribute
		attribute_data_space = H5Aget_space(src_attribute_id);

		// Get the size of the compound datatype
	    size_t size = H5Tget_size(attribute_data_type);
	    
		hsize_t num_points;
	    H5Sget_simple_extent_dims(attribute_data_space, &num_points, NULL);

	    // Allocate buffer to read the attribute
	    void *src_buffer = malloc(size * num_points);

	    // Read the attribute data
	    herr_t status = H5Aread(src_attribute_id, attribute_data_type, src_buffer);

	    hid_t dest_attribute_type = H5Tcreate(H5T_COMPOUND, size);

	    // Allocate buffer to read the attribute
	    void *dest_buffer = malloc(size * num_points);

	    // Get number of members in the compound datatype
    	int num_members = H5Tget_nmembers(attribute_data_type);

    	if (DEBUG)
    		fprintf(log_ptr, "Copying COMPOUND attribute %s %ld elements %d members\n", name_of_attribute, num_points, num_members);

        herr_t copy_compound_type_return_value = copy_compound_type(src_attribute_id, src_buffer, dest_buffer, attribute_data_type, num_points, num_members, 0);

        free(src_buffer);

        if (copy_compound_type_return_value == -1) {
        	return -1;
        }

	    // If attribute already exists, open the existing attribute. Otherwise, create the attribute.
        if (H5Aexists(dest_object_id, name_of_attribute)) {
        	dest_attribute_id = H5Aopen(dest_object_id, name_of_attribute, H5P_DEFAULT);
        } else {
	    	dest_attribute_id = H5Acreate1(dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space, H5P_DEFAULT);
        }

	    if (dest_attribute_id < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error creating attribute %ld %s %ld %ld\n", dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space);
			return dest_attribute_id;
		}

	    herr_t write_return_val = H5Awrite(dest_attribute_id, attribute_data_type, dest_buffer);

	    if (write_return_val < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error writing attribute %ld %ld\n", dest_attribute_id, attribute_data_type);
			return write_return_val;
		}
    } else if (H5Tget_class(attribute_data_type) == H5T_VLEN) {
    	// Fetch data space of attribute
		attribute_data_space = H5Aget_space(src_attribute_id);

		hsize_t dims[1];
		H5Sget_simple_extent_dims(attribute_data_space, dims, NULL);

		if (DEBUG)
    		fprintf(log_ptr, "Copying VLEN attribute %s type %ld %ld elements\n", name_of_attribute, attribute_data_type, dims[0]);

    	// Allocate memory to read VLEN data
		hvl_t *src_data = (hvl_t *)malloc(dims[0] * sizeof(hvl_t));

		herr_t status = H5Aread(src_attribute_id, attribute_data_type, src_data);
		
	    // If attribute already exists, open the existing attribute. Otherwise, create the attribute.
		if (H5Aexists(dest_object_id, name_of_attribute)) {
			dest_attribute_id = H5Aopen(dest_object_id, name_of_attribute, H5P_DEFAULT);
		} else {
			dest_attribute_id = H5Acreate(dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space, H5P_DEFAULT, H5P_DEFAULT);
		}

		hvl_t *dest_data = copy_vlen_type(src_attribute_id, attribute_data_type, src_data, dims[0]);

		if (dest_data == NULL) {
			return -1;
		}

	    herr_t write_status = H5Awrite(dest_attribute_id, attribute_data_type, dest_data);
	    
	    if (write_status < 0) {	
	        if (DEBUG)
    			fprintf(log_ptr, "Error writing attribute %ld %ld\n", dest_attribute_id, attribute_data_type);
	        return write_status;
	    }

	    if ((H5Tget_class(H5Tget_super(attribute_data_type)) == H5T_COMPOUND) || (H5Tget_class(H5Tget_super(attribute_data_type)) == H5T_REFERENCE)) {
	    	for (int i = 0; i < dims[0]; i++) {
	    		free(dest_data[i].p);
	    	}
	    }

	    free(dest_data);
	    free(src_data);
    } else {
    	if (DEBUG)
    		fprintf(log_ptr, "Copying OTHER attribute %s\n", name_of_attribute);
    	// Fetch data space of attribute
		attribute_data_space = H5Aget_space(src_attribute_id);

		if (attribute_data_space < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error fetching attribute data space %ld\n", src_attribute_id);
			return attribute_data_space;
		}

		// Fetch data size of attribute
		hsize_t attribute_data_size = H5Aget_storage_size(src_attribute_id);

		// Create and populate buffer for attribute data
		void* attribute_data_buffer = malloc(attribute_data_size);
		herr_t read_return_val = H5Aread(src_attribute_id, attribute_data_type, attribute_data_buffer);

		if (read_return_val < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error reading attribute data %ld %ld\n", src_attribute_id, attribute_data_type);
			return read_return_val;
		}

		if (H5Aexists(dest_object_id, name_of_attribute)) {
			dest_attribute_id = H5Aopen(dest_object_id, name_of_attribute, H5P_DEFAULT);
		} else {
			// Create attribute in destination file
			dest_attribute_id = H5Acreate1(dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space, H5P_DEFAULT);
		}
		
		if (dest_attribute_id < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error creating attribute %ld %s %ld %ld\n", dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space);
			return dest_attribute_id;
		}

		// Write attribute data to the newly created attribute in destination file
		herr_t write_return_val = H5Awrite(dest_attribute_id, attribute_data_type, attribute_data_buffer);

		if (write_return_val < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error writing attribute %ld %ld\n", dest_attribute_id, attribute_data_type);
			return write_return_val;
		}

		free(attribute_data_buffer);
    }

    free(name_of_attribute);
	return 0;
}

herr_t delete_attributes(hid_t loc_id, const char *name, const H5A_info_t *ainfo, void *opdata) {
	herr_t status = H5Adelete(loc_id, name);

	if (status < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error deleting attribute %ld %s\n", loc_id, name);
		return -1;
	}
}

// Copy structure of the HDF5 without copying contents. Essentially a DFS into the directed graph structure of an HDF5 file.
// In the directed graph structure, datasets are leaf nodes and groups are sub-trees
herr_t shallow_copy_object(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	if (DEBUG)
    	fprintf(log_ptr, "Creating shallow copy of object %ld %s\n", loc_id, name);

	// Open the object
	original_H5Oopen = dlsym(RTLD_NEXT, "H5Oopen");
	hid_t object_id = original_H5Oopen(loc_id, name, H5P_DEFAULT);

	if (object_id < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error opening object %ld %s\n", loc_id, name);
		return object_id;
	}

	// Fetch id of parent object
	hid_t *dest_parent_object_id = (hid_t *)opdata;
	
	if (dest_parent_object_id == NULL) {
		if (DEBUG)
    		fprintf(log_ptr, "Error due to NULL destination file parent object\n");
		return -1;
	}

	// Fetch object type
	H5I_type_t object_type = H5Iget_type(object_id);

	if (object_type == H5I_BADID) {
		if (DEBUG)
    		fprintf(log_ptr, "Error fetching type of identifier %ld\n", object_id);
		return object_type;
	}

	// Fetch length of name of object
	int size_of_name_buffer = H5Iget_name(object_id, NULL, 0) + 1; // Preliminary call to fetch length of object name

	if (size_of_name_buffer == 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error fetching size of name %ld\n", object_id);
		return -1;
	}
	
	// If object is a dataset, make shallow copy of dataset and terminate
	if (object_type == H5I_DATASET) {
		hid_t dataset_id, data_type, data_space;

		// Create and populate buffer for name of dataset
    	char *object_name = (char *)malloc(size_of_name_buffer);
    	H5Iget_name(object_id, object_name, size_of_name_buffer); // Fill object_name buffer with the name

		// Open the dataset
		dataset_id = H5Dopen(src_file_id, object_name, H5P_DEFAULT);

		if (dataset_id == H5I_INVALID_HID) {
			if (DEBUG)
    			fprintf(log_ptr, "Error opening dataset %ld %s\n", src_file_id, object_name);
			return dataset_id;
		}

		// Fetch data type of dataset
		data_type = H5Dget_type(dataset_id);

		if (data_type == H5I_INVALID_HID) {
			if (DEBUG)
    			fprintf(log_ptr, "Error fetching type of dataset %ld\n", dataset_id);
			return data_type;
		}

		// Create null dataspace for shallow copy
		data_space = H5Dget_space(dataset_id);

		if (data_space == H5I_INVALID_HID) {
			if (DEBUG)
    			fprintf(log_ptr, "Error fetching data space of dataset %ld\n", dataset_id);
			return data_space;
		}

		// Create dataset in destination file
		hid_t dest_dataset_id = H5Dcreate(*dest_parent_object_id, object_name, data_type, data_space, H5P_DEFAULT, H5Dget_create_plist(dataset_id), H5P_DEFAULT);

		if (dest_dataset_id < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error creating shallow copy of dataset %s. dest_parent_object_id is %ld data_type %ld dataspace %ld\n", name, *dest_parent_object_id, data_type, data_space);
			return dest_dataset_id;
		}

		// Create a scalar dataspace for the attribute.
	    hid_t attr_dataspace_id = H5Screate(H5S_SCALAR);

	    // Create an attribute to indicate that the dataset is empty.
	    hid_t attr_id = H5Acreate2(dest_dataset_id, "CARVED_DATASET_IS_EMPTY", H5T_NATIVE_HBOOL, attr_dataspace_id, 
	                               H5P_DEFAULT, H5P_DEFAULT);

	    hbool_t is_empty = true;
	    H5Awrite(attr_id, H5T_NATIVE_HBOOL, &is_empty);

	    free(object_name);
	// If object is a group, make shallow copy of the group and recursively go down the tree
	} else if (object_type == H5I_GROUP) {
		// Create group in destination file
		hid_t dest_group_id = H5Gcreate1(*dest_parent_object_id, name, size_of_name_buffer);

		if (dest_group_id < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error creating shallow copy of group %s. dest_parent_object_id is %ld\n", name, *dest_parent_object_id);
			return dest_group_id;
		}

		// Iterate over objects at this level in the source file, and make shallow copes in the destination file
		herr_t link_iterate_return_val = H5Literate2(object_id, H5_INDEX_NAME, H5_ITER_INC, NULL, shallow_copy_object, &dest_group_id);

		if (link_iterate_return_val < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Link iteration failed\n");
			return link_iterate_return_val;
		}	
	}
	
	return 0;
}

char *get_carved_filename(const char *filename, char *is_netcdf4, char *use_carved) {
	char *carved_directory = getenv("CARVED_DIRECTORY");

	// Make a copy of the filename.
	char *filename_copy;
	filename_copy = malloc(strlen(filename) + 1);
	strcpy(filename_copy, filename);

	// If original file is a netCDF4 file and tool is being used in re-execution mode, interposed nc_open returns filename with the .carved suffix.
	if (is_netcdf4 != NULL && use_carved != NULL) {
		filename_copy[strlen(filename_copy) - 7] = '\0';
	}

	// Traverse the file path to get to the last occurrence of directory separtors to get to name of the file
	char *filename_without_directories = strrchr(filename_copy, '/');

	// If there are no directory separators in filename, strrchr returns NULL. 
	// If it has returned NULL, replace variable back with the filename. 
	// Otherwise, increment by 1 to skip the last directory separator
	if (filename_without_directories != NULL) {
	        filename_without_directories = filename_without_directories + 1;
	} else {
	        filename_without_directories = filename_copy;
	}

	char *carved_filename;
	int carved_filename_length_without_suffix;

	// If no directory is specified for the carved files, the length of the carved file directory path does not need to be considered.
	// If a directory is specified for the carved files, the length of the carved file directory path needs to be included when allocating memory.
	if (carved_directory == NULL) {
		carved_filename_length_without_suffix = strlen(filename);
	} else {
		carved_filename_length_without_suffix = strlen(carved_directory) + strlen(filename_without_directories);
	}

	carved_filename = (char *)malloc(carved_filename_length_without_suffix + 7 + 1); // 7 + 1 is for the suffix .carved and null terminating character

	// Construct the path of the carved file.
	// If no directory for carved files is specified, carved files are created in the same location as the original files.
	// If a directory for carved files is specified, carved files are created in the specified directory. 
	if (carved_directory == NULL) {
	    carved_filename[0] = '\0';
		strcat(carved_filename, filename);
		strcat(carved_filename, ".carved");	
	} else {
		carved_filename[0] = '\0';
		strcat(carved_filename, carved_directory);
		strcat(carved_filename, filename_without_directories);
		strcat(carved_filename, ".carved");
	}

	free(filename_copy);

	return carved_filename;
}

bool does_dataset_exist(hid_t dataset_id) {
	// If the CARVED_DATASET_IS_EMPTY attribute does not exist, the dataset is not empty.
	if (!H5Aexists(dataset_id, "CARVED_DATASET_IS_EMPTY")) {
		return true;
	}

	hid_t attr_id = H5Aopen(dataset_id, "CARVED_DATASET_IS_EMPTY", H5P_DEFAULT);

	if (attr_id < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error opening CARVED_DATASET_IS_EMPTY attribute %ld\n", dataset_id);
		return attr_id;
	}

	hbool_t is_empty;

	herr_t attribute_read_ret = H5Aread(attr_id, H5T_NATIVE_UINT8, &is_empty);

	if (attribute_read_ret < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error reading CARVED_DATASET_IS_EMPTY attribute %ld\n", attr_id);
		return attribute_read_ret;
	}

	return !(is_empty == true);
}

bool is_already_recorded(const char *filename) {
	for (int i = 0; i < files_opened_current_size; i++) {
		if (strcmp(files_opened[i], filename) == 0) {
			return true;
		}
	}

	return false;
}