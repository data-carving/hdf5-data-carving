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

	for (int i = 0; i < num_elements; i++) {
		// Process the reference data
	    hid_t referenced_obj = H5Rdereference1(src_attribute_id, H5R_OBJECT, (source_ref + i)); // Should this be replaced by H5Rdereference? Attempting to replace it leads to errors

	    if (referenced_obj < 0) {
	    	if (DEBUG)
	        	fprintf(log_ptr, "Error dereferencing object %d\n", src_attribute_id);
	        return referenced_obj;
	    }

	    // Fetch length of name of dataset
	    int size_of_name_buffer = H5Iget_name(referenced_obj, NULL, 0) + 1; // Preliminary call to fetch length of dataset name

	    if (size_of_name_buffer == 0) {
	        if (DEBUG)
	        	fprintf(log_ptr, "Error fetching size of dataset name buffer %d\n", referenced_obj);
	        return -1;
	    }

	    // Create and populate buffer for dataset name
	    char *referenced_obj_name = (char *)malloc(size_of_name_buffer);
	    H5Iget_name(referenced_obj, referenced_obj_name, size_of_name_buffer); // Fill referenced_obj_name buffer with the dataset name

	    if (DEBUG)
	        fprintf(log_ptr, "Creating reference to object %s\n", referenced_obj_name);

	    herr_t ref_dest_creation_return_value = H5Rcreate((dest_ref + i), dest_file_id, referenced_obj_name, H5R_OBJECT, -1);

	    if (ref_dest_creation_return_value < 0) {
	        if (DEBUG)
	        	fprintf(log_ptr, "Error creating destination file reference %d %s\n", dest_file_id, referenced_obj_name);
	        return ref_dest_creation_return_value;
	    }
	}
    
	return dest_ref;
}

void copy_compound_type(hid_t src_id, void *src_buffer, void *dest_buffer, hid_t data_type, int num_elements, int num_members, size_t starting_offset) {
    for (hsize_t j = 0; j < num_elements; j++) {
    	if (DEBUG)
    		fprintf(log_ptr, "Copying element %d\n", j);

		for (int i = 0; i < num_members; i++) {
			if (DEBUG)
    			fprintf(log_ptr, "Copying member %d\n", i);

			char *field_name = H5Tget_member_name(data_type, i);
    		size_t field_offset = H5Tget_member_offset(data_type, i);
    		hid_t field_type = H5Tget_member_type(data_type, i);

    		size_t offset = starting_offset + field_offset;

    		if (DEBUG)
    			fprintf(log_ptr, "Field name %s Field offset %d Field type %d\n", field_name, field_offset, H5Tget_class(field_type));

    		if (H5Tget_class(field_type) == H5T_REFERENCE) {
    			// TODO add check for REF_OBJ and DSET_REGION
        		hobj_ref_t *ref_data_dest = copy_reference_object(src_buffer + offset, 1, src_id);

		        H5Tinsert(data_type, field_name, offset, field_type);
				memcpy(dest_buffer + offset, ref_data_dest, H5Tget_size(field_type));
    		} else if (H5Tget_class(field_type) == H5T_COMPOUND) {
				int nested_num_members = H5Tget_nmembers(field_type);

    			copy_compound_type(src_id, src_buffer, dest_buffer, field_type, 1, nested_num_members, offset);
    		} else if (H5Tget_class(field_type) == H5T_VLEN) {
				hvl_t *dest_data = copy_vlen_type(src_id, field_type, (hvl_t *)(src_buffer + offset), 1);
				
				memcpy(dest_buffer + offset, dest_data, H5Tget_size(field_type));
    		} else {
    			H5Tinsert(data_type, field_name, offset, field_type);
    			char *value = (char *)src_buffer + offset;
				memcpy(dest_buffer + offset, value, H5Tget_size(field_type));
    		}
		}

    	starting_offset += (H5Tget_size(data_type));
	}
}

hvl_t *copy_vlen_type(hid_t src_attribute_id, hid_t data_type, hvl_t *rdata, int num_elements) {
	hvl_t *dest_data = malloc(num_elements * sizeof(hvl_t));

	if (H5Tget_class(H5Tget_super(data_type)) == H5T_REFERENCE) {
		if (H5Tequal(H5Tget_super(data_type), H5T_STD_REF_OBJ)) {
			// Process the VLEN data
		    for (int i = 0; i < num_elements; i++) {
		    	if (DEBUG)
    				fprintf(log_ptr, "Copying REFERENCE element %d len %d\n", i, dest_data[i].len);

			    dest_data[i].len = rdata[i].len;
			 	dest_data[i].p = copy_reference_object(rdata[i].p, rdata[i].len, src_attribute_id);   
		    }
		} else if (H5Tequal(H5Tget_super(data_type), H5T_STD_REF_DSETREG)) {
			// TODO: Add support for dataset region references
        	printf("Dataset region references not supported yet.\n");
        	return -1;
		}
	} else if (H5Tget_class(H5Tget_super(data_type)) == H5T_COMPOUND) {
		for (int i = 0; i < num_elements; i++) {
			dest_data[i].len = rdata[i].len;
			// Get the size of the compound datatype
	    	size_t size = H5Tget_size(H5Tget_super(data_type));

	    	hsize_t num_points = rdata[i].len;
	    	dest_data[i].p = malloc(size * num_points);

	    	hid_t vlen_element_data_type = H5Tget_super(data_type);
    		int num_members = H5Tget_nmembers(vlen_element_data_type);
    		
    		if (DEBUG)
    			fprintf(log_ptr, "Copying COMPOUND element %d len %d members %d\n", i, dest_data[i].len, num_members);
    		
    		copy_compound_type(src_attribute_id, rdata[i].p, dest_data[i].p, vlen_element_data_type, num_points, num_members, 0);
		}
	} else {
		// Process the VLEN data
	    for (int i = 0; i < num_elements; i++) {
	    	if (DEBUG)
    				fprintf(log_ptr, "Copying OTHER element %d len %d\n", i, dest_data[i].len);

	        for (int j = 0; j < rdata[i].len; j++) {
	            dest_data[i].len = rdata[i].len;
	            dest_data[i].p = rdata[i].p;
	        }
	    }
	}

	return dest_data;
}	

herr_t copy_attributes(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	if (DEBUG)
		fprintf(log_ptr, "Copying attributes of object %s\n", name);
	// Open the object
	original_H5Oopen = dlsym(RTLD_NEXT, "H5Oopen");
	hid_t object_id = original_H5Oopen(loc_id, name, H5P_DEFAULT);

	if (object_id < 0) {
		printf("Error opening object %d %s\n", loc_id, name);
		return object_id;
	}

	// Fetch id of parent object
	hid_t *dest_parent_object_id = *(hid_t *)opdata;
	
	if (dest_parent_object_id == NULL) {
		printf("Error due to NULL destination file parent object\n");
		return -1;
	}

	// Fetch object type
	H5I_type_t object_type = H5Iget_type(object_id);

	if (object_type == H5I_BADID) {
		printf("Error fetching type of identifier %d\n", object_id);
		return object_type;
	}

	if (object_type == H5I_DATASET) {
		hid_t dest_object_id = H5Dopen(dest_parent_object_id, name, H5P_DEFAULT);

		if (dest_object_id < 0) {
			printf("Error opening dest object %d %s\n", dest_parent_object_id, name);
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
			printf("Error opening dest object %d %s\n", dest_parent_object_id, name);
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

herr_t copy_object_attributes(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	hid_t dest_attribute_id, attribute_data_type, attribute_data_space;
	hid_t *dest_object_id = *(hid_t *)opdata;

	if (dest_object_id == NULL) {
		return -1;
	}

	// Open the attribute
	hid_t src_attribute_id = H5Aopen(loc_id, name, H5P_DEFAULT);

	if (src_attribute_id < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error opening attribute %d %s\n", loc_id, name);
		return src_attribute_id ;
	}

	// Fetch length of name of attribute
	int size_of_name_buffer = H5Aget_name(src_attribute_id, NULL, 0) + 1;

	if (size_of_name_buffer < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error fetching attribute name %d\n", src_attribute_id);
		return size_of_name_buffer;
	}
	// Create and populate buffer for attribute name
	char *name_of_attribute = (char *)malloc(size_of_name_buffer);
	H5Aget_name(src_attribute_id, size_of_name_buffer, name_of_attribute);

	// Fetch data type of attribute
	attribute_data_type = H5Aget_type(src_attribute_id);

	if (attribute_data_type == H5I_INVALID_HID) {
		if (DEBUG)
    		fprintf(log_ptr, "Error fetching attribute data type %d\n", src_attribute_id);
		return attribute_data_type;
	}

	// Check if attribute_data_type is a REFERENCE datatype
    if (H5Tget_class(attribute_data_type) == H5T_REFERENCE) {
	    // Get the number of elements in the reference attribute
	    hsize_t num_elements = H5Aget_storage_size(src_attribute_id) / sizeof(hobj_ref_t);

	    if (num_elements == 0) {
	        if (DEBUG)
    			fprintf(log_ptr, "Error getting attribute storage size %d\n", src_attribute_id);
	        return -1;
	    }
	    
	    // Allocate memory to store the reference data
	    hobj_ref_t *ref_data_src_file = malloc(num_elements * sizeof(hobj_ref_t));

	    // Read the reference attribute into the allocated memory
	    herr_t read_return_val = H5Aread(src_attribute_id, H5T_STD_REF_OBJ, ref_data_src_file);

	    if (read_return_val < 0) {
	        if (DEBUG)
    			fprintf(log_ptr, "Error reading attribute %d\n", src_attribute_id);
	        return read_return_val;
	    }

	    // Currently, supports only object references, and not dataset region references
	    if (H5Tequal(attribute_data_type, H5T_STD_REF_OBJ)) {
	    	if (DEBUG)
    			fprintf(log_ptr, "Copying REFERENCE attribute %s type %d %d elements\n", name_of_attribute, H5T_STD_REF_OBJ, num_elements);

	        hobj_ref_t *ref_data_dest = copy_reference_object(ref_data_src_file, num_elements, src_attribute_id);

	        // Copy the dataspace
	        hid_t ref_data_dest_dataspace = H5Aget_space(src_attribute_id);   
	        if (ref_data_dest_dataspace < 0) {
	            if (DEBUG)
    				fprintf(log_ptr, "Error copying dataspace %d\n", src_attribute_id);
	            return -1;
	        }

	        if (H5Aexists(dest_object_id, name_of_attribute)) {
	        	dest_attribute_id = H5Aopen(dest_object_id, name_of_attribute, H5P_DEFAULT);
	        } else {
	        	// Create the attribute and write the reference
	        	dest_attribute_id = H5Acreate2(dest_object_id, name_of_attribute, H5T_STD_REF_OBJ, ref_data_dest_dataspace, H5P_DEFAULT, H5P_DEFAULT);
	        }

	        if (dest_attribute_id < 0) {
	            if (DEBUG)
    				fprintf(log_ptr, "Error creating destination file attribute %d %s %d\n", dest_object_id, name_of_attribute, ref_data_dest_dataspace);
	            return -1;
	        }

	        herr_t status = H5Awrite(dest_attribute_id, H5T_STD_REF_OBJ, ref_data_dest);
	        if (status < 0) {
	            if (DEBUG)
    				fprintf(log_ptr, "Error writing reference to attribute %d\n", dest_attribute_id);
	            return -1;
	        }
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

    	int num_members = H5Tget_nmembers(attribute_data_type);

    	if (DEBUG)
    		fprintf(log_ptr, "Copying COMPOUND attribute %s %d elements %d members\n", name_of_attribute, num_points, num_members);

        copy_compound_type(src_attribute_id, src_buffer, dest_buffer, attribute_data_type, num_points, num_members, 0);

        if (H5Aexists(dest_object_id, name_of_attribute)) {
        	dest_attribute_id = H5Aopen(dest_object_id, name_of_attribute, H5P_DEFAULT);
        } else {
	    	dest_attribute_id = H5Acreate1(dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space, H5P_DEFAULT);
        }

	    if (dest_attribute_id < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error creating attribute %d %s %d %d\n", dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space);
			return dest_attribute_id;
		}

	    herr_t write_return_val = H5Awrite(dest_attribute_id, attribute_data_type, dest_buffer);

	    if (write_return_val < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error writing attribute %d %d\n", dest_attribute_id, attribute_data_type);
			return write_return_val;
		}
    } else if (H5Tget_class(attribute_data_type) == H5T_VLEN) {
    	// Fetch data space of attribute
		attribute_data_space = H5Aget_space(src_attribute_id);

		hsize_t dims[1];
		H5Sget_simple_extent_dims(attribute_data_space, dims, NULL);

		if (DEBUG)
    		fprintf(log_ptr, "Copying VLEN attribute %s type %d %d elements\n", name_of_attribute, attribute_data_type, dims[0]);

		hvl_t *rdata = (hvl_t *)malloc(dims[0] * sizeof(hvl_t));

		herr_t status = H5Aread(src_attribute_id, attribute_data_type, rdata);
			
		if (H5Aexists(dest_object_id, name_of_attribute)) {
			dest_attribute_id = H5Aopen(dest_object_id, name_of_attribute, H5P_DEFAULT);
		} else {
			dest_attribute_id = H5Acreate(dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space, H5P_DEFAULT, H5P_DEFAULT);
		}

		hvl_t *dest_data = copy_vlen_type(src_attribute_id, attribute_data_type, rdata, dims[0]);

	    herr_t write_status = H5Awrite(dest_attribute_id, attribute_data_type, dest_data);
	    
	    if (write_status < 0) {	
	        if (DEBUG)
    			fprintf(log_ptr, "Error writing attribute %d %d\n", dest_attribute_id, attribute_data_type);
	        return write_status;
	    }
    } else {
    	if (DEBUG)
    		fprintf(log_ptr, "Copying OTHER attribute %s\n", name_of_attribute);
    	// Fetch data space of attribute
		attribute_data_space = H5Aget_space(src_attribute_id);

		if (attribute_data_space < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error fetching attribute data space %d\n", src_attribute_id);
			return attribute_data_space;
		}

		// Fetch data size of attribute
		hsize_t attribute_data_size = H5Aget_storage_size(src_attribute_id);

		// Create and populate buffer for attribute data
		void* attribute_data_buffer = malloc(attribute_data_size);
		herr_t read_return_val = H5Aread(src_attribute_id, attribute_data_type, attribute_data_buffer);

		if (read_return_val < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error reading attribute data %d %d\n", src_attribute_id, attribute_data_type);
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
    			fprintf(log_ptr, "Error creating attribute %d %s %d %d\n", dest_object_id, name_of_attribute, attribute_data_type, attribute_data_space);
			return dest_attribute_id;
		}

		// Write attribute data to the newly created attribute in destination file
		herr_t write_return_val = H5Awrite(dest_attribute_id, attribute_data_type, attribute_data_buffer);

		if (write_return_val < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error writing attribute %d %d\n", dest_attribute_id, attribute_data_type);
			return write_return_val;
		}
    }

	return 0;
}

herr_t delete_attributes(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	herr_t status = H5Adelete(loc_id, name);

	if (status < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error deleting attribute %d %s\n", loc_id, name);
		return -1;
	}
}

// Copy structure of the HDF5 without copying contents. Essentially a DFS into the directed graph structure of an HDF5 file.
// In the directed graph structure, datasets are leaf nodes and groups are sub-trees
herr_t shallow_copy_object(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
	if (DEBUG)
    	fprintf(log_ptr, "Creating shallow copy of object %d %s\n", loc_id, name);

	// Open the object
	original_H5Oopen = dlsym(RTLD_NEXT, "H5Oopen");
	hid_t object_id = original_H5Oopen(loc_id, name, H5P_DEFAULT);

	if (object_id < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error opening object %d %s\n", loc_id, name);
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
    		fprintf(log_ptr, "Error fetching type of identifier %d\n", object_id);
		return object_type;
	}

	// Fetch length of name of object
	int size_of_name_buffer = H5Iget_name(object_id, NULL, 0) + 1; // Preliminary call to fetch length of object name

	if (size_of_name_buffer == 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error fetching size of name %d\n", object_id);
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
			if (DEBUG)
    			fprintf(log_ptr, "Error opening dataset %d %s\n", src_file_id, object_name);
			return dataset_id;
		}

		// Fetch data type of dataset
		data_type = H5Dget_type(dataset_id);

		if (data_type == H5I_INVALID_HID) {
			if (DEBUG)
    			fprintf(log_ptr, "Error fetching type of dataset %d\n", dataset_id);
			return data_type;
		}

		// Create null dataspace for shallow copy
		data_space = H5Dget_space(dataset_id);

		if (data_space == H5I_INVALID_HID) {
			if (DEBUG)
    			fprintf(log_ptr, "Error fetching data space of dataset %d\n", dataset_id);
			return data_space;
		}

		// Create dataset in destination file
		hid_t dest_dataset_id = H5Dcreate(*dest_parent_object_id, object_name, data_type, data_space, H5P_DEFAULT, H5Dget_create_plist(dataset_id), H5P_DEFAULT);

		if (dest_dataset_id < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error creating shallow copy of dataset %s. dest_parent_object_id is %d data_type %d dataspace %d\n", name, *dest_parent_object_id, data_type, data_space);
			return dest_dataset_id;
		}

		// Create a scalar dataspace for the attribute.
	    hid_t attr_dataspace_id = H5Screate(H5S_SCALAR);

	    // Create an attribute to indicate that the dataset is empty.
	    hid_t attr_id = H5Acreate2(dest_dataset_id, "CARVED_DATASET_IS_EMPTY", H5T_NATIVE_HBOOL, attr_dataspace_id, 
	                               H5P_DEFAULT, H5P_DEFAULT);

	    hbool_t is_empty = true;
	    H5Awrite(attr_id, H5T_NATIVE_HBOOL, &is_empty);
	// If object is a group, make shallow copy of the group and recursively go down the tree
	} else if (object_type == H5I_GROUP) {
		// Create group in destination file
		hid_t dest_group_id = H5Gcreate1(*dest_parent_object_id, name, size_of_name_buffer);

		if (dest_group_id < 0) {
			if (DEBUG)
    			fprintf(log_ptr, "Error creating shallow copy of group %s. dest_parent_object_id is %d\n", name, *dest_parent_object_id);
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

	if (is_netcdf4 != NULL && use_carved != NULL) {
		char *filename_copy = malloc(strlen(filename) + 1);
		strcpy(filename_copy, filename);
		filename_copy[strlen(filename_copy) - 7] = '\0';
		filename = filename_copy;
	}

	char *filename_without_directory_separators = strrchr(filename, '/');

	if (filename_without_directory_separators != NULL) {
	        filename_without_directory_separators = filename_without_directory_separators + 1;
	} else {
	        filename_without_directory_separators = filename;
	}

	char *carved_filename;
	int carved_filename_length_without_suffix;

	if (carved_directory == NULL) {
		carved_filename_length_without_suffix = strlen(filename);
	} else {
		carved_filename_length_without_suffix = strlen(carved_directory) + strlen(filename_without_directory_separators);
	}

	carved_filename = (char *)malloc(carved_filename_length_without_suffix + 7 + 1); // 7 + 1 is for the suffix .carved and null terminating character

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

	return carved_filename;
}

bool does_dataset_exist(hid_t dataset_id) {
	if (!H5Aexists(dataset_id, "CARVED_DATASET_IS_EMPTY")) {
		return true;
	}

	hid_t attr_id = H5Aopen(dataset_id, "CARVED_DATASET_IS_EMPTY", H5P_DEFAULT);

	if (attr_id < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error opening CARVED_DATASET_IS_EMPTY attribute %d\n", dataset_id);
		return attr_id;
	}

	hbool_t is_empty;

	herr_t attribute_read_ret = H5Aread(attr_id, H5T_NATIVE_UINT8, &is_empty);

	if (attribute_read_ret < 0) {
		if (DEBUG)
    		fprintf(log_ptr, "Error reading CARVED_DATASET_IS_EMPTY attribute %d\n", attr_id);
		return attribute_read_ret;
	}

	return !(is_empty == true);
}

bool is_already_recorded(char *filename) {
	for (int i = 0; i < files_opened_current_size; i++) {
		if (strcmp(files_opened[i], filename) == 0) {
			return true;
		}
	}

	return false;
}