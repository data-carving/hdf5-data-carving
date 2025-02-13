#ifndef H5CARVE_HELPER_FUNCTIONS_H
#define H5CARVE_HELPER_FUNCTIONS_H

hobj_ref_t *copy_reference_object(hobj_ref_t *source_ref, int num_elements, hid_t src_attribute_id);
herr_t copy_compound_type(hid_t src_id, void *src_buffer, void *dest_buffer, hid_t data_type, int num_elements, int num_members, size_t starting_offset);
hvl_t *copy_vlen_type(hid_t src_attribute_id, hid_t data_type, hvl_t *src_data, int num_elements);
herr_t copy_attributes(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata);
int copy_object_attributes(hid_t loc_id, const char *name, const H5A_info_t *ainfo, void *opdata);
herr_t delete_attributes(hid_t loc_id, const char *name, const H5A_info_t *ainfo, void *opdata);
bool is_already_recorded(const char *filename);
herr_t shallow_copy_object(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata);
char *get_carved_filename(const char *filename, char *is_netcdf4, char *use_carved);
bool does_dataset_exist(hid_t dataset_id);
herr_t create_fallback_metadata(const char *filename, hid_t destination_root_group);
// hsize_t get_total_num_elems_and_base_type(hid_t type_id, hid_t *base_type_id);
hsize_t get_total_num_elems_and_base_type(hid_t type_id, hid_t *base_type_id);
// void create_array_of_references(hid_t src_attribute_id, hid_t dest_attribute_id, hid_t array_dtype_copy, H5R_ref_t *src_data, H5R_ref_t *head_dest_data, H5R_ref_t *current_dest_data, int total_elements);
H5R_ref_t* copy_reference_object_H5R_ref_t(hid_t src_attribute_id, hid_t dest_file_id, hid_t attribute_data_type, size_t total_elements, H5R_ref_t *src_data);
void *copy_array(hid_t src_attribute_id, void *src_data, hid_t attribute_data_type, hid_t base_type_id, int total_elements);

typedef enum {
    LOCAL,
    REMOTE,
} fallback_enum;

#endif