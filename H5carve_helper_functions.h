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
void count_objects_in_group(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata);
char *get_carved_filename(const char *filename, char *is_netcdf4, char *use_carved);
bool does_dataset_exist(hid_t dataset_id);

#endif