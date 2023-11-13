#ifndef H5CARVE_HELPER_FUNCTIONS_H
#define H5CARVE_HELPER_FUNCTIONS_H

herr_t copy_attributes(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata);
herr_t shallow_copy_object(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata);
void count_objects_in_group(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata);
char *get_carved_filename(const char *filename);
bool is_dataset_null(hid_t dataset_id);

#endif