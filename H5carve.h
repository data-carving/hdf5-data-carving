#ifndef H5CARVE_H
#define H5CARVE_H

// Functions being interposed on include H5Fopen, H5Dread, and H5Oopen.
herr_t (*original_H5Dread)(hid_t, hid_t, hid_t, hid_t, hid_t, void*);
hid_t (*original_H5Fopen)(const char *, unsigned, hid_t);
hid_t (*original_H5Oopen)(hid_t, const char *, hid_t);
int (*original_nc_open)(const char *path, int omode, int *ncidp);
void (*original_H5_term_library)(void);

// Global variables to be used across function calls
char *use_carved;
extern hid_t src_file_id;
extern hid_t dest_file_id;
extern hid_t original_file_id;
char *is_netcdf4; // TODO: replace with an robust automatic check i.e. some kind of byte encoding 
extern char **files_opened;
extern int files_opened_current_size;
extern FILE *log_ptr;
extern char *DEBUG;

#endif