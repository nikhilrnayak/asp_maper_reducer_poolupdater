#ifndef lib_map_reduce_types_h
#define lib_map_reduce_types_h

struct mapped
{
	int rank;
	int count;
	char letter;
	char word[1];
};
#define mapped_s struct mapped

struct mapper_pool_entry_type
{
	int rank;
	char *tray[1];
};

#define mapper_pool_entry_type struct mapper_pool_entry_type

#endif
