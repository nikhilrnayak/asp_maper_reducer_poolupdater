lib = -lrt -pthread -w

all: mprod_ncons.c reader.c lib_map_reduce.c lib_map_reduce.h lib_map_reduce_types.h
	cc  mprod_ncons.c -o mprod_ncons $(lib)


clean:
	rm mprod_ncons letter.txt write.txt

