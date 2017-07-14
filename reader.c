#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "lib_map_reduce.h"	//lib provides the sorting function and buffer sizes

//int store_buf[STORE_BUF_SIZE];
mapped_s *mapped_i;		//struct defined in the lib lib_map_reduce_types.h

int mapper(long *, int);		//performs the mapping operation	-	breaks the lines into words

int reader(char *filename)		//collects lines from the file
{
	long store_p[P_SIZE];
	int i = 0, j = 0, word_count;
	char buf[2];				//buffer to read the file
	char *count = (char *)malloc(sizeof(mapped_s *)), *store = (char *)malloc(sizeof(char));	//buffers for storing and printing results
	int file_t = open(filename, O_RDONLY);							//opens a handle to the file read.txt
	buf[1] = NULL;
	do												//this section reads entire lines from file
	{
		buf[0] = NULL;
		read(file_t, buf, 1);
		if(*buf == '\n' || *buf == NULL)
		{
			*(store + j) = NULL;			
			store_p[i] = (long)store;	//if the no. of lines exceed the prog capacity increase the size of store_p in lib_map_reduce.h
			i++;
			j = 0;
			store = (char *)malloc(sizeof(char));
		}
		else if(buf[0] != NULL)									//store lines in to store buf
		{
			realloc(store, sizeof(*store) + (j + 1)*sizeof(char));
			*(store + j) = buf[0];
			j++;
		}
		else
			break;
	}while(buf[0] != NULL);										//exit upon eof
	word_count = mapper(store_p, i - 1);								//call mapper
	sort(store_buf, 0, word_count - 1);							//call sort defined in lib_map_reduce.c
/*
	for(j = 0; j < word_count; j++)									//print sorted elements on terminal
	{
		mapped_i = (mapped_s *)store_buf[j];
		realloc(count, sizeof(mapped_i->word));
		write(1, "(", strlen("("));
		write(1, mapped_i->word, strlen(mapped_i->word));
		sprintf(count, ", %d)\n", mapped_i->count);
		write(1, count, strlen(count));
	}
*/
	//printf("Exiting reader\n");
	return word_count;
}

int mapper(long *store_p, int i)
{
	int j, k, l, m = 0, n;
	for(j = 0; j < i; j++)
	{	
		l = 0;
		while((char)*((char *)*(store_p + j) + l) != '\n' && (char)*((char *)*(store_p + j) + l) != NULL )
		{
			if((char)*((char *)*(store_p + j) + l) != ' ')
			{
				mapped_i = (mapped_s *)malloc(sizeof(*mapped_i));
				mapped_i->count = 1;
				store_buf[m] = mapped_i;
				m++;
				n = 0;
			}
			for(k = l; *((char *)*(store_p + j) + k) != ' ' && *((char *)*(store_p + j) + k) != '\n' && *((char *)*(store_p + j) + k) != NULL; k++)
			{
				realloc(mapped_i, sizeof(*mapped_i) + (n+1)*sizeof(char));
				*(mapped_i->word+n) = *((char *)*(store_p + j) + k);		
				l = k;
				n++;
				*((char *)*(store_p + j) + k) = NULL;
				*(mapped_i->word+n) = NULL;
			}
			l++;
			if(*((char *)*(store_p + j) + l) == '\n' || *((char *)*(store_p + j) + l) == NULL)
				break;
		}
	}
	for(j = 0; j < i; j++)
	{
		free((void *)store_p[j]);
		store_p[j] = NULL;
	}
	return m;
}
