#include <stdio.h>
#include <string.h>
#include "lib_map_reduce.h"

void sort(long *f_store_buf, int start, int end)					//quick sort for arranging alphabetically - is case sensitive
{
	if(start < end)
	{
		int pivot = end, p_index = start, i, j, limit, temp;
		mapped_s *mapped_v1 = (mapped_s *)f_store_buf[pivot];
		mapped_s *mapped_v2, *mapped_v3;
		for(i = start; i < end + 1; i++)
		{ 
			mapped_v3 = (mapped_s *)f_store_buf[p_index];
			mapped_v2 = (mapped_s *)f_store_buf[i];
			limit = (strlen((char *)mapped_v3->word) < strlen((char *)mapped_v1->word)) ? strlen((char *)mapped_v3->word) : strlen((char *)mapped_v1->word);
			for(j = 0; j < limit; j++)
			{
				if(mapped_v2->word[j] < mapped_v1->word[j])
				{
					f_store_buf[i] = mapped_v3;
					f_store_buf[p_index] = mapped_v2;
					p_index++;
					break;
				}
				else if(mapped_v2->word[j] == mapped_v1->word[j] && strlen((char *)mapped_v2->word) <= strlen((char *)mapped_v1->word))
				{
					if(j == limit - 1)
					{
						f_store_buf[i] = mapped_v3;
						f_store_buf[p_index] = mapped_v2;
						p_index++;
						break;
					}
				}
				else if(mapped_v2->word[j] == mapped_v1->word[j] && strlen((char *)mapped_v2->word) > strlen((char *)mapped_v1->word));
				else
					break;
			}
			if(p_index > 0)
				pivot = p_index - 1;
		}
		sort(f_store_buf, start, pivot-1);
		sort(f_store_buf, pivot+1, end);
	}
}
