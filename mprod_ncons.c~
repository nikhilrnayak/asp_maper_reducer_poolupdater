#include <stdio.h>

#include <pthread.h>

#include <stdlib.h>

#include <fcntl.h>

#include <unistd.h>

#include <string.h>

#include <stdlib.h>

#include <time.h>

#include "reader.c"



#define sleep_delay1 250

#define sleep_delay2 500

#define sleep_delay3 100

#define sleep_delay4 5

#define sleep_delay5 5


/*
#define THREAD_SIZE1 5

#define BUFFER_SIZE1 5

#define THREAD_SIZE2 6

#define BUFFER_SIZE2 6

#define THREAD_SIZE3 1
*/
#define THREAD_SIZE4 1


int THREAD_SIZE1;

int BUFFER_SIZE1;

int THREAD_SIZE2;

int BUFFER_SIZE2;

int THREAD_SIZE3;

char *filename;

pthread_mutex_t mutex_lock1[20];//[ BUFFER_SIZE1 ];			//lock for mapper pool	

pthread_mutex_t mutex_lock2[20];//[ BUFFER_SIZE2 ];			//lock for reducer pool

pthread_mutex_t mutex_lock3;						//lock for summarizer pool while being used by summarizer thread

pthread_mutex_t mutex_lock4;						//lock for summarizer pool while being used by word writer thread

pthread_t thread_prod_id;

pthread_t thread_ncons_id[20];//[ THREAD_SIZE1 ];

pthread_t thread_mcons_id[20];//[ THREAD_SIZE2 ];

pthread_t thread_pcons_id[20];//[ THREAD_SIZE3 ];

pthread_t thread_qcons_id;

pthread_t thread_ocons_id;

pthread_cond_t cond_var1[20];//[ BUFFER_SIZE1 ];

pthread_cond_t cond_var2[20];//[ BUFFER_SIZE2 ];

pthread_cond_t cond_var3;

pthread_cond_t cond_var4;

volatile char flag_read_buf_slot1[20];//[ BUFFER_SIZE1 ];

volatile char flag_read_buf_slot2[20];//[ BUFFER_SIZE2 ];

volatile char flag_read_buf_slot3 = 'E';

volatile char flag_read_buf_slot4 = 'R';

mapper_pool_entry_type **read_buf1;

mapped_s **read_buf2;

long **read_buf3;

long *read_buf4;

long *letter_count[ 50 ];

int letter_count_index = -1;

long *last_count;

char eof_flag = 'F';

char eof_flag2 = 'F';

int eof_not[20];//[ THREAD_SIZE1 ];

char eof_flag3 = 'F';

char eof_flag4 = 'F';

int item_count = 0;

int item_count2 = 0;

int item_count3 = 0;

int file_reader;

int token = 0;

int max_rank = 0;



void *producer(void *);				//Mapper Pool Updater

void *consumer(void *);				//Mapper

void *consumer2(void *);			//Reducer

void *consumer3(void *);			//summarizer

void *consumer4(void *);			//word writer

void *consumer5(void *);			//letter writer


void main(int argc, char **argv)

{

	int i = 0, rc;
	filename = argv[1];
	THREAD_SIZE1 = atoi(argv[2]);							//get command line args
	BUFFER_SIZE1 = THREAD_SIZE1;
	THREAD_SIZE2 = atoi(argv[3]);
	BUFFER_SIZE2 = THREAD_SIZE2;
	THREAD_SIZE3 = atoi(argv[4]);
	pthread_attr_t thread_attr;

	pthread_attr_init( &thread_attr );

	pthread_attr_setdetachstate( &thread_attr, PTHREAD_CREATE_JOINABLE );

	file_reader = open( "read.txt", O_RDONLY );
	

	read_buf4 = (long *)malloc( sizeof( long ));

	read_buf1 = ( mapper_pool_entry_type ** ) malloc ( BUFFER_SIZE1 * sizeof( mapper_pool_entry_type * ) );

	read_buf2 = ( mapped_s ** )malloc( BUFFER_SIZE2 * sizeof( mapped_s * ) );

	last_count = ( long * )malloc( 50 * sizeof( long ) );

	read_buf3 = ( long ** )malloc( 50 * sizeof( long * ) );

	for( i = 0; i < 50; i++ )

		last_count[ i ] = -1;

	for( i = 0; i < BUFFER_SIZE1; i++ )

	{

		pthread_mutex_init( &mutex_lock1[ i ], NULL );

		pthread_cond_init( &cond_var1[ i ], NULL );

		flag_read_buf_slot1[ i ] = 'E';						// 'F' tells that the buffer is full and 'E' tells the buffer is empty.

		eof_not[ i ] = 0;

	}

	for( i = 0; i < BUFFER_SIZE2; i++ )

	{

		pthread_mutex_init( &mutex_lock2[ i ], NULL );

		pthread_cond_init( &cond_var2[ i ], NULL );

		flag_read_buf_slot2[ i ] = 'E';						// 'F' tells that the buffer is full and 'E' tells the buffer is empty.

	}		

	pthread_mutex_init( &mutex_lock3, NULL );

	pthread_cond_init( &cond_var3, NULL );
	
	rc = pthread_create( &thread_ocons_id, &thread_attr, consumer5, (void *)0);

	rc = pthread_create( &thread_qcons_id, &thread_attr, consumer4, (void *)0);
	
	for( i = 0; i < 1; i++ )
	{
		rc= pthread_create( &thread_pcons_id[ i ], &thread_attr, consumer3, (void *)i );
	}
	for( i = 0; i < THREAD_SIZE2; i++ )

	{

		rc = pthread_create( &thread_mcons_id[ i ], &thread_attr, consumer2, (void *)i );

	}

	for( i = 0; i < THREAD_SIZE1; i++ )

	{

		rc = pthread_create( &thread_ncons_id[ i ], &thread_attr, consumer, (void *)i );

	}	

	rc = pthread_create( &thread_prod_id, &thread_attr, producer, (void *)i );

	pthread_attr_destroy( &thread_attr );

	pthread_exit( (void *) 0 );

}



void *producer( void *argv )

{

	char *read_char = ( char * )malloc( sizeof( char ) );

	char my_eof_flag = 'F';

	mapper_pool_entry_type *sender;

	mapped_s *temp_buf;					

	int count = 0;

	int i, increase = 0;

	int elems = reader( filename );							//call to file reader and sorting lib

	int myrank = 0;

	struct timespec tns;

	//printf("elems is %d\n", elems);

	for(i = 0; i < BUFFER_SIZE1; i++)

	{	

		if( count < elems )

		{

			temp_buf = store_buf[ count ];

			*read_char = *(((char *)temp_buf->word) + 0 );

			sender = (mapper_pool_entry_type *)malloc( sizeof( *sender ) );

			sender->rank = myrank;

			sender->tray[ 0 ] = temp_buf->word ;

			temp_buf = store_buf[ count ];

			increase = 0;

		}

		while( count < elems )

		{

			count++;

			increase++;

			temp_buf = store_buf[ count ];

			if( count == elems )

			{

				sender->tray[ increase ] = NULL;

				my_eof_flag = 'T';

				break;

			}

			if( *read_char == *( ( (char *) temp_buf->word) + 0 ) && *( ( (char *) temp_buf->word) + 0 ) != NULL )

			{

				sender = realloc(sender, sizeof( *sender ) + ( increase + 1 ) * sizeof( char * ) );

				sender->tray[ increase ] = temp_buf->word;

				sender->tray[ increase + 1 ] = NULL;

			}

			else 

			{

				sender->tray[ increase ] = NULL;

				break;

			}

		}				

		max_rank = myrank;

		myrank++;

		if( sender != NULL )

		{

			pthread_mutex_lock( &mutex_lock1[ i ] );

				clock_gettime(CLOCK_REALTIME, &tns);

				tns.tv_nsec += sleep_delay1;

				while( flag_read_buf_slot1[ i ] == 'F' )

					pthread_cond_timedwait( &cond_var1[ i ], &mutex_lock1[ i ], &tns );

				read_buf1[ i ] = sender;

				//printf("rank %d size %d string %s\n", sender->rank, strlen(sender->tray)/sizeof(char *), (char *)sender->tray[ 0 ] );

				item_count++;

				flag_read_buf_slot1[ i ] = 'F';

				pthread_cond_signal( &cond_var1[ i ] );

				sender = NULL;

			pthread_mutex_unlock( &mutex_lock1[ i ] );

		}

		if( i == BUFFER_SIZE1 - 1 )

			i = -1;

		if( my_eof_flag == 'T' )

			break;

	}

	eof_flag = 'T';

	pthread_exit( ( void * ) 0 );

	

}



void *consumer( void *argv )

{

	int id = (int)argv;

	int n = 0;

	int i = 0;

	int flg_sig = 1;

	int mapped_count = 0, word_count = 0;

	char anomaly = 'F';

	mapped_s *temp_buf;

	struct timespec tns;

	struct timespec tns2;

	mapper_pool_entry_type **temp_store = (mapper_pool_entry_type **)malloc( 2 * sizeof ( mapper_pool_entry_type * ) );

	temp_store[ 1 ] = NULL;

	temp_store[ 0 ] = NULL;

	for(;;)

	{

		while( item_count <= 0 && eof_flag == 'F' && anomaly == 'F' );

		pthread_mutex_lock( &mutex_lock1[ id ] );

			clock_gettime(CLOCK_REALTIME, &tns2);

			tns2.tv_nsec += sleep_delay2;

			while( flag_read_buf_slot1[ id ] != 'F' )

			{

				if( ( item_count <= 0 && eof_flag == 'T' ) || flag_read_buf_slot1[ id ] == 'E' )

					break;

				pthread_cond_timedwait( &cond_var1[ id ], &mutex_lock1[ id ], &tns2);

			}

			if( flag_read_buf_slot1[ id ] == 'F' && read_buf1[ id ] != NULL )

			{

				temp_store[0] = read_buf1[ id ];

				//printf(/*string*/ "%s\n" /*rank %d\n*/, temp_store[ 0 ]->tray[ 0 ]/*, temp_store[ 0 ]->rank*/);

				read_buf1[ id ] = NULL;

				item_count--;

				flag_read_buf_slot1[ id ] = 'E';

			}
/*
			if( read_buf1[ id ] == NULL && item_count == 1 && eof_flag == 'T' )

			{

				anomaly = 'T';

				item_count--;

				flag_read_buf_slot1[ id ] = 'E';

			}

			if( flag_read_buf_slot1[ id ] == 'F' && read_buf1[ id ] == NULL && item_count == 0 && anomaly == 'T' )

				flag_read_buf_slot1[ id ] == 'E';
*/
			pthread_cond_signal( &cond_var1[ id ] );

		pthread_mutex_unlock( &mutex_lock1[ id ] );

		if( temp_store[0] !=  NULL )

		{

			word_count = 0;

			mapped_count = 0;

			while( temp_store[ 0 ]->tray[word_count] != NULL)

				word_count++;

			while( mapped_count < word_count )

			{

				temp_buf = ( mapped_s * )malloc( sizeof( *temp_buf ) + strlen( temp_store[ 0 ]->tray[ mapped_count ] ) * sizeof( char ) );

				temp_buf->count = 1;

				temp_buf->rank =  temp_store[ 0 ]->rank;

				strcpy( temp_buf->word, temp_store[ 0 ]->tray[ mapped_count ] );

				mapped_count++;

				pthread_mutex_lock( &mutex_lock2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] );

					clock_gettime(CLOCK_REALTIME, &tns);

					tns.tv_nsec += sleep_delay2;

					while( flag_read_buf_slot2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] == 'F')

					{

						if( flag_read_buf_slot2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] == 'E' )

							break;

						pthread_cond_timedwait( &cond_var2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ], &mutex_lock2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ], &tns );

					}

					if( flag_read_buf_slot2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] == 'E' )

					{

						//printf(/*%d, %d consumer - rank %d string*/ "%s\n", /*id, ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2,temp_buf->rank,*/temp_buf->word );

						read_buf2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] = temp_buf;

						temp_buf = NULL;

						item_count2++;

						flag_read_buf_slot2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] = 'F';  

					}

					pthread_cond_signal( &cond_var2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] );			

				pthread_mutex_unlock( &mutex_lock2[ ( id + ( n * THREAD_SIZE1 ) ) % THREAD_SIZE2 ] );

			}

			n++;

			

		}

		if( item_count <= 0 && eof_flag == 'T' )

			break;

		temp_store[0] = NULL;

	}

	free(temp_store);

	eof_not[ id ] = 1;

	for( i = 0; i < BUFFER_SIZE1; i++)

		flg_sig *= eof_not[ i ];

	if( flg_sig == 1 )

		eof_flag2 = 'T';

	pthread_exit( ( void * ) 0 );

}



void *consumer2( void *argv )

{

	struct timespec tns;

	struct timespec tns3;

	int id = (int)argv;

	int i1, j1;

	char temp_hold[ 20 ] = "\0";

	int counter = 0, myrank = 0;

	mapped_s *holder = (mapped_s *)malloc( sizeof ( mapped_s * ) );

	for(;;)

	{

		while( item_count2 <= 0 && eof_flag2 == 'F' );

		pthread_mutex_lock( &mutex_lock2[ id ] );

			clock_gettime( CLOCK_REALTIME, &tns );

			tns.tv_nsec += sleep_delay2;

			while( flag_read_buf_slot2[ id ] == 'E' && eof_flag2 == 'F' && item_count2 > 0 )

			{	

				if( ( item_count2 <= 0 && eof_flag2 == 'T' ) || flag_read_buf_slot2[ id ] == 'F' )

					break;

				pthread_cond_timedwait( &cond_var2[ id ], &mutex_lock2[ id ], &tns );

			}

			if( flag_read_buf_slot2[ id ] == 'F' && read_buf2[ id ]->word != NULL )

			{

				//printf("thread %d, rank %d - string %s\n", id, read_buf2[id]->rank, read_buf2[ id ]->word);

				holder = read_buf2[ id ];

				read_buf2[ id ] = NULL;

				item_count2--;

				flag_read_buf_slot2[ id ] = 'E';

			}

			pthread_cond_signal( &cond_var2[ id ] );

		pthread_mutex_unlock( &mutex_lock2[ id ]);

		if( holder != NULL && strcmp( holder->word, "\0" ) != 0 )

		{

			if( last_count[ holder->rank ] == -1 )

			{

				last_count[ holder->rank ]++;

				read_buf3[ holder->rank ] = realloc(read_buf3[holder->rank], sizeof(*read_buf3[holder->rank]) + (last_count[ holder->rank ] + 1 ) * sizeof( long * ) );

				*( read_buf3[ holder->rank ] + last_count[ holder->rank ] ) = ( long )holder;

			}

			else

			{

				if( strcmp( holder->word, ((mapped_s *)(*(read_buf3[ holder->rank ] + last_count[ holder->rank] )))->word ) == 0 )

				{

					((mapped_s *)(*(read_buf3[ holder->rank ] + last_count[ holder->rank])))->count++;

				}

				else

				{

					last_count[ holder->rank ]++;

					read_buf3[ holder->rank ] = realloc(read_buf3[holder->rank], sizeof(*read_buf3[holder->rank]) + (last_count[ holder->rank ] + 1 ) * sizeof( long * ) );

					*( read_buf3[ holder->rank ] + last_count[ holder->rank ] ) = ( long )holder;

				}

			}

			holder = NULL;				

		}



		if( item_count2 <= 0 && eof_flag2 == 'T' )

			break;

	}

	if( id == 0 )

	{

		//printf("=======================================================================================================\n");

		for( i1= 0; i1 <= max_rank; i1++ )

		{

			for(j1 = 0; j1 <= last_count[ i1 ]; j1++)

			{

				//printf("%d,%s\n", ( ( mapped_s * )( *(read_buf3[ i1 ]+j1) ) )->count, ( ( mapped_s * )( *( read_buf3[ i1 ] + j1 ) ) )->word );

				pthread_mutex_lock( &mutex_lock3 );

					clock_gettime(CLOCK_REALTIME, &tns3);

					tns3.tv_nsec += sleep_delay3;
					while( flag_read_buf_slot3 != 'E' )

					{
						if( flag_read_buf_slot3 == 'E' )

							break;

						pthread_cond_timedwait( &cond_var3, &mutex_lock3, &tns3);

					}
					if( flag_read_buf_slot3 == 'E' && ( ( mapped_s * )( *( read_buf3[ i1 ] + j1 ) ) ) != NULL )

					{

						item_count3++;

						read_buf4 = ( mapped_s * )( *( read_buf3[ i1 ] + j1 ) );

						//printf("%d,%s\n", ((mapped_s *)read_buf4)->count, ((mapped_s *)read_buf4)->word );

						flag_read_buf_slot3 = 'F';

					}

					pthread_cond_signal( &cond_var3 );

				pthread_mutex_unlock( &mutex_lock3 );

			}

		}

		eof_flag3 = 'T';

	}

	//printf("flag cons2 %c %d\n", eof_flag2, item_count2);

	//printf("reducer stage exited %d\n", id);

	pthread_exit( (void *) 0 );

}


void *consumer3( void *argv)

{

	char last_char = NULL;

	struct timespec tns3;
	
	struct timespec tns4;

	mapped_s *letter_stat_holder;

	//printf("Entered summarizer\n");

	int i;

	while( 1 )

	{

		while( item_count3 <= 0 && eof_flag3 == 'F' );

		if( item_count3 <= 0 && eof_flag3 == 'T' )

			break;

		pthread_mutex_lock( &mutex_lock3 );

				clock_gettime(CLOCK_REALTIME, &tns3);

				tns3.tv_nsec += sleep_delay3;

				while( flag_read_buf_slot3 != 'F' )

				{

					if( flag_read_buf_slot3 == 'F' && ( item_count3 <= 0 && eof_flag3 == 'T' ) )

						break;

					pthread_cond_timedwait( &cond_var3, &mutex_lock3, &tns3);

				}

				if( flag_read_buf_slot3 == 'F' && read_buf4 != NULL )

				{

					if( (char)(*(((mapped_s *)read_buf4)->word ) + 0) != last_char )

					{

						letter_count_index++;

						letter_stat_holder = (mapped_s *)malloc( sizeof( *letter_stat_holder ));

						letter_stat_holder->letter = (char)(*(((mapped_s *)read_buf4)->word ) + 0);

						letter_stat_holder->count = ((mapped_s *)read_buf4)->count;

						letter_count[ letter_count_index ] = letter_stat_holder;
						last_char = (char)(*(((mapped_s *)read_buf4)->word ) + 0);
						//printf("(%c, %d)\n", letter_stat_holder->letter, letter_stat_holder->count);
						pthread_mutex_lock( &mutex_lock4 );
							flag_read_buf_slot4 = 'N';
							clock_gettime( CLOCK_REALTIME, &tns4);
							tns4.tv_nsec += sleep_delay4;
							while( flag_read_buf_slot4 == 'N' )
							{
								pthread_cond_wait( &cond_var4, &mutex_lock4);//, &tns4 );
							}
						pthread_mutex_unlock( &mutex_lock4 );

						item_count3--;
						flag_read_buf_slot3 = 'E';

					}

					else

					{

						((mapped_s *)letter_count[ letter_count_index ])->count += ((mapped_s *)read_buf4)->count;
						pthread_mutex_lock( &mutex_lock4 );
							flag_read_buf_slot4 = 'N';
							clock_gettime( CLOCK_REALTIME, &tns4);
							tns4.tv_nsec += sleep_delay4;
							while( flag_read_buf_slot4 == 'N' )
							{
								pthread_cond_wait( &cond_var4, &mutex_lock4);//, &tns4 );
							}
						pthread_mutex_unlock( &mutex_lock4 );						
						item_count3--;
						flag_read_buf_slot3 = 'E';

					}
				}

				pthread_cond_signal( &cond_var3 );

		pthread_mutex_unlock( &mutex_lock3 );

	}
	eof_flag4 = 'T';

	pthread_exit(NULL);

}

void *consumer4( void *argv)

{

	struct timespec tns5;

	//mapped_s *word_writer;
	FILE *fp = fopen("./write.txt","w");

	//printf("Entered word writer\n");


	for(;;)

	{
		while( item_count3 <= 0 && eof_flag3 == 'F' );

		if( eof_flag4 == 'T' )
			break;
		pthread_mutex_lock( &mutex_lock4 );
			clock_gettime(CLOCK_REALTIME, &tns5);

			tns5.tv_nsec += sleep_delay5;

			while( flag_read_buf_slot4 != 'N' )

			{
				if( eof_flag4 == 'T' )

					break;

				pthread_cond_timedwait( &cond_var4, &mutex_lock4, &tns5);

			}
			fprintf(fp,"(%s, %d)\n", ((mapped_s *)read_buf4)->word, ((mapped_s *)read_buf4)->count);
			flag_read_buf_slot4 = 'R';
			pthread_cond_signal( &cond_var4 );

		pthread_mutex_unlock( &mutex_lock4 );

	}
	fclose(fp);
	//printf("Exited writer\n");

	pthread_exit(NULL);

}

void *consumer5(void *argv)
{
	int i;
	FILE *fp = fopen("letter.txt","w");
	while( eof_flag4 == 'F' );
	for( i = 0; i < letter_count_index + 1 ; i++ )
	{
		fprintf(fp, "(%c, %d)\n", ((mapped_s *)letter_count[ i ])->letter, ((mapped_s *)letter_count[ i ])->count);
	}
	pthread_exit(NULL);
}
