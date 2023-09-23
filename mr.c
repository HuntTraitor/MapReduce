#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

struct thread {
  kvlist_t* input;
  kvlist_t* output;
  mapper_t mapper;
  reducer_t reducer;
};

void* mapper_thread(void* thread);
void* reducer_thread(void* thread);

void* mapper_thread(void* thread) {
  struct thread* my_thread = (struct thread*)thread;
  mapper_t mapper = *my_thread->mapper;
  kvlist_iterator_t* itr = kvlist_iterator_new(my_thread->input);
  while (1) {
    kvpair_t* pair = kvlist_iterator_next(itr);
    if (pair == NULL) {
      break;
    }
    mapper(pair, my_thread->output);
  }
  kvlist_iterator_free(&itr);
  return NULL;
}

void* reducer_thread(void* thread) {
  struct thread* my_thread = (struct thread*)thread;
  reducer_t reducer = *my_thread->reducer;
  kvlist_sort(my_thread->input);
  kvpair_t* comp_pair;
  kvlist_t* temp_list = kvlist_new();

  kvlist_iterator_t* itr = kvlist_iterator_new(my_thread->input);
  kvpair_t* pair = kvlist_iterator_next(itr);
  if (pair != NULL) {
    kvlist_append(temp_list, kvpair_clone(pair));
    comp_pair = kvpair_clone(pair);
  } else {
    kvlist_iterator_free(&itr);
    kvlist_free(&temp_list);
    return NULL;
  }

  while (1) {
    kvpair_t* pair = kvlist_iterator_next(itr);

    if (pair == NULL) {
      kvlist_iterator_t* temp_list_itr = kvlist_iterator_new(temp_list);
      kvpair_t* temp_list_pair = kvlist_iterator_next(temp_list_itr);
      reducer(temp_list_pair->key, temp_list, my_thread->output);
      kvlist_iterator_free(&temp_list_itr);
      kvlist_free(&temp_list);
      break;
    }

    if (strcmp(pair->key, comp_pair->key) == 0) {
      kvlist_append(temp_list, kvpair_clone(pair));
    } else {
      kvlist_iterator_t* temp_list_itr = kvlist_iterator_new(temp_list);
      kvpair_t* temp_list_pair = kvlist_iterator_next(temp_list_itr);
      reducer(temp_list_pair->key, temp_list, my_thread->output);
      kvlist_iterator_free(&temp_list_itr);
      kvlist_free(&temp_list);
      temp_list = kvlist_new();
      kvlist_append(temp_list, kvpair_clone(pair));
      kvpair_free(&comp_pair);
      comp_pair = kvpair_clone(pair);
    }
  }
  kvpair_free(&comp_pair);
  kvlist_iterator_free(&itr);
  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  /*SPLIT
   * PHASE----------------------------------------------------------------------------*/
  // initializing array of lists
  kvlist_t** list = calloc(num_mapper, sizeof(kvlist_t*));
  for (unsigned long i = 0; i < num_mapper; i++) {
    list[i] = kvlist_new();
  }

  // filling those lists
  kvlist_iterator_t* itr1 = kvlist_iterator_new(input);
  unsigned long count = 0;
  while (1) {
    kvpair_t* pair = kvlist_iterator_next(itr1);

    if (pair == NULL) {
      break;
    }
    kvlist_append(list[count], kvpair_clone(pair));
    count++;
    if (count == num_mapper) {
      count = 0;
    }
  }
  kvlist_iterator_free(&itr1);

  /*MAP
   * PHASE------------------------------------------------------------------------------*/

  kvlist_t** mapped_list = calloc(num_mapper, sizeof(kvlist_t*));

  for (unsigned long i = 0; i < num_mapper; i++) {
    mapped_list[i] = kvlist_new();
  }

  pthread_t maps[num_mapper];
  struct thread* mapped_threads = malloc(sizeof(struct thread) * num_mapper);
  for (unsigned long i = 0; i < num_mapper; i++) {
    mapped_threads[i].input = list[i];
    mapped_threads[i].output = mapped_list[i];
    mapped_threads[i].mapper = mapper;
    pthread_create(&maps[i], NULL, mapper_thread, &mapped_threads[i]);
  }

  for (unsigned long i = 0; i < num_mapper; i++) {
    pthread_join(maps[i], NULL);
  }

  /*SHUFFLE
   * PHASE--------------------------------------------------------------------------*/

  // take mod num_reducer on every list to put similar in each other

  kvlist_t** shuffled_list = calloc(num_reducer, sizeof(kvlist_t*));

  for (unsigned long i = 0; i < num_reducer; i++) {
    shuffled_list[i] = kvlist_new();
  }

  for (unsigned long i = 0; i < num_mapper; i++) {
    kvlist_iterator_t* itr3 = kvlist_iterator_new(mapped_list[i]);
    while (1) {
      kvpair_t* pair = kvlist_iterator_next(itr3);

      if (pair == NULL) {
        break;
      }
      kvlist_append(shuffled_list[hash(pair->key) % num_reducer],
                    kvpair_clone(pair));
    }
    kvlist_iterator_free(&itr3);
  }

  /*REDUCER
   * PHASE-------------------------------------------------------------------------------*/

  pthread_t reducers[num_reducer];

  struct thread* reduced_threads = malloc(sizeof(struct thread) * num_reducer);

  kvlist_t** reduced_list = calloc(num_reducer, sizeof(kvlist_t*));

  for (unsigned long i = 0; i < num_reducer; i++) {
    reduced_list[i] = kvlist_new();
  }

  for (unsigned long i = 0; i < num_reducer; i++) {
    reduced_threads[i].input = shuffled_list[i];
    reduced_threads[i].output = reduced_list[i];
    reduced_threads[i].reducer = reducer;
    pthread_create(&reducers[i], NULL, reducer_thread, &reduced_threads[i]);
  }

  for (unsigned long i = 0; i < num_reducer; i++) {
    pthread_join(reducers[i], NULL);
  }

  /*FINAL
   * PHASE---------------------------------------------------------------------------------*/

  for (unsigned long i = 0; i < num_reducer; i++) {
    kvlist_extend(output, reduced_list[i]);
  }

  for (unsigned long i = 0; i < num_mapper; i++) {
    kvlist_free(&list[i]);
    kvlist_free(&mapped_list[i]);
  }
  for (unsigned long i = 0; i < num_reducer; i++) {
    kvlist_free(&shuffled_list[i]);
    kvlist_free(&reduced_list[i]);
  }

  free(list);
  free(mapped_list);
  free(shuffled_list);
  free(reduced_list);
  free(reduced_threads);
  free(mapped_threads);
}
