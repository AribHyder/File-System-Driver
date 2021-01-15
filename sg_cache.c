////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : Agha Arib Hyder
//   Last Modified : 12/11/20
//

// Include Files
#include <stdlib.h>
#include <cmpsc311_log.h>

// Project Includes
#include <sg_cache.h>
#include <string.h>

// Defines
// struct to hold metadata and block for each line in the cache
typedef struct cacheline {
    int free;
    int line_num;
    int LRU;
    SG_Node_ID rem_id;
    SG_Block_ID blk_id;
    char *block;
} cacheline_t;
// struct to hold metadata of the entire cache
typedef struct cache {
    int queries;
    int open;
    int num_items;
    int hits;
    float ratio;
    uint16_t size;
    cacheline_t *cache_data;
} cache_t;
// Functional Prototypes
cache_t *cache;
//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : initSGCache
// Description  : Initialize the cache of block elements
//
// Inputs       : maxElements - maximum number of elements allowed
// Outputs      : 0 if successful, -1 if failure

int initSGCache( uint16_t maxElements ) {

    cache = malloc(sizeof(cache_t));
    cache->size = maxElements;

    // initialize values for the cache
    cache->num_items = 0;
    cache->queries = 0;
    cache->hits = 0;
    cache->ratio = 0;
    cache->open = 1;
    cache->cache_data = calloc(maxElements, sizeof(cacheline_t));
    // initialize free value and line numbers of cache, allocate data for cachelines
    for (int i = 0; i < cache->size; i++) {
        cache->cache_data[i].free = 0;
        cache->cache_data[i].block = malloc(SG_BLOCK_SIZE);
        cache->cache_data[i].line_num = i;
    }

    logMessage(LOG_INFO_LEVEL, "init_cmpsc311_cache: initialization complete\n");
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : closeSGCache
// Description  : Close the cache of block elements, clean up remaining data
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

int closeSGCache( void ) {

    if (cache->open == 0) {
        return -1;
    }

    // calculate the hit rate from queries and hits
    cache->open = 0;
    float hitz = (float)(cache->hits);
    float queriez = (float)(cache->queries);
    cache->ratio = (hitz/queriez)*100;
    logMessage(LOG_INFO_LEVEL, "Closing cache: %d queries, %d hits (%.2f%c hit rate).\n", cache->queries, cache->hits, cache->ratio, '%');
    // free cache data
    for (int i = 0; i < cache->size; i++) {
        free(cache->cache_data[i].block); 
    }
    free(cache->cache_data);
    free(cache);
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : getSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
// Outputs      : pointer to block or NULL if not found

char * getSGDataBlock( SG_Node_ID nde, SG_Block_ID blk ) {
    
    cache->queries++;
    char  *current = malloc(SG_BLOCK_SIZE);
    // check if we have the block in the cache and update hits if we do
    for (int i = 0; i < cache->size; i++) {
        if ((cache->cache_data[i].rem_id == nde) && (cache->cache_data[i].blk_id == blk)) {
            cache->hits++;
            memcpy(current, cache->cache_data[i].block, SG_BLOCK_SIZE);
            // if we get a hit, set its LRU to 0 and increment LRUs of all other cache lines
            // the least recently used block will always have the highest LRU value
            cache->cache_data[i].LRU = 0;
            for (int j = 0; j < cache->size; j++) {
                cache->cache_data[j].LRU++;
            }

            logMessage(LOG_INFO_LEVEL, "Getting found cache item: %d length 1024\n", cache->cache_data[i].line_num);
            logMessage(LOG_INFO_LEVEL, "sgDriverObtainBlock: Used cached block [%d], node [%d] in cache.\n", blk, nde); 
            return current;
        }
    }
    
    logMessage(LOG_INFO_LEVEL, "Getting cache item (not found!)\n");
    logMessage(LOG_INFO_LEVEL, "Cache state [%d items, %d bytes used]\n", cache->num_items, (cache->num_items)*1024);

    return NULL;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : putSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
//                block - block to insert into cache
// Outputs      : 0 if successful, -1 if failure

int putSGDataBlock( SG_Node_ID nde, SG_Block_ID blk, char *block ) {
    // first check if we have a previous version of the block stored in the cache to replace
    for (int i = 0; i < cache->size; i++) {
        if ((cache->cache_data[i].rem_id) == nde && (cache->cache_data[i].blk_id == blk)) {
            memcpy(cache->cache_data[i].block, block, SG_BLOCK_SIZE);
            cache->cache_data[i].LRU = 0;

            for (int j = 0; j < cache->size; j++) {
                cache->cache_data[j].LRU++;
            }

            logMessage(LOG_INFO_LEVEL, "Cache state [%d items, %d bytes used]\n", cache->num_items, (cache->num_items)*1024);
            logMessage(LOG_INFO_LEVEL, "Added cache item %d, length 1024\n", cache->cache_data[i].line_num);
            logMessage(LOG_INFO_LEVEL, "Inserted block [%d], node [%d] into cache.\n", blk, nde);
            return 0;
        }
    }
    // check if any lines of the cache are free to place the data block into
    for (int i = 0; i < cache->size; i++) {
        if (cache->cache_data[i].free == 0) {
            cache->cache_data[i].rem_id = nde;
            cache->cache_data[i].blk_id = blk;
            memcpy(cache->cache_data[i].block, block, SG_BLOCK_SIZE);
            cache->cache_data[i].free = 1;
            cache->cache_data[i].LRU = 0;

            for (int j = 0; j < cache->size; j++) {
                cache->cache_data[j].LRU++;
            }
            cache->num_items++;
            logMessage(LOG_INFO_LEVEL, "Cache state [%d items, %d bytes used]\n", cache->num_items, (cache->num_items)*1024);
            logMessage(LOG_INFO_LEVEL, "Added cache item %d, length 1024\n", cache->cache_data[i].line_num);
            logMessage(LOG_INFO_LEVEL, "Inserted block [%d], node [%d] into cache.\n", blk, nde);
            return 0;
        }
    }
    // since we didn't find the block and no cache lines are free, evict LRU block and replace it with the new one
    cacheline_t *current = &cache->cache_data[0];
    
    for (int k = 1; k < cache->size; k++) {
        if (current->LRU < cache->cache_data[k].LRU) {
            current = &cache->cache_data[k];
        }
    }

    logMessage(LOG_INFO_LEVEL, "Ejecting cache item %d, length 1024\n",current->line_num);
    cache->num_items--;
    logMessage(LOG_INFO_LEVEL, "Cache state [%d items, %d bytes used]\n", cache->num_items, (cache->num_items)*1024);
    current->rem_id = nde;
    current->blk_id = blk;
    memcpy(current->block, block, SG_BLOCK_SIZE);
    current->LRU = 0;
    
    for (int j = 0; j < cache->size; j++) {
        cache->cache_data[j].LRU++;
    }

    if (current->block != NULL) {
        cache->num_items++;
        logMessage(LOG_INFO_LEVEL, "Cache state [%d items, %d bytes used]\n", cache->num_items, (cache->num_items)*1024);
        logMessage(LOG_INFO_LEVEL, "Added cache item %d, length 1024\n", current->line_num);
        logMessage(LOG_INFO_LEVEL, "Inserted block [%d], node [%d] into cache.\n", blk, nde);
        return 0;
    }

    return( -1 );
}
