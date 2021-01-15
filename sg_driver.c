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

// Project Includes
#include <sg_driver.h>
#include <sg_service.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include<sg_cache.h>
// Defines
//struct for block info
typedef struct block {
    int block_number;
    SG_Block_ID loc_id;
    SG_Node_ID rem_id;
    SG_Block_ID blk_id;
    SG_SeqNum rseqq;
    char *blk_ptr;
} block_t;
//struct for file info
typedef struct File {
    SgFHandle file_h;
    struct File *next;
    size_t file_ptr;
    size_t file_size;
    char filename;
    block_t data[200];
    int num_blocks;
    int open;
} File_t;

typedef struct map {
    struct map *next;
    SG_Node_ID node_id;
    SG_SeqNum rseq;
} map_t;
//
// Global Data
SgFHandle file_handle = 0;
File_t *headd;
map_t *node_head;
int num_of_files = 0;
int reads = 0;
int global_flag = 0;
// Driver file entry

// Global data
//initialize file_handle assignment, # of files, and head node of file linked list
int sgDriverInitialized = 0; // The flag indicating the driver initialized
SG_Block_ID sgLocalNodeId;   // The local node identifier
SG_SeqNum sgLocalSeqno = SG_INITIAL_SEQNO;  // The local sequence number

// Driver support functions
int sgInitEndpoint( void ); // Initialize the endpoint

//
// Functions

//
// File system interface implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgopen
// Description  : Open the file for for reading and writing
//
// Inputs       : path - the path/filename of the file to be read
// Outputs      : file handle if successful test, -1 if failure

SgFHandle sgopen(const char *path) {

    // First check to see if we have been initialized
    if (!sgDriverInitialized) {

        // Call the endpoint initialization 
        if ( sgInitEndpoint() ) {
            logMessage( LOG_ERROR_LEVEL, "sgopen: Scatter/Gather endpoint initialization failed." );
            return( -1 );
        }

        // Set to initialized
        sgDriverInitialized = 1;
    }
    
    //if the file path did not exist and the head file node is empty
    if (num_of_files == 0) {
        
        headd->file_h = file_handle;
        file_handle++;
        num_of_files++;
        headd->file_ptr = 0;
        headd->file_size = 0;
        headd->filename = *path;
        headd->num_blocks = 0;
        headd->open = 1;
        headd->next = NULL;
        return headd->file_h;
    }
    
    //if the head file node is not empty and the file did not previously exist
    File_t *aFile = (File_t *) malloc(sizeof(File_t));
    num_of_files++;

    aFile->file_h = file_handle;
    file_handle++;
    aFile->file_ptr = 0;
    aFile->file_size = 0;
    aFile->filename = *path;
    aFile->open = 1;
    aFile->num_blocks = 0;
    
    File_t *current = headd;
    //set the new files location in the file linked list
    while (current->next != NULL) {
        current = current->next;
    }
    
    current->next = aFile;
    aFile->next = NULL;

    // Return the file handle 
    return( aFile->file_h );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgread
// Description  : Read data from the file
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
//                len - the length of the read
// Outputs      : number of bytes read, -1 if failure

int sgread(SgFHandle fh, char *buf, size_t len) {
    
    File_t *current = headd;
    File_t *aFile;
    char the_data[SG_BLOCK_SIZE];

    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_DATA_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    //look for the file handle we are looking for
    for (int i = 0; i < num_of_files; i++) {
        if (current->file_h == fh) {
            aFile = current;
        }
        else {
            current = current->next;
        }
    }
    
    //check if the file handle is bad or if it was not previously open
    if (aFile->file_h != fh) {
        return -1; 
    }
    if (aFile->open == 0) {
        return -1;
    }

    //reset the len parameter if it wants to read longer than the file's length
    if (aFile->file_size < len) {
        len = aFile->file_size;
    }
    
    //get index for block array in the file
    int index = (aFile->file_ptr) / SG_BLOCK_SIZE;
    int mod = (aFile->file_ptr) % SG_BLOCK_SIZE;
    //try to retreive the block in the cache
    char *cache_block = malloc(SG_BLOCK_SIZE);
    cache_block = getSGDataBlock(aFile->data[index].rem_id, aFile->data[index].blk_id);
    //if we find the block, copy data from the cache into the buf
    if (cache_block != NULL) {
        memcpy(buf, cache_block + mod, len);
    }
    //if we did not find the block in cache, obtain the block regularly from the SG system
    else {
        pktlen = SG_BASE_PACKET_SIZE;
        if ( (ret = serialize_sg_packet(sgLocalNodeId, // Local ID
                                        aFile->data[index].rem_id,   // Remote ID
                                        aFile->data[index].blk_id,  // Block ID
                                        SG_OBTAIN_BLOCK,  // Operation
                                        sgLocalSeqno,    // Sender sequence number
                                        SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                        NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
            return( -1 );
        }
        sgLocalSeqno++;
        // Send the packet
        rpktlen = SG_DATA_PACKET_SIZE;
        if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
            return( -1 );
        }
        // Unpack the recieived data
        if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                        &srem, the_data, recvPacket, rpktlen)) != SG_PACKT_OK ) {
            return( -1 );
        }

        //copy the data into the buffer of 'len' bytes
        memcpy(buf, the_data + mod, len);
        //place the block in cache since we did not find it earlier
        putSGDataBlock(aFile->data[index].rem_id, aFile->data[index].blk_id, the_data);
    }
    
    aFile->file_ptr += len;

    // Return the bytes processed
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgwrite
// Description  : write data to the file
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : number of bytes written if successful test, -1 if failure

int sgwrite(SgFHandle fh, char *buf, size_t len) {
    
    File_t *current = headd;
    File_t *aFile = headd;
    block_t aBlock;
    char the_data[SG_BLOCK_SIZE];
    int update = 0;
    int update_middle = 0;
    
    char initPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    int file_found = 0;

    //look for the file handle
    for (int i = 0; i < num_of_files; i++) {
        if (current->file_h == fh) {
            file_found = 1;
            aFile = current;
        }
        else {
            current = current->next;
        }
    }
    
    if (file_found == 0) {
        return -1;
    }

    int mod = (aFile->file_ptr)%SG_BLOCK_SIZE;
    int index = (aFile->file_ptr)/SG_BLOCK_SIZE;
    char *cache_block = malloc(SG_BLOCK_SIZE);

    //check if we are creating a block rather than updating
    if ((mod == 0) && (aFile->file_ptr == aFile->file_size)) {
        
        pktlen = SG_DATA_PACKET_SIZE;
        if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                        SG_NODE_UNKNOWN,   // Remote ID
                                        SG_BLOCK_UNKNOWN,  // Block ID
                                        SG_CREATE_BLOCK,  // Operation
                                        sgLocalSeqno,    // Sender sequence number
                                        SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                        buf, initPacket, &pktlen)) != SG_PACKT_OK ) {
            return( -1 );
        }
        sgLocalSeqno++;
    }
    else {
        update = 1;
        //determine if we are writing to a block in the middle of the file or second half of the last block
        if (aFile->file_ptr < aFile->file_size) {
            update_middle = 1;
        }

        cache_block = getSGDataBlock(aFile->data[index].rem_id, aFile->data[index].blk_id);
        //push the update through the cache if we find the block in cache
        if (cache_block != NULL) {
            memcpy(cache_block + mod, buf, len);
            putSGDataBlock(aFile->data[index].rem_id, aFile->data[index].blk_id, cache_block);
            //update block - obtain block, change correct bytes, send block back with update block
            pktlen = SG_DATA_PACKET_SIZE;
            if ( (ret = serialize_sg_packet(sgLocalNodeId, // Local ID
                                            aFile->data[index].rem_id,  // Remote ID
                                            aFile->data[index].blk_id,  // Block ID
                                            SG_UPDATE_BLOCK,   // Operation
                                            sgLocalSeqno,    // Sender sequence number
                                            SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                            getSGDataBlock(aFile->data[index].rem_id, aFile->data[index].blk_id), initPacket, &pktlen)) != SG_PACKT_OK ) {
                return( -1 );
            }
            sgLocalSeqno++;   
        }
        //if block was not in the cache, use the SG system to obtain and update it regularly
        else {
            pktlen = SG_BASE_PACKET_SIZE;
            if ( (ret = serialize_sg_packet(sgLocalNodeId, // Local ID
                                            aFile->data[index].rem_id,   // Remote ID
                                            aFile->data[index].blk_id,  // Block ID
                                            SG_OBTAIN_BLOCK,  // Operation
                                            sgLocalSeqno,    // Sender sequence number
                                            SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                            NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
                return( -1 );
            }
            sgLocalSeqno++;

            // Send the packet
            rpktlen = SG_DATA_PACKET_SIZE;
            if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
                return( -1 );
            }
            // Unpack the recieived data
            if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                            &srem, the_data, recvPacket, rpktlen)) != SG_PACKT_OK ) {
                return( -1 );
            }
            //place the block because it was not in the cache
            memcpy(the_data + mod, buf, len);
            putSGDataBlock(aFile->data[index].rem_id, aFile->data[index].blk_id, the_data);

            //update block - obtain block, change correct bytes, send block back with update block
            pktlen = SG_DATA_PACKET_SIZE;
            if ( (ret = serialize_sg_packet(sgLocalNodeId, // Local ID
                                            aFile->data[index].rem_id,  // Remote ID
                                            aFile->data[index].blk_id,  // Block ID
                                            SG_UPDATE_BLOCK,   // Operation
                                            sgLocalSeqno,    // Sender sequence number
                                            SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                            the_data, initPacket, &pktlen)) != SG_PACKT_OK ) {
                return( -1 );
            }
            sgLocalSeqno++;
            
        }
    }
    
    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        return( -1 );
    }

    //add block data to block list, update number of blocks and the file's size
    aBlock.rem_id = rem;
    aBlock.blk_id = blkid;
    
    if (node_head->node_id == 0) {
        node_head->node_id = rem;
        node_head->rseq = srem;
        node_head->next = NULL;
    }
    
    // if we created a block
    if (update == 0) {
        aFile->data[aFile->num_blocks] = aBlock;
        aFile->num_blocks++;
        aBlock.block_number = aFile->num_blocks;
        putSGDataBlock(aFile->data[index].rem_id, aFile->data[index].blk_id, buf);
    }
    // if we updated the last block in the file (write to second half of last block)
    if (update_middle == 0) {
        aFile->file_size += len;
        aFile->file_ptr = aFile->file_size;
    }
    // if we updated a block in the middle of the file we do not increase the file size
    else {
        aFile->file_ptr += len;
    }
    
    // Log the write, return bytes written
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgseek
// Description  : Seek to a specific place in the file
//
// Inputs       : fh - the file handle of the file to seek in
//                off - offset within the file to seek to
// Outputs      : new position if successful, -1 if failure

int sgseek(SgFHandle fh, size_t off) {
    
    File_t *current = headd;
    File_t *aFile;
    //look for the file handle 
    
    for (int i = 0; i < num_of_files; i++) {
        
        if (current->file_h == fh) {
            aFile = current;
        }
        else {
            current = current->next;
        }
    }
    
    //return error if file handle is bad or file is not open or if the offset points to EOF
    if (aFile->file_h != fh) {
        return -1;
    }

    if (aFile->open == 0) {
        return -1;
    }
    
    if (aFile->file_size <= (int)off) {
        return -1;
    }
    
    //set the file pointer to the offset
    aFile->file_ptr = off;
    
    // Return new position
    return( off );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgclose
// Description  : Close the file
//
// Inputs       : fh - the file handle of the file to close
// Outputs      : 0 if successful test, -1 if failure

int sgclose(SgFHandle fh) {

    File_t *current = headd;
    File_t *aFile;
    //find the file handle 
    for (int i = 0; i < num_of_files; i++) {
        if (current->file_h == fh) {
            aFile = current;
        }
        else {
            current = current->next;
        }
    }
    //return error if file handle bad or file not open
    if ((aFile->file_h != fh) || (aFile->open == 0)) {
        return -1;
    }
    //close the file
    aFile->open = 0;

    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgshutdown
// Description  : Shut down the filesystem
//
// Inputs       : none
// Outputs      : 0 if successful test, -1 if failure

int sgshutdown(void) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    // Setup the packet with the SG_STOP_ENDPOINT op code to shut down the system
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( SG_NODE_UNKNOWN, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_STOP_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        return( -1 );
    }

    closeSGCache();

    map_t *curr = node_head;
    map_t *delete = node_head;
    // free node to rseq mapping data
    while (curr->next != NULL) {
        delete = curr;
        curr = curr->next;
        free(delete);
    }

    // Log, return successfully
    logMessage( LOG_INFO_LEVEL, "Shut down Scatter/Gather driver." );
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : serialize_sg_packet
// Description  : Serialize a ScatterGather packet (create packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status serialize_sg_packet(SG_Node_ID loc, SG_Node_ID rem, SG_Block_ID blk,
                                     SG_System_OP op, SG_SeqNum sseq, SG_SeqNum rseq, char *data,
                                     char *packet, size_t *plen) {

    uint8_t data_indicator;
    SG_Packet_Status status;
    uint32_t magic = SG_MAGIC_VALUE;
    // check if the head node has a matching node ID and pass it in if it does and increment
    map_t *crt = node_head;
    int found = 0;
    if (crt->node_id == rem) {
        crt->rseq++;
        rseq = crt->rseq;
        found = 1;
    }
    // if our node ID was not the head, check the rest of the linked list for that node ID and pass it in if found
    while (crt->next != NULL && found == 0) {
        crt = crt->next;

        if (crt->node_id == rem) {
            rseq = crt->rseq;
            rseq += 1;
            found = 1;
        }
    }
    // if node ID is not found in our mapping, pass in the initial seq no + 1 (create_block op needs to increment it)
    if (found == 0) {
        rseq = SG_INITIAL_SEQNO + 1;
    }
    
    // validating all parameters for correct values, otherwise return proper error
    
    if (sseq == 0) {
        status = 5;
        return status;
    }

    if (rseq == 0) {
        status = 6;
        return status;
    }
   
    if (loc == 0) {
        status = 1;
        return status;
    }
    
    if (rem == 0) {
        status = 2;
        return status;
    }
    
    if (blk == 0) {
        status = 3;
        return status;
    }

    if ((op > 6) || (op < 0)) {
        status = 4;
        return status;
    }
    
    // Determine if there is data and assign data_indicator and *plen to correct values
    if (data == NULL) {
        data_indicator = (uint8_t)0;
        *plen = SG_BASE_PACKET_SIZE;

    }
    else {
        data_indicator = (uint8_t)1;
        *plen = SG_DATA_PACKET_SIZE;
    }

    // building the packet with all the given values

    memcpy(packet, &magic, sizeof(magic));
    memcpy(packet + sizeof(magic), &loc, sizeof(loc));
    memcpy(packet + sizeof(magic) + sizeof(loc), &rem, sizeof(rem));
    memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem), &blk, sizeof(blk));
    memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk), &op, sizeof(op));
    memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(op), &sseq, sizeof(sseq));
    memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(op) + sizeof(sseq), &rseq, sizeof(rseq));
    memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(op) + sizeof(sseq) + sizeof(rseq), &data_indicator, sizeof(uint8_t));
    // add data only if we indicated that there is data, otherwise just add magic value
    if (data_indicator == (uint8_t)1) {
        memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(op) + sizeof(sseq) + sizeof(rseq) + sizeof(data_indicator), data, SG_BLOCK_SIZE);
        memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(op) + sizeof(sseq) + sizeof(rseq) + sizeof(data_indicator) + SG_BLOCK_SIZE, &magic, sizeof(magic));
    }
    else {
        memcpy(packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(op) + sizeof(sseq) + sizeof(rseq) + sizeof(data_indicator), &magic, sizeof(magic));
    }
    
    // return packet status is good since no errors were thrown

    status = 0;
    return status; 
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : deserialize_sg_packet
// Description  : De-serialize a ScatterGather packet (unpack packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status deserialize_sg_packet(SG_Node_ID *loc, SG_Node_ID *rem, SG_Block_ID *blk,
                                       SG_System_OP *op, SG_SeqNum *sseq, SG_SeqNum *rseq, char *data,
                                       char *packet, size_t plen) {

    SG_Packet_Status status;
    uint8_t data_indicator;
    uint32_t magic = SG_MAGIC_VALUE;
    
    // copy loc value from packet and verify that it is correct

    memcpy(loc, (packet + sizeof(magic)), sizeof(long unsigned int));
    if (*loc == 0) {
        status = 1;
        return status;
    }

    // copy rem value from packet and verify that it is correct
    
    memcpy(rem, packet + sizeof(magic) + sizeof(loc), sizeof(long unsigned int));
    if (*rem == 0) {
        status = 2;
        return status;
    }

    // copy blk value from packet and verify that it is correct

    memcpy(blk, packet + sizeof(magic) + sizeof(loc) + sizeof(rem), sizeof(long unsigned int));
    if (*blk == 0) {
        status = 3;
        return status;
    }

    // copy op value from packet and verify that it is correct

    memcpy(op, packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk), sizeof(int));
    if ((*op > 6) || (*op < 0)) {
        status = 4;
        return status;
    }

    // copy sseq value from packet and verify that it is correct

    memcpy(sseq, packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(int), sizeof(uint16_t));
    if (*sseq == 0) {
        status = 5;
        return status;
    }

    // copy rseq value from packet and verify that it is correct

    memcpy(rseq, packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(int) + sizeof(uint16_t), sizeof(uint16_t));
    
    map_t *crt = node_head;
    int found = 0;
    // check if the head node is our node id in deserialize and save its most recent rseq value
    if (crt->node_id == *rem) {
        crt->rseq = *rseq;
        found = 1;
    }
    // if it was not found, check rest of mapping and save newest rseq value if found
    while (crt->next != NULL && found == 0) {
        crt = crt->next;
        if (crt->node_id == *rem) {
            crt->rseq = *rseq;
            found = 1;
        }
    }
    // the node id does not exist in our mapping so allocate mem for a new node, assign its metadata, and set it to the end of the linked list
    if (found == 0) {
        map_t *newnde = malloc(sizeof(map_t));
        newnde->node_id = *rem;
        newnde->rseq = *rseq;
        newnde->next = NULL;

        crt = node_head;
        while (crt->next != NULL) {
            crt = crt->next;
        }
        crt->next = newnde;
        
        logMessage(LOG_ERROR_LEVEL, "sgAddNodeInfo: unable to find node in node table [%lu]\n", crt->next->node_id);
        logMessage(LOG_INFO_LEVEL, "Added node [%lu] seq [%d]\n", crt->next->node_id, crt->next->rseq);
    }
    
    if (*rseq == 0) {
        status = 6;
        return status;
    }

    // copy data_indicator value from packet so we can check if there is data

    memcpy(&data_indicator, packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(int) + sizeof(uint16_t) + sizeof(uint16_t), sizeof(uint8_t));
    
    // if data_indicator value says there is data, copy the data from the packet

    if (data_indicator == (uint8_t)1) {
        memcpy(data, packet + sizeof(magic) + sizeof(loc) + sizeof(rem) + sizeof(blk) + sizeof(int) + sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint8_t), SG_BLOCK_SIZE);
    }

    // return that the packet status is good since no errors were thrown
    
    status = 0;
    return status;
}

//
// Driver support functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgInitEndpoint
// Description  : Initialize the endpoint
//
// Inputs       : none
// Outputs      : 0 if successfull, -1 if failure

int sgInitEndpoint( void ) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    
    // initializing the file linked list, nodeid/rseq linked list, and cache
    headd = (File_t *) malloc(sizeof(File_t));
    headd->file_h = 0;
    headd->next = NULL;
    node_head = (map_t *) calloc(1, sizeof(map_t));
    global_flag = 1;
    node_head->next = NULL;
    initSGCache(SG_MAX_CACHE_ELEMENTS);

    // Local and do some initial setup
    logMessage( LOG_INFO_LEVEL, "Initializing local endpoint ..." );
    sgLocalSeqno = SG_INITIAL_SEQNO;

    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( SG_NODE_UNKNOWN, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_INIT_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Sanity check the return value
    if ( loc == SG_NODE_UNKNOWN ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", loc );
        return( -1 );
    }

    // Set the local node ID, log and return successfully
    sgLocalNodeId = loc;
    logMessage( LOG_INFO_LEVEL, "Completed initialization of node (local node ID %lu", sgLocalNodeId );
    
    
    
    return( 0 );
}
