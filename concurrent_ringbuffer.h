//
// Copyright (c) Far Data Lab (FDL).
// All rights reserved.
//
//

#pragma once

#include <atomic>
#include <cstring>
 
#define RING_SIZE           16777216
#define FORWARD_DEGREE      1048576
#define CACHE_LINE          64
#define INT_ALIGNED         16
 
//define a generic class for atomic variable
template <class C>
using Atomic = std::atomic<C>;
typedef char*        BufferT;
typedef unsigned int MessageSizeT;
typedef unsigned int RingSizeT;
 
struct RingBuffer {
  Atomic<int> ForwardTail[INT_ALIGNED];
  Atomic<int> SafeTail[INT_ALIGNED];
  Atomic<int> Head[INT_ALIGNED];
  char Buffer[RING_SIZE];
};

//create the entire ring buffer
RingBuffer*
AllocateMessageBuffer(
  BufferT BufferAddress
) {
  RingBuffer* ringBuffer = (RingBuffer*)BufferAddress;

  size_t ringBufferAddress = (size_t)ringBuffer;
  while (ringBufferAddress % CACHE_LINE != 0) {
    ringBufferAddress++;
  }
  ringBuffer = (RingBuffer*)ringBufferAddress;

  memset(ringBuffer, 0, sizeof(RingBuffer));

  return ringBuffer;
}

void
DeallocateMessageBuffer(
  RingBuffer* Ring
) {
  memset(Ring, 0, sizeof(RingBuffer));
}

bool
InsertToMessageBuffer(
  RingBuffer* Ring,
  const BufferT CopyFrom,
  MessageSizeT MessageSize
) {

    //align to cache_line size
    MessageSizeT messageBytes = sizeof(MessageSizeT) + MessageSize;
    while (messageBytes % CACHE_LINE != 0) {
      messageBytes++;
    }

    while (true) {

      int forwardTail = Ring->ForwardTail[0];
      int head = Ring->Head[0];
      RingSizeT distance = 0;

      if (forwardTail < head) {
        distance = forwardTail + RING_SIZE - head;
      }
      else {
        distance = forwardTail - head;
      }
      //if the write position is too much in advance, then disallow writing into buffer
      if (distance >= FORWARD_DEGREE) {
        return false;
      }

      //if the space left over is not enough for the new message size, return false
      if (messageBytes > RING_SIZE - distance) {
        return false;
      }

      int newTail = (forwardTail + messageBytes) % RING_SIZE;

      if (Ring->ForwardTail[0].compare_exchange_weak(forwardTail, newTail) == true) {
        forwardTail = Ring->ForwardTail[0];
        int current_head = Ring->Head[0];
        if (current_head != head) {
          // Head has moved - rollback and retry
          Ring->ForwardTail[0] = forwardTail;
          continue;
        }

        if (forwardTail + messageBytes <= RING_SIZE) { //if no need to wrap around
          char* messageAddress = &Ring->Buffer[forwardTail];

          *((MessageSizeT*)messageAddress) = messageBytes;

          memcpy(messageAddress + sizeof(MessageSizeT), CopyFrom, MessageSize);

          int safeTail = Ring->SafeTail[0];
          while (Ring->SafeTail[0].compare_exchange_weak(safeTail, (safeTail + messageBytes) % RING_SIZE) == false) {
            safeTail = Ring->SafeTail[0];
          }
        }
        else {
          RingSizeT remainingBytes = RING_SIZE - forwardTail - sizeof(MessageSizeT);
          char* messageAddress1 = &Ring->Buffer[forwardTail];
          *((MessageSizeT*)messageAddress1) = messageBytes;

          if (MessageSize <= remainingBytes) {
            memcpy(messageAddress1 + sizeof(MessageSizeT), CopyFrom, MessageSize);
          } else {
            char* messageAddress2 = &Ring->Buffer[0];
            if (remainingBytes) { 
              // because remaining bytes are the size we can fit in the remaining in the buffer without the meta-data size 
              memcpy(messageAddress1 + sizeof(MessageSizeT), CopyFrom, remainingBytes);
            }
              memcpy(messageAddress2, (const char*)CopyFrom + remainingBytes, MessageSize - remainingBytes);
          }

          int safeTail = Ring->SafeTail[0];
          while (Ring->SafeTail[0].compare_exchange_weak(safeTail, (safeTail + messageBytes) % RING_SIZE) == false) {
            safeTail = Ring->SafeTail[0];
          }
        }
        return true;
      }
    }
  }
  
  bool
  FetchFromMessageBuffer(
        RingBuffer* Ring,
        BufferT CopyTo,
        MessageSizeT* MessageSize
  ) {
        int forwardTail = Ring->ForwardTail[0];
        int safeTail = Ring->SafeTail[0];
        int head = Ring->Head[0];
  
        //nothing to be read from, return false
        if (forwardTail == head) {
          return false;
        }
  
        //if the producer has advanced the forwardTail (would like to write something)
        //but not all content has been safely written into the buffer, return false
        if (forwardTail != safeTail) {
          return false;
        }
  
        
        RingSizeT availBytes = 0;
        char* sourceBuffer1 = nullptr;
        char* sourceBuffer2 = nullptr;
  
        if (safeTail > head) { //if safeTail is on the right of head 
          //don't need to wrap around
          availBytes = safeTail - head;
          *MessageSize = availBytes;
          sourceBuffer1 = &Ring->Buffer[head];
        }
        else {
          //if wrapped around, then we need 2 buffers to copy into copyTo
          availBytes = RING_SIZE - head;
          *MessageSize = availBytes + safeTail;
          sourceBuffer1 = &Ring->Buffer[head];
          sourceBuffer2 = &Ring->Buffer[0];
        }
        
        //copy the first buffer, and then set the buffer to 0
        memcpy(CopyTo, sourceBuffer1, availBytes);
        memset(sourceBuffer1, 0, availBytes);
  
        //if there's anything in source buffer 2, copy that, and set the var to 0 (same as deallocate?)
        //Question: why not deallocate?
        if (sourceBuffer2) {
          memcpy((char*)CopyTo + availBytes, sourceBuffer2, safeTail);
          memset(sourceBuffer2, 0, safeTail);
       }
 
       Ring->Head[0] = safeTail;
 
       return true;
}
 
void
ParseNextMessage(
       BufferT CopyTo,
       MessageSizeT TotalSize,
       BufferT* MessagePointer,
       MessageSizeT* MessageSize,
       BufferT* StartOfNext,
       MessageSizeT* RemainingSize
) {
       // std::cout << "parse next message start" << std::endl;
       char* bufferAddress = (char*)CopyTo;
       MessageSizeT totalBytes = *(MessageSizeT*)bufferAddress;
 
       *MessagePointer = (BufferT)(bufferAddress + sizeof(MessageSizeT));
       *MessageSize = totalBytes - sizeof(MessageSizeT);
       *RemainingSize = TotalSize - totalBytes;
       // std::cout << "totalbytes: " << totalBytes << "total size: " << TotalSize << "remaining size:" << *RemainingSize << std::endl;
 
       if (*RemainingSize > 0) {
              *StartOfNext = (BufferT)(bufferAddress + totalBytes);
       }
       else {
              *StartOfNext = nullptr;
       }
}