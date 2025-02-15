//
// Copyright (c) Far Data Lab (FDL).
// All rights reserved.
//
//

#pragma once

#include <atomic>
#include <mutex>  
#include <cstring>
 
#define RING_SIZE           16777216
#define FORWARD_DEGREE      1048576
#define CACHE_LINE          64
#define INT_ALIGNED         16
 
template <class C>
using Atomic = std::atomic<C>;
typedef char*        BufferT;
typedef unsigned int MessageSizeT;
typedef unsigned int RingSizeT;
 
struct RingBuffer {
       Atomic<int> ForwardTail[INT_ALIGNED];
       Atomic<int> SafeTail[INT_ALIGNED];
       int Head[INT_ALIGNED];
       std::mutex ringMutex;
       char Buffer[RING_SIZE];
};

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
       // std::cout << "start in InserToMessageBuffer" << std::endl;
       std::lock_guard<std::mutex> lock(Ring->ringMutex);

       int forwardTail = Ring->ForwardTail[0];
       int head = Ring->Head[0];
       RingSizeT distance = 0;

       if (forwardTail < head) {
              distance = forwardTail + RING_SIZE - head;
       }
       else {
              distance = forwardTail - head;
       }

       if (distance >= FORWARD_DEGREE) {
              return false;
       }

       MessageSizeT messageBytes = sizeof(MessageSizeT) + MessageSize;
       while (messageBytes % CACHE_LINE != 0) {
              messageBytes++;
       }
 
       if (messageBytes > RING_SIZE - distance) {
              return false;
       }

       int newForwardTail = (forwardTail + messageBytes) % RING_SIZE;
 
 
       if (forwardTail + messageBytes <= RING_SIZE) {
              char* messageAddress = &Ring->Buffer[forwardTail];
 
              *((MessageSizeT*)messageAddress) = messageBytes;
 
              memcpy(messageAddress + sizeof(MessageSizeT), CopyFrom, MessageSize);

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
                            memcpy(messageAddress1 + sizeof(MessageSizeT), CopyFrom, remainingBytes);
                     }
                     memcpy(messageAddress2, (const char*)CopyFrom + remainingBytes, MessageSize - remainingBytes);
              }
       }
       
       Ring->ForwardTail[0].store(newForwardTail, std::memory_order_relaxed);
       Ring->SafeTail[0].store(newForwardTail,   std::memory_order_relaxed);
       // std::cout << "return from insert" << std::endl;
       return true;
}
 
bool
FetchFromMessageBuffer(
       RingBuffer* Ring,
       BufferT CopyTo,
       MessageSizeT* MessageSize
) {
       std::lock_guard<std::mutex> lock(Ring->ringMutex);
       // std::cout << "start Fetch" << std::endl;
       int forwardTail = Ring->ForwardTail[0];
       int safeTail = Ring->SafeTail[0];
       int head = Ring->Head[0];
 
       if (forwardTail == head) {
              return false;
       }
 
       if (forwardTail != safeTail) {
              return false;
       }
 
       RingSizeT availBytes = 0;
       char* sourceBuffer1 = nullptr;
       char* sourceBuffer2 = nullptr;
 
       if (safeTail > head) {
              availBytes = safeTail - head;
              *MessageSize = availBytes;
              sourceBuffer1 = &Ring->Buffer[head];
       }
       else {
              availBytes = RING_SIZE - head;
              *MessageSize = availBytes + safeTail;
              sourceBuffer1 = &Ring->Buffer[head];
              sourceBuffer2 = &Ring->Buffer[0];
       }
 
       memcpy(CopyTo, sourceBuffer1, availBytes);
       memset(sourceBuffer1, 0, availBytes);
 
       if (sourceBuffer2) {
              memcpy((char*)CopyTo + availBytes, sourceBuffer2, safeTail);
              memset(sourceBuffer2, 0, safeTail);
       }
 
       Ring->Head[0] = safeTail;
       // Ring->Head[0].store(safeTail, std::memory_order_relaxed);
 
       // std::cout << "end Fetch" << std::endl;
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
       char* bufferAddress = (char*)CopyTo;
       MessageSizeT totalBytes = *(MessageSizeT*)bufferAddress;
 
       *MessagePointer = (BufferT)(bufferAddress + sizeof(MessageSizeT));
       *MessageSize = totalBytes - sizeof(MessageSizeT);
       *RemainingSize = TotalSize - totalBytes;
 
       if (*RemainingSize > 0) {
              *StartOfNext = (BufferT)(bufferAddress + totalBytes);
       }
       else {
              *StartOfNext = nullptr;
       }
}