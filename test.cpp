#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <cassert>
#include "ringbuffer.h"

#define MESSAGE_COUNT 1000000

// Include the ring buffer header here

struct Message {
    uint64_t sequence;
};

void producer(RingBuffer* ring, uint64_t start_seq, uint64_t count) {
  // std::cout << "producer start" << std::endl;
    Message msg;
    for (uint64_t i = 0; i != count; ++i) {
        msg.sequence = start_seq + i;
        while (!InsertToMessageBuffer(ring, (char*)&msg, sizeof(Message))) {
            // std::this_thread::yield();
        }
    }
    // std::cout << "producer finish" << std::endl;
}

void consumer(RingBuffer* ring, uint64_t expected_count, std::vector<bool>& received) {
    // std::cout << "consumer start" << std::endl;
    char* buffer = new char[RING_SIZE];
    // char buffer[RING_SIZE];
    MessageSizeT size;
    uint64_t count = 0;
    
    while (count < expected_count) {
        if (FetchFromMessageBuffer(ring, buffer, &size)) {
            MessageSizeT total_size_left = size;
            BufferT curr_bufferp = buffer;
            // std::cout << "total size left" << total_size_left << std::endl;
            while (total_size_left > 0) {
              //while still something to read, read next message
              // std::cout << "get anything from buffer" << std::endl;
              BufferT msgPtr;
              MessageSizeT msgSize;
              BufferT nextPtr;
              MessageSizeT remaining;
              
              ParseNextMessage(curr_bufferp, total_size_left, &msgPtr, &msgSize, &nextPtr, &remaining);
              // std::cout << "remaining: " << remaining << std::endl;
              Message* msg = (Message*)msgPtr;
              
              // Verify message
              received[msg->sequence] = true;
              count++;
              curr_bufferp = nextPtr;
              total_size_left = remaining;
              // std::cout << total_size_left <<std::endl;
            }
        }
        // } else {
        //   // std::cout << "stuck here" << std::endl;
        //     std::this_thread::yield();
        // }
    }
    delete[] buffer;
    // std::cout << "consumer finish" << std::endl;
}

void runSingleProducerTest() {
    char* buffer = new char[sizeof(RingBuffer) + CACHE_LINE];
    std::cout << "52" << std::endl;
    RingBuffer* ring = AllocateMessageBuffer(buffer);
    
    std::vector<bool> received(MESSAGE_COUNT, false);
    
    auto start = std::chrono::high_resolution_clock::now();
    std::thread producer_thread(producer, ring, 0, MESSAGE_COUNT);
    
    std::thread consumer_thread(consumer, ring, MESSAGE_COUNT, std::ref(received));
    
    producer_thread.join();
    consumer_thread.join();

    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Verify all messages were received
    for (uint64_t i = 0; i < MESSAGE_COUNT; i++) {
      if (received[i] == false) {
        std::cout << "missing message number " << i << std::endl;
      }
    }
    
    double throughput = (MESSAGE_COUNT * 1000.0) / duration.count();
    std::cout << "Single producer throughput: " << throughput << " messages/second\n";
    
    delete[] buffer;
}

void runMultipleProducerTest() {
    char* buffer = new char[sizeof(RingBuffer) + CACHE_LINE];
    RingBuffer* ring = AllocateMessageBuffer(buffer);
    
    std::vector<bool> received(MESSAGE_COUNT*2, false);
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::thread consumer_thread(consumer, ring, MESSAGE_COUNT*2, std::ref(received));
    std::thread producer_thread(producer, ring, 0, MESSAGE_COUNT);
    std::thread producer_thread2(producer, ring, MESSAGE_COUNT, MESSAGE_COUNT);
    
    producer_thread.join();
    producer_thread2.join();
    consumer_thread.join();


    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Verify all messages were received
    for (uint64_t i = 0; i != MESSAGE_COUNT*2; ++i) {
      if (received[i] == false) {
        std::cout << "missing message number " << i << std::endl;
      }
    }
    
    double throughput = (MESSAGE_COUNT *2 * 1000.0) / duration.count();
    std::cout << "Multiple producer throughput: " << throughput << " messages/second\n";
    
    delete[] buffer;
}

int main() {
    runSingleProducerTest();
    runMultipleProduceçrTest();
    return 0;
}