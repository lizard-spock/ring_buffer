#include <iostream>
#include <thread>
#include <vector>
#include "ringbuffer.h"
#include <chrono>

#define MSG_COUNT         1000000

//8 byte msg
struct TestMSG{
  uint64_t seq_num;
};

void producer_func(RingBuffer* ringbuffer, int count){
  TestMSG* messagep = (TestMSG*)malloc(sizeof(TestMSG));
  for (uint64_t i = 0; i != count; ++i) {
    messagep->seq_num = i;
    while (!InsertToMessageBuffer(ringbuffer, (char*)messagep, sizeof(TestMSG))) {
      std::this_thread::yield();
    }
  }
}

//status vector is 1 if received, 0 if not received correctly. 
void consumer_func(RingBuffer* ringbuffer, uint64_t count, std::vector<int>& status){
  char temp_buffer[8];
  MessageSizeT msg_size; //write to by FetchFromMessageBuffer
  uint64_t count_read = 0;
  while (count_read < count){
    if (FetchFromMessageBuffer(ringbuffer, temp_buffer, &msg_size)) {
      BufferT message_pointer;
      MessageSizeT message_size;
      BufferT start_of_next;
      MessageSizeT remaining_size;
      ParseNextMessage(temp_buffer, msg_size, &message_pointer, &message_size, &start_of_next, &remaining_size);
      if (((TestMSG*)message_pointer)->seq_num >= MSG_COUNT) {
        std::cout << "wrong seqnum" << std::endl;
      }
      status[((TestMSG*)message_pointer)->seq_num] = 1;
      count_read += 1;
    } else {
      std::this_thread::yield();
    }
  }
}



int main(int argc, char** argv){
  //create the ring buffer object 
  BufferT buffer_addr = (BufferT)malloc(sizeof(RingBuffer) + CACHE_LINE);
  RingBuffer* ring_bufferp = AllocateMessageBuffer(buffer_addr);
  
  std::vector<int> status(MSG_COUNT, 0);

  auto start = std::chrono::high_resolution_clock::now();

  std::thread producer(producer_func, ring_bufferp, MSG_COUNT);
  std::thread consumer(consumer_func, ring_bufferp, MSG_COUNT, std::ref(status));

  producer.join();
  consumer.join();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  for (uint64_t i = 0; i != MSG_COUNT-1; ++i) {
    if (status[i] == 0) {
      std::cout << "missing message number " << i << std::endl;
    }
  }

  double throughput = (MSG_COUNT * 1000.0) / duration.count();
  std::cout << "throughput: " << throughput << std::endl;

  return 0;
}