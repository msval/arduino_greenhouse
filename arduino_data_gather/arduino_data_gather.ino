#include <SPI.h>
#include <nRF24L01.h>
#include <RF24.h>

// set up nRF24L01 radio on SPI bus plus pins 9 & 10
RF24 radio(9, 10);

// channel designations, pus as many as you need, 2 are enough for this
// every transceiver can have one out and up to 5 read channels
const uint64_t pipes[2] = { 0xF0F0F0F0E1LL, 0xF0F0F0F0D2LL };

typedef struct {
  char source;
  char destination;
  float temperatureIn;
  float temperatureOut;
  float checkTemperature;
  float humidity;
  int light;
  char action;
} SensorReadingData;

SensorReadingData data_out, data_in;

void setup(void) {
  Serial.begin(9600);
  
  radio.begin();
  // 15 millis delay for channel to settle, try to send 15x
  radio.setRetries(15, 15);

  radio.openWritingPipe(pipes[0]);
  radio.openReadingPipe(1, pipes[1]);

  radio.startListening();
}

void loop(void) {
  // check if the data is available
  if (radio.available()) {
    bool done = false;

    // read the data until finished
    while (!done) {
      done = radio.read(&data_in, sizeof(data_in));
      
      // send data to serial only if destination is Cassandra node
      if (data_in.destination == 'C') {
        // print the data
        Serial.print("{");
        Serial.print("\"source\": \"");Serial.print(data_in.source);Serial.print("\",");
        Serial.print("\"destination\": \"");Serial.print(data_in.destination);Serial.print("\",");
        Serial.print("\"temperaturein\": ");Serial.print(data_in.temperatureIn);Serial.print(",");
        Serial.print("\"temperatureout\": ");Serial.print(data_in.temperatureOut);Serial.print(",");
        Serial.print("\"temperaturecheck\": ");Serial.print(data_in.checkTemperature);Serial.print(",");
        Serial.print("\"humidity\": ");Serial.print(data_in.humidity);Serial.print(",");
        Serial.print("\"light\": ");Serial.print(data_in.light);Serial.print(",");
        Serial.print("\"action\": \"");Serial.print(data_in.action);Serial.print("\"");
        Serial.print("}");      
        Serial.println();
      }
    }
    
    delay(10);
  }
  
  // send the data after reading is done
  
  // use if info should go back to the sensor.
  //radio.stopListening();
  //bool ok = radio.write(&data_out, sizeof(data_out));
  //radio.startListening();
}

