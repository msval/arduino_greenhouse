#include <SPI.h>
#include <nRF24L01.h>
#include <RF24.h>
#include <math.h>

#include <DHT.h>

#define source_address 'G'
#define destination_address 'C'

#define DHTPIN 2
#define DHTTYPE DHT11

// instantiate DHT sensor
DHT dht(DHTPIN, DHTTYPE);

// set up nRF24L01 radio on SPI bus plus pins 9 & 10
RF24 radio(9, 10);

// radio pipe addresses for the 2 nodes to communicate.
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

SensorReadingData data_in, data_out;

int readingTempOut;
int readingTempIn;
int readingLight;

void setup(void) {
  radio.begin();
  
  // 15 millis delay for channel to settle, try to send 15x
  radio.setRetries(15, 15);

  radio.openWritingPipe(pipes[1]);
  
  radio.openReadingPipe(1, pipes[0]);
  radio.startListening();
}

void loop(void) {
  
  if (radio.available()) {
    bool done = false;
    while (!done) {
      done = radio.read(&data_in, sizeof(data_in));
      // we could do something with the data ...
    }
  }
  
  readingTempOut = analogRead(A0);
  readingTempIn = analogRead(A1);
  readingLight = analogRead(A2);
  
  float kelvinsOut = thermisterKelvin(readingTempOut);
  // correction because of longer wires on outside sensor
  float celsiusOut = kelvinToCelsius(kelvinsOut) - 0.8;
  
  float kelvinsIn = thermisterKelvin(readingTempIn);
  float celsiusIn = kelvinToCelsius(kelvinsIn);
  
  // we'll measure the input values and map 0-1023 input
  // to 0 - 100 values
  int lightLevel = map(readingLight, 0, 1023, 0, 100);
  
  float h = dht.readHumidity();
  float t = dht.readTemperature();
  
  data_out.source = source_address;
  data_out.destination = destination_address;
  
  data_out.temperatureIn = celsiusIn;
  data_out.temperatureOut = celsiusOut;
  data_out.checkTemperature = t;
  data_out.humidity = h;
  data_out.light = lightLevel;
  data_out.action = '-';
  
  // stop listening so we can talk.
  radio.stopListening();
  bool ok = radio.write(&data_out, sizeof(data_out));
  radio.startListening();
  
  delay(10000);
}

float thermisterKelvin(int rawADC) {
  // Steinhart-Hart equation
  // 1/T = A + B (LnR) + C(LnR)ˆ3
  
  // LnR is the natural log of measured resistance
  // A, B and C are constants specific to resistor
  
  // we'll use:
  // T = 1 / (A + B(LnR) + C(LNR)ˆ3)
  
  // we are using long because arduino int goes up to 32 767
  // if we multiply it by 1024 we won't get correct values
  long resistorType = 10000;
  
  // input pin gives values from 0 - 1024
  float r = (1024 * resistorType / rawADC) - resistorType;
  
  // let' calculate natural log of resistance 
  float lnR = log(r);
  
  // constants specific to a 10K Thermistor
  float a = 0.001129148;
  float b = 0.000234125;
  float c = 0.0000000876741;
  
  // the resulting temperature is in kelvins 
  float temperature = 1 / (a + b * lnR + c * pow(lnR, 3));
  
  return temperature;
}

float kelvinToCelsius(float tempKelvin) {
  return tempKelvin - 273.15;
}

