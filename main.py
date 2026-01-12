import time
import serial
import re
import json
import os
import sys
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from loguru import logger

def get_base_path():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = get_base_path()
LOG_PATH = os.path.join(BASE_DIR, "mqtt_service.log")

logger.add(LOG_PATH, rotation="1 MB", level="INFO", format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

load_dotenv()

SERIAL_PORT = os.getenv("SERIAL_PORT", "COM1")
BAUDRATE = int(os.getenv("BAUD_RATE", 9600))
BROKER = os.getenv("MQTT_BROKER", "localhost")
PORT = int(os.getenv("MQTT_PORT", 1883))
TOPIC_PUBLISH = os.getenv("TOPIC_INPUT", "DATA/WEIGHT")

class ScaleDeviceService:
    def __init__(self):
        self.has_sent_stable = False
        self.stable_count = 0
        self.required_stable = 3
        self.ser = None
        
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"MQTT Connected to {BROKER}:{PORT}")
        else:
            logger.error(f"Connection Failed, Code: {rc}")

    def on_disconnect(self, client, userdata, rc):
        logger.warning("MQTT Disconnected")

    def connect_serial_device(self):
        """Mencoba menghubungkan ke timbangan fisik"""
        try:
            self.ser = serial.Serial(SERIAL_PORT, BAUDRATE, timeout=0.1)
            logger.success(f"Timbangan Terhubung di {SERIAL_PORT}!")
            return True
        except serial.SerialException as e:
            logger.warning(f"Menunggu Timbangan ({SERIAL_PORT})...")
            return False
        except Exception as e:
            logger.error(f"Error Lain: {e}")
            return False

    def run(self):
        logger.info("SCALE SERVICE STARTED (NO SIMULATION)")
        
        try:
            self.client.connect(BROKER, PORT, 60)
            self.client.loop_start() 
        except Exception as e:
            logger.critical(f"MQTT Error: {e}")
            return

        while True:
            try:
                if self.ser is None or not self.ser.is_open:
                    if self.connect_serial_device():
                        pass
                    else:
                        time.sleep(5)
                        continue 

                if self.ser and self.ser.in_waiting > 0:
                    try:
                        raw_bytes = self.ser.read_until(b'\x03')
                        if len(raw_bytes) > 0:
                            raw_string = raw_bytes.decode('utf-8', errors='ignore').strip()
                            self.process_and_publish(raw_string)
                    except Exception as e:
                        logger.error(f"Read Error (Device Dicabut?): {e}")
                        self.ser.close()
                        self.ser = None
                
                time.sleep(0.01)

            except KeyboardInterrupt:
                logger.info("Service Stopping...")
                if self.ser: self.ser.close()
                self.client.loop_stop()
                break
            except Exception as e:
                logger.critical(f"Critical Loop Error: {e}")
                time.sleep(5)

    def process_and_publish(self, raw_string):
        try:
            clean_string = re.sub(r'[^\x20-\x7E]', '', raw_string)
            status_match = re.search(r"(S|U)", clean_string)
            weight_match = re.search(r"([0-9]+\.?[0-9]*)", clean_string)

            if status_match and weight_match:
                status = status_match.group(1)
                weight = float(weight_match.group(1))

                if status == "U":
                    self.stable_count = 0
                    self.has_sent_stable = False
                
                elif status == "S":
                    self.stable_count += 1
                    
                    if self.stable_count >= self.required_stable:
                        if not self.has_sent_stable:
                            logger.info(f"STABLE EVENT: {weight}kg")
                            
                            mqtt_payload = {"weight": (f"{weight:.2f}")}
                            self.client.publish(TOPIC_PUBLISH, json.dumps(mqtt_payload))
                            
                            self.has_sent_stable = True 

        except Exception as e:
            logger.error(f"Logic Error: {e}")

if __name__ == "__main__":
    service = ScaleDeviceService()
    service.run()