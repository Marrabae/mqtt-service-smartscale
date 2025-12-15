import time
import serial
import re
import json
import os
import sys
from datetime import datetime
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

SERIAL_PORT = "COM1"
BAUDRATE = 9600

BROKER = os.getenv("MQTT_BROKER", "localhost")
PORT = int(os.getenv("MQTT_PORT", 1883))
TOPIC_PUBLISH = os.getenv("TOPIC_INPUT", "DATA/WEIGHT")

REGEX_PATTERN = re.compile(r"(S|U)\s*([0-9]+\.?[0-9]*)")

LOG_FILE = os.path.join(os.path.dirname(__file__), "mqtt_service.log")

def log(message):
    """Fungsi Log ke Console & File"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"[{timestamp}] {message}"
    print(log_message)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_message + "\n")
    except:
        pass

class ScaleDeviceService:
    def __init__(self):
        self.is_locked = False
        self.stable_count = 0
        self.required_stable = 3
        
        # Setup MQTT Client
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log(f"✅ MQTT Connected to {BROKER}:{PORT}")
        else:
            log(f"❌ Connection Failed, Code: {rc}")

    def on_disconnect(self, client, userdata, rc):
        log("⚠️ MQTT Disconnected")

    def run(self):
        log("="*60)
        log(f"SCALE SERVICE (PAHO MQTT VERSION)")
        log(f"Serial: {SERIAL_PORT} @ {BAUDRATE}")
        log(f"Mode: Localhost (No SSL)")
        log("="*60)

        try:
            ser = serial.Serial(SERIAL_PORT, BAUDRATE, timeout=0.1)
            log("✅ Serial Connected!")
        except Exception as e:
            log(f"❌ Serial Error: {e}")
            return

        # 2. Connect MQTT (Background Loop)
        try:
            log("Connecting to Broker...")
            self.client.connect(BROKER, PORT, 60)

            self.client.loop_start() 
        except Exception as e:
            log(f"❌ MQTT Setup Error: {e}")
            return

        while True:
            try:
                if ser.in_waiting > 0:
                    try:
                        raw_bytes = ser.read_until(b'\x03')
                        
                        if len(raw_bytes) > 0:
                            raw_string = raw_bytes.decode('utf-8', errors='ignore').strip()
                            self.process_and_publish(raw_string)
                            
                    except Exception as e:
                        log(f"⚠️ Read Error: {e}")

                time.sleep(0.01)

            except KeyboardInterrupt:
                log("🛑 Service Stopping...")
                self.client.loop_stop()
                break
            except Exception as e:
                log(f"❌ Critical Loop Error: {e}")
                time.sleep(5)

    def process_and_publish(self, raw_string):
        try:
            clean_string = re.sub(r'[^\x20-\x7E]', '', raw_string)
            
            status_match = re.search(r"(S|U)", clean_string)
            weight_match = re.search(r"([0-9]+\.?[0-9]*)", clean_string)

            if status_match and weight_match:
                status = status_match.group(1)
                weight = float(weight_match.group(1))
                
                should_publish = False
                action_type = ""

                if status == "S":
                    if not self.is_locked:
                        self.stable_count += 1
                    
                    if self.stable_count >= self.required_stable:
                        if not self.is_locked and weight > 0.05:
                            self.is_locked = True
                            should_publish = True
                            action_type = "LOCKED"
                else:
                    if not self.is_locked:
                        self.stable_count = 0

                if weight < 0.02:
                    if self.is_locked:
                        self.is_locked = False
                        self.stable_count = 0
                        should_publish = True
                        action_type = "RESET"

                if should_publish:
                    debug_info = {
                        "action": action_type,
                        "raw": clean_string,
                        "cnt": self.stable_count
                    }
                    log(f"🔔 ACTION: {json.dumps(debug_info)}")

                    mqtt_payload = {
                        "weight": (f"{weight:.2f}")
                    }
                    
                    info = self.client.publish(TOPIC_PUBLISH, json.dumps(mqtt_payload))
                    
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        log(f"⚠️ Publish Failed: {info.rc}")

        except Exception as e:
            log(f"⚠️ Process Logic Error: {e}")

if __name__ == "__main__":
    service = ScaleDeviceService()
    service.run()