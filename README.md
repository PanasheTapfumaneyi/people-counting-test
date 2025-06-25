### 1. Installation

```bash
# Clone the repository
git clone (https://github.com/PanasheTapfumaneyi/people-counting-test/)
cd people-counting-test

# Create virtual environment
python -m venv venv
venv\\Scripts\\activate 

# Install dependencies
pip install -r requirements.txt
```

### 2. Setup MQTT Broker (for testing)

```bash
# Using Docker
docker run -it -p 1883:1883 eclipse-mosquitto

```

### 3. Run the Pipeline

```bash
python people_counting_pipeline.py

# Update video file if needed
Line 757:  video_source="videos/footage.mp4"
```

### 4. Subscribe to the pipeline messages 

Subscribing to all the pipeline messages
```bash
mosquitto_sub -t "DAKRI/MAURITIUS/+/+/PEOPLE_COUNTING" -h localhost
```
Subscribing to the health messages 
```bash
mosquitto_sub -t "DAKRI/MAURITIUS/+/+/HEALTH/PEOPLE_COUNTING" -h localhost
```
