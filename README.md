### 1. Installation

```bash
# Clone the repository
git clone <your-repo>
cd people-counting-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\\Scripts\\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Setup MQTT Broker (for testing)

```bash
# Using Docker (recommended)
docker run -it -p 1883:1883 eclipse-mosquitto

# Or install locally
# Ubuntu/Debian:
sudo apt-get install mosquitto mosquitto-clients
sudo systemctl start mosquitto

# macOS:
brew install mosquitto
mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf
```

### 3. Run the Pipeline

```bash
# Basic usage with webcam
python people_counting_pipeline.py

# With video file
VIDEO_SOURCE="path/to/your/video.mp4" python people_counting_pipeline.py
```
