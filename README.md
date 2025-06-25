### 1. Installation

```bash
# Clone the repository
git clone (https://github.com/PanasheTapfumaneyi/people-counting-test/)
cd people-counting-test

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
# Using Docker
docker run -it -p 1883:1883 eclipse-mosquitto

```

### 3. Run the Pipeline

```bash
# Basic usage with webcam
python people_counting_pipeline.py

# With video file
VIDEO_SOURCE="videos/(examplevideo.mp4)" python people_counting_pipeline.py
```
