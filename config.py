# MQTT Configuration
MQTT_CONFIG = {
    "broker": "localhost",
    "port": 1883,
    "keepalive": 60,
    "client_id": "people_counter_01",
    "qos": 1
}

# Application Configuration
APP_CONFIG = {
    "namespace": "DAKRI/MAURITIUS",
    "machine_code": "TEST001",
    "area": "ENTRANCE",
    "health_topic_suffix": "HEALTH/PEOPLE_COUNTING",
    "output_topic_suffix": "PEOPLE_COUNTING"
}

# Video Configuration
VIDEO_CONFIG = {
    "default_source": 0,  
    "frame_width": 1280,  
    "frame_height": 720,  
    "fps": 30       
}

# Detection Configuration
DETECTION_CONFIG = {
    "model_weights": "yolo/yolov4-tiny.weights",
    "model_config": "yolo/yolov4-tiny.cfg",
    "classes_file": "coco/coco.names",
    "confidence_threshold": 0.7,
    "nms_threshold": 0.4,
    "use_gpu": True,
    "min_person_area": 500,  
    "max_person_area": 0.8, 
    "min_aspect_ratio": 1.2, 
    "max_aspect_ratio": 4.0 
}

# Tracking Configuration
TRACKING_CONFIG = {
    "max_disappeared": 30,   
    "max_distance": 60,       
    "min_frames_for_stable_id": 1, 
    "track_history_length": 30,    
    "max_centroid_shift": 50,       
    "smoothing_factor": 0.5  
}

# Counting Configuration
COUNTING_CONFIG = {
    "line_position": 0.6,   
    "line_margin": 0.1,   
    "event_window": 10,     
    "max_recent_events": 10  
}

# Performance Configuration
PERFORMANCE_CONFIG = {
    "fps_window": 30,       
    "health_update_interval": 5,  
    "log_level": "INFO"     
}

# Visualization Configuration
VISUALIZATION_CONFIG = {
    "show_detections": True,
    "show_tracks": True,
    "show_counting_line": True,
    "show_stats": True,
    "detection_color": (0, 255, 0),     
    "track_color": (0, 255, 255),       
    "counting_line_color": (255, 0, 0),  
    "text_color": (255, 0, 0),          
    "text_scale": 1.0,
    "text_thickness": 2
}

# System Health Thresholds
HEALTH_THRESHOLDS = {
    "min_acceptable_fps": 10,
    "max_acceptable_frame_time": 100,  
    "max_acceptable_processing_time": 50  
}