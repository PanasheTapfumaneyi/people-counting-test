import cv2
import asyncio
import json
from datetime import datetime
from typing import Optional, Dict, Any
import paho.mqtt.client as mqtt
import numpy as np
import time
from collections import defaultdict, deque
from threading import Thread
from scipy.optimize import linear_sum_assignment
from config import *


class MockMachineInfo:
    def __getitem__(self, key):
        return {"machine_code": APP_CONFIG["machine_code"], "area": APP_CONFIG["area"]}[
            key
        ]


class MockLogger:
    def __init__(self, name):
        self.name = name

    def error(self, msg):
        print(f"ERROR: {msg}")

    def warning(self, msg):
        print(f"WARNING: {msg}")

    def info(self, msg):
        print(f"INFO: {msg}")


class SystemStatus:
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNHEALTHY = "UNHEALTHY"


class HealthMetric:
    def __init__(self, name, value, timestamp, status):
        self.name = name
        self.value = value
        self.timestamp = timestamp
        self.status = status


mqtt_client = mqtt.Client()
mqtt_client.connect(
    MQTT_CONFIG["broker"], MQTT_CONFIG["port"], MQTT_CONFIG["keepalive"]
)
mqtt_client.loop_start()


async def publish(topic, payload, retain=False):
    def _publish():
        mqtt_client.publish(topic, payload, retain=retain, qos=MQTT_CONFIG["qos"])

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _publish)
    return True


async def subscribe(topic):
    def _subscribe():
        mqtt_client.subscribe(topic, qos=MQTT_CONFIG["qos"])

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _subscribe)
    return None


MACHINE_INFO = MockMachineInfo()
Logger = MockLogger


class DataPipeline:
    def __init__(self):
        self.machine_code = MACHINE_INFO["machine_code"]
        self.area = MACHINE_INFO["area"]
        self.topic = f"{APP_CONFIG['namespace']}/{self.area}/{self.machine_code}/MES"
        self.output_topic = f"{APP_CONFIG['namespace']}/{self.area}/{self.machine_code}/{APP_CONFIG['output_topic_suffix']}"
        self.health_namespace = f"{APP_CONFIG['namespace']}/{self.area}/{self.machine_code}/{APP_CONFIG['health_topic_suffix']}"
        self.state: Dict[str, Any] = {}
        self.metrics: Dict[str, HealthMetric] = {}
        self.last_health_update = time.time()

    def _update_metric(self, name: str, value: Any, status: SystemStatus) -> None:
        """Update a health metric."""
        self.metrics[name] = HealthMetric(
            name=name, value=value, timestamp=datetime.now(), status=status
        )

    async def _publish_metrics(self) -> None:
        """Publish current metrics to MQTT."""
        current_time = time.time()
        if (
            current_time - self.last_health_update
            < PERFORMANCE_CONFIG["health_update_interval"]
        ):
            return

        self.last_health_update = current_time

        metrics_data = {
            name: {
                "value": metric.value,
                "status": metric.status,
                "timestamp": metric.timestamp.isoformat(),
            }
            for name, metric in self.metrics.items()
        }
        await publish(self.health_namespace, json.dumps(metrics_data))

    async def extractor(self) -> Optional[Dict[str, Any]]:
        """Extract data - defined later in implementation"""
        raise NotImplementedError

    async def transformer(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform data - defined later in implementation"""
        raise NotImplementedError

    async def loader(self, data: Dict[str, Any]) -> bool:
        """Load data - defined later in implementation"""
        raise NotImplementedError


class SimpleCentroidTracker:
    """Improved centroid tracker for people with better ID management"""

    def __init__(self):
        self.nextObjectID = 0
        self.objects = {}
        self.disappeared = {}
        self.max_disappeared = TRACKING_CONFIG["max_disappeared"]
        self.max_distance = TRACKING_CONFIG["max_distance"]
        self.min_frames_for_stable_id = TRACKING_CONFIG["min_frames_for_stable_id"]

    def register(self, centroid):
        """Register a new object"""
        self.objects[self.nextObjectID] = (centroid, 1)
        self.disappeared[self.nextObjectID] = 0
        self.nextObjectID += 1

    def deregister(self, object_id):
        """Deregister object"""
        del self.objects[object_id]
        del self.disappeared[object_id]

    def update(self, detections):
        """Update tracker with new objects"""
        if len(detections) == 0:
            for object_id in list(self.disappeared.keys()):
                self.disappeared[object_id] += 1
                if self.disappeared[object_id] > self.max_disappeared:
                    self.deregister(object_id)
            return {}

        input_centroids = []
        for (x1, y1, x2, y2, _) in detections:
            cx = int((x1 + x2) / 2.0)
            cy = int(y1 + (y2 - y1) * 0.2)
            input_centroids.append((cx, cy))

        if len(self.objects) == 0:
            for centroid in input_centroids:
                self.register(centroid)
        else:
            object_ids = list(self.objects.keys())
            object_centroids = [self.objects[oid][0] for oid in object_ids]

            D = np.zeros((len(object_centroids), len(input_centroids)))
            for i, obj_centroid in enumerate(object_centroids):
                for j, input_centroid in enumerate(input_centroids):
                    D[i, j] = np.linalg.norm(
                        np.array(obj_centroid) - np.array(input_centroid)
                    )

            rows, cols = linear_sum_assignment(D)

            used_rows = set()
            used_cols = set()

            max_shift = TRACKING_CONFIG["max_centroid_shift"]

            for row, col in zip(rows, cols):
                if D[row, col] > self.max_distance:
                    continue

                object_id = object_ids[row]
                current_centroid, frames_present = self.objects[object_id]
                new_centroid = input_centroids[col]

                dx = abs(new_centroid[0] - current_centroid[0])
                dy = abs(new_centroid[1] - current_centroid[1])

                if dx < max_shift and dy < max_shift:
                    alpha = TRACKING_CONFIG["smoothing_factor"]
                    smoothed_cx = int(
                        alpha * new_centroid[0] + (1 - alpha) * current_centroid[0]
                    )
                    smoothed_cy = int(
                        alpha * new_centroid[1] + (1 - alpha) * current_centroid[1]
                    )
                    self.objects[object_id] = (
                        (smoothed_cx, smoothed_cy),
                        frames_present + 1,
                    )
                    self.disappeared[object_id] = 0
                    used_rows.add(row)
                    used_cols.add(col)

            unused_rows = set(range(D.shape[0])) - used_rows
            unused_cols = set(range(D.shape[1])) - used_cols

            for row in unused_rows:
                object_id = object_ids[row]
                self.disappeared[object_id] += 1
                _, frames_present = self.objects[object_id]
                if (
                    self.disappeared[object_id] > self.max_disappeared
                    and frames_present > self.min_frames_for_stable_id
                ):
                    self.deregister(object_id)

            for col in unused_cols:
                self.register(input_centroids[col])

        stable_objects = {}
        for obj_id, (centroid, frames_present) in self.objects.items():
            if frames_present >= self.min_frames_for_stable_id:
                stable_objects[obj_id] = centroid

        return stable_objects


class PeopleCountingPipeline(DataPipeline):
    """Pipeline for people counting"""
    def __init__(self, video_source=VIDEO_CONFIG["default_source"]):
        super().__init__()

        # Video source
        self.video_source = video_source
        self.cap = None

        # Detection setup
        self.confidence_threshold = DETECTION_CONFIG["confidence_threshold"]
        self.nms_threshold = DETECTION_CONFIG["nms_threshold"]

        # Initialize YOLO
        try:
            self.net = cv2.dnn.readNet(
                DETECTION_CONFIG["model_weights"], DETECTION_CONFIG["model_config"]
            )
            self.classes = (
                open(DETECTION_CONFIG["classes_file"]).read().strip().split("\n")
            )
            self.use_yolo = True

            # Use GPU if available
            if DETECTION_CONFIG["use_gpu"]:
                self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_CUDA)
                self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CUDA)
        except Exception as e:
            print(f"YOLO not available, using background subtraction: {e}")
            self.bg_subtractor = cv2.createBackgroundSubtractorMOG2(detectShadows=True)
            self.use_yolo = False

        # Tracking
        self.tracker = SimpleCentroidTracker()
        self.track_history = defaultdict(
            lambda: deque(maxlen=TRACKING_CONFIG["track_history_length"])
        )

        # Counting
        self.counting_line = None
        self.occupancy_count = 0
        self.entry_count = 0
        self.exit_count = 0
        self.recent_events = deque(maxlen=COUNTING_CONFIG["max_recent_events"])

        # State tracking
        self.track_states = {}
        self.processed_crossings = set()

        # Performance metrics
        self.frame_count = 0
        self.start_time = time.time()
        self.last_frame_time = time.time()
        self.fps_history = deque(maxlen=PERFORMANCE_CONFIG["fps_window"])

    def setup_counting_line(self, frame_shape):
        """Setup counting line"""
        height, width = frame_shape[:2]
        # Horizontal line at configured position
        self.counting_line = {
            "y": int(height * COUNTING_CONFIG["line_position"]),
            "x1": int(width * COUNTING_CONFIG["line_margin"]),
            "x2": int(width * (1 - COUNTING_CONFIG["line_margin"])),
        }
        print(f"Counting line set at y={self.counting_line['y']}")

    def detect_people_yolo(self, frame):
        """Detect people using YOLO"""
        blob = cv2.dnn.blobFromImage(
            frame, 1 / 255.0, (416, 416), swapRB=True, crop=False
        )
        self.net.setInput(blob)

        output_layer_names = self.net.getUnconnectedOutLayersNames()
        outputs = self.net.forward(output_layer_names)

        detections = []
        height, width = frame.shape[:2]

        for output in outputs:
            for detection in output:
                scores = detection[5:]
                class_id = np.argmax(scores)
                confidence = scores[class_id]

                if class_id == 0 and confidence > self.confidence_threshold:
                    center_x = int(detection[0] * width)
                    center_y = int(detection[1] * height)
                    w = int(detection[2] * width)
                    h = int(detection[3] * height)

                    x1 = max(0, int(center_x - w / 2))
                    y1 = max(0, int(center_y - h / 2))
                    x2 = min(width, int(center_x + w / 2))
                    y2 = min(height, int(center_y + h / 2))

                    # Size filtering for people
                    box_width = x2 - x1
                    box_height = y2 - y1
                    aspect_ratio = box_height / box_width if box_width > 0 else 0
                    area = box_width * box_height

                    # Filter based on configured dimensions
                    if (
                        area > DETECTION_CONFIG["min_person_area"]
                        and box_width < width * DETECTION_CONFIG["max_person_area"]
                        and box_height < height * DETECTION_CONFIG["max_person_area"]
                        and aspect_ratio > DETECTION_CONFIG["min_aspect_ratio"]
                        and aspect_ratio < DETECTION_CONFIG["max_aspect_ratio"]
                    ):
                        detections.append([x1, y1, x2, y2, confidence])

        # Apply NMS
        if len(detections) > 0:
            boxes = np.array([d[:4] for d in detections])
            confidences = np.array([d[4] for d in detections])
            indices = cv2.dnn.NMSBoxes(
                boxes, confidences, self.confidence_threshold, self.nms_threshold
            )

            if len(indices) > 0:
                detections = [detections[i] for i in indices.flatten()]

        return np.array(detections) if detections else np.array([]).reshape(0, 5)

    def filter_overlapping_detections(self, detections, overlap_threshold=0.6):
        """Remove detections that overlap"""
        if len(detections) <= 1:
            return detections

        filtered = []
        detections = sorted(
            detections, key=lambda x: x[4], reverse=True
        )  # Sort by confidence

        for i, det1 in enumerate(detections):
            x1_1, y1_1, x2_1, y2_1, conf1 = det1
            keep = True

            for det2 in filtered:
                x1_2, y1_2, x2_2, y2_2, conf2 = det2

                # Calculate intersection area
                x1_int = max(x1_1, x1_2)
                y1_int = max(y1_1, y1_2)
                x2_int = min(x2_1, x2_2)
                y2_int = min(y2_1, y2_2)

                if x1_int < x2_int and y1_int < y2_int:
                    intersection = (x2_int - x1_int) * (y2_int - y1_int)
                    area1 = (x2_1 - x1_1) * (y2_1 - y1_1)
                    area2 = (x2_2 - x1_2) * (y2_2 - y1_2)

                    # Check if overlap is too high
                    overlap_ratio = intersection / min(area1, area2)
                    if overlap_ratio > overlap_threshold:
                        keep = False
                        break

            if keep:
                filtered.append(det1)

        return np.array(filtered) if filtered else np.array([]).reshape(0, 5)

    def detect_people(self, frame):
        if not self.use_yolo:
            raise RuntimeError("YOLO not available")

        detections = self.detect_people_yolo(frame)
        detections = self.filter_overlapping_detections(detections)
        return detections

    def check_line_crossing(self, track_id, current_pos, frame_number):
        """Check if a person crossed the line"""
        if self.counting_line is None:
            return None

        history = self.track_history[track_id]
        if len(history) < 2:
            return None

        line_y = self.counting_line["y"]
        line_x1 = self.counting_line["x1"]
        line_x2 = self.counting_line["x2"]

        # Get current and previous position
        current_y = current_pos[1]
        previous_pos = history[-2] if len(history) >= 2 else history[-1]
        previous_y = previous_pos[1]
        current_x = current_pos[0]

        # Check if person is within line's range
        if not (line_x1 <= current_x <= line_x2):
            return None

        # Check for line crossing
        crossing_key = f"{track_id}_{frame_number//COUNTING_CONFIG['event_window']}"

        if crossing_key in self.processed_crossings:
            return None

        if (previous_y < line_y <= current_y) or (current_y <= line_y < previous_y):
            self.processed_crossings.add(crossing_key)

            # Determine direction
            if previous_y < line_y and current_y >= line_y:
                return "entry"
            elif previous_y > line_y and current_y <= line_y:
                return "exit"

        return None

    def update_counts(self, event_type, track_id, frame_number):
        """Update entry/exit count"""
        if event_type == "entry":
            self.entry_count += 1
            self.occupancy_count += 1
            print(
                f"ENTRY detected - ID {track_id} - New occupancy: {self.occupancy_count}"
            )
        elif event_type == "exit":
            self.exit_count += 1
            self.occupancy_count = max(0, self.occupancy_count - 1)
            print(
                f"EXIT detected - ID {track_id} - New occupancy: {self.occupancy_count}"
            )

        # Add to events
        event = {
            "event_id": f"evt_{len(self.recent_events)}",
            "type": event_type,
            "timestamp": datetime.now().isoformat(),
            "person_id": f"p_{track_id}",
            "confidence": 0.9,
            "detection_zone": "entrance_line",
            "frame_number": frame_number,
        }
        self.recent_events.append(event)

    def draw_visualization(self, frame, detections, tracks):
        """Draw detection and visualizations"""
        # Draw counting line if enabled
        if VISUALIZATION_CONFIG["show_counting_line"] and self.counting_line:
            cv2.line(
                frame,
                (self.counting_line["x1"], self.counting_line["y"]),
                (self.counting_line["x2"], self.counting_line["y"]),
                VISUALIZATION_CONFIG["counting_line_color"],
                3,
            )
            cv2.putText(
                frame,
                "COUNTING LINE",
                (self.counting_line["x1"], self.counting_line["y"] - 10),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.7,
                VISUALIZATION_CONFIG["counting_line_color"],
                2,
            )

        # Draw detections if enabled
        if VISUALIZATION_CONFIG["show_detections"]:
            for detection in detections:
                x1, y1, x2, y2, conf = detection
                cv2.rectangle(
                    frame,
                    (int(x1), int(y1)),
                    (int(x2), int(y2)),
                    VISUALIZATION_CONFIG["detection_color"],
                    2,
                )
                cv2.putText(
                    frame,
                    f"Person {conf:.2f}",
                    (int(x1), int(y1) - 10),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    VISUALIZATION_CONFIG["detection_color"],
                    2,
                )

        # Draw tracks if enabled
        if VISUALIZATION_CONFIG["show_tracks"]:
            for track_id, centroid in tracks.items():
                cv2.circle(frame, centroid, 5, VISUALIZATION_CONFIG["track_color"], -1)
                cv2.putText(
                    frame,
                    f"ID: {track_id}",
                    (centroid[0] + 10, centroid[1]),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    VISUALIZATION_CONFIG["track_color"],
                    2,
                )

                # Draw track history
                history = self.track_history[track_id]
                if len(history) > 1:
                    points = np.array(list(history), np.int32)
                    cv2.polylines(
                        frame, [points], False, VISUALIZATION_CONFIG["track_color"], 2
                    )

        # Draw statistics if enabled
        if VISUALIZATION_CONFIG["show_stats"]:
            current_fps = np.mean(self.fps_history) if len(self.fps_history) > 0 else 0
            stats = [
                f"Occupancy: {self.occupancy_count}",
                f"Entries: {self.entry_count}",
                f"Exits: {self.exit_count}",
                f"FPS: {current_fps:.1f}",
            ]

            for i, stat in enumerate(stats):
                cv2.putText(
                    frame,
                    stat,
                    (10, 30 + i * 30),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    VISUALIZATION_CONFIG["text_scale"],
                    VISUALIZATION_CONFIG["text_color"],
                    VISUALIZATION_CONFIG["text_thickness"],
                )

        return frame

    async def extractor(self) -> Optional[Dict[str, Any]]:
        """Extract frame from video"""
        try:
            if self.cap is None:
                self.cap = cv2.VideoCapture(self.video_source)
                if not self.cap.isOpened():
                    print("Error: Could not open video source")
                    return None

                # Set desired frame properties if specified
                if VIDEO_CONFIG["frame_width"] > 0:
                    self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, VIDEO_CONFIG["frame_width"])
                if VIDEO_CONFIG["frame_height"] > 0:
                    self.cap.set(
                        cv2.CAP_PROP_FRAME_HEIGHT, VIDEO_CONFIG["frame_height"]
                    )
                if VIDEO_CONFIG["fps"] > 0:
                    self.cap.set(cv2.CAP_PROP_FPS, VIDEO_CONFIG["fps"])

            ret, frame = self.cap.read()
            if not ret:
                print("End of video")
                return None

            # Setup counting line if not set
            if self.counting_line is None:
                self.setup_counting_line(frame.shape)

            self.frame_count += 1

            return {
                "frame": frame,
                "frame_number": self.frame_count,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            print(f"Error in extractor: {e}")
            return None

    async def transformer(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process frame for people detection and tracking"""
        try:
            if data is None:
                return None

            frame = data["frame"]
            frame_number = data["frame_number"]

            # Calculate FPS
            current_time = time.time()
            frame_time = current_time - self.last_frame_time
            self.last_frame_time = current_time
            if frame_time > 0:
                self.fps_history.append(1 / frame_time)

            # Detect people
            detections = self.detect_people(frame)

            # Update tracking
            tracks = self.tracker.update(detections)

            # Update track history and check for line crossings
            active_tracks = []
            for track_id, centroid in tracks.items():
                self.track_history[track_id].append(centroid)

                # Check for line crossing
                crossing = self.check_line_crossing(track_id, centroid, frame_number)
                if crossing:
                    self.update_counts(crossing, track_id, frame_number)

                # Build active track info
                track_info = {
                    "person_id": f"p_{track_id}",
                    "bbox": [centroid[0] - 25, centroid[1] - 50, 50, 100],
                    "confidence": 0.9,
                    "track_length": len(self.track_history[track_id]),
                    "direction": "unknown",
                }
                active_tracks.append(track_info)

            # Calculate overall FPS
            elapsed = current_time - self.start_time
            fps = self.frame_count / elapsed if elapsed > 0 else 0

            # Check system health
            current_fps = np.mean(self.fps_history) if self.fps_history else 0
            frame_time_ms = frame_time * 1000

            if current_fps < HEALTH_THRESHOLDS["min_acceptable_fps"]:
                self._update_metric("fps", current_fps, SystemStatus.UNHEALTHY)
            elif current_fps < HEALTH_THRESHOLDS["min_acceptable_fps"] * 1.5:
                self._update_metric("fps", current_fps, SystemStatus.DEGRADED)
            else:
                self._update_metric("fps", current_fps, SystemStatus.HEALTHY)

            if frame_time_ms > HEALTH_THRESHOLDS["max_acceptable_frame_time"]:
                self._update_metric("frame_time", frame_time_ms, SystemStatus.UNHEALTHY)
            elif frame_time_ms > HEALTH_THRESHOLDS["max_acceptable_frame_time"] * 0.8:
                self._update_metric("frame_time", frame_time_ms, SystemStatus.DEGRADED)
            else:
                self._update_metric("frame_time", frame_time_ms, SystemStatus.HEALTHY)

            # Draw visualization
            vis_frame = self.draw_visualization(frame.copy(), detections, tracks)

            # Display frame if in visualization mode
            if (
                VISUALIZATION_CONFIG["show_detections"]
                or VISUALIZATION_CONFIG["show_tracks"]
            ):
                cv2.imshow("People Counting", vis_frame)
                cv2.waitKey(1)

            # Output data
            result = {
                "timestamp": data["timestamp"],
                "frame_number": frame_number,
                "current_occupancy": self.occupancy_count,
                "total_entries": self.entry_count,
                "total_exits": self.exit_count,
                "recent_events": list(self.recent_events)[-5:],
                "active_tracks": active_tracks,
                "processing_metrics": {
                    "fps": round(fps, 2),
                    "frame_time_ms": round(frame_time_ms, 2),
                    "detection_count": len(detections),
                    "tracking_count": len(tracks),
                    "processing_time_ms": round(frame_time * 1000, 2),
                },
            }

            return result

        except Exception as e:
            print(f"Error in transformer: {e}")
            return None

    async def loader(self, data: Dict[str, Any]) -> bool:
        """Publish results to MQTT"""
        try:
            if data is None:
                return False

            # Publish main data
            payload = json.dumps(data, indent=2)
            await publish(self.output_topic, payload)

            # Publish health metrics
            await self._publish_metrics()

            return True

        except Exception as e:
            print(f"Error in loader: {e}")
            return False

    async def run(self):
        """Main execution loop"""
        print("Starting People Counting Pipeline...")

        try:
            while True:
                # Extract frame
                frame_data = await self.extractor()
                if frame_data is None:
                    print("No more frames, stopping...")
                    break

                # Transform (detect and track)
                result = await self.transformer(frame_data)

                # Load (publish to MQTT)
                success = await self.loader(result)

                if not success:
                    print("Failed to publish data")

                # Small delay to prevent overwhelming the system
                await asyncio.sleep(0.001)

        except KeyboardInterrupt:
            print("Stopping pipeline...")
        finally:
            if self.cap:
                self.cap.release()
            cv2.destroyAllWindows()


async def main():
    pipeline = PeopleCountingPipeline(
        video_source="videos/footage.mp4"
    )  # Set video file as source
    await pipeline.run()


if __name__ == "__main__":
    asyncio.run(main())
