from factorykit.configs.manager import MACHINE_INFO
from factorykit.connectors.mqtt import publish, subscribe
from factorykit.utils.logger import Logger
from factorykit.utils.health import HealthMetric, SystemStatus
import asyncio
import json
from datetime import datetime
from typing import Optional, Dict, Any

logger = Logger("PipelineTemplate")

class DataPipeline:
    def __init__(self):
        self.machine_code = MACHINE_INFO["machine_code"]
        self.area = MACHINE_INFO["area"]
        self.topic = f"DAKRI/MAURITIUS/{self.area}/{self.machine_code}/MES"
        self.output_topic = f"DAKRI/MAURITIUS/{self.area}/{self.machine_code}/PIPELINE"
        self.health_namespace = f"DAKRI/MAURITIUS/{self.area}/{self.machine_code}/HEALTH/PIPELINE"
        self.state: Dict[str, Any] = {}
        self.metrics: Dict[str, HealthMetric] = {}
        
    def _update_metric(self, name: str, value: Any, status: SystemStatus) -> None:
        """Update a health metric."""
        self.metrics[name] = HealthMetric(
            name=name, value=value, timestamp=datetime.now(), status=status
        )

    async def _publish_metrics(self) -> None:
        """Publish current metrics to MQTT."""
        metrics_data = {
            name: {
                "value": metric.value,
                "status": metric.status.value,
                "timestamp": metric.timestamp.isoformat(),
            }
            for name, metric in self.metrics.items()
        }
        await publish(self.health_namespace, json.dumps(metrics_data))
        
    async def extractor(self) -> Optional[Dict[str, Any]]:
        """
        Extract data from the source (e.g., MQTT, API, etc.)
        Returns the extracted data or None if extraction fails
        """
        try:
            start_time = datetime.now()
            payload = await subscribe(self.topic)
            if payload is None:
                # Add retry mechanism
                for _ in range(3):
                    payload = await subscribe(self.topic)
                    if payload is not None:
                        break
                    await asyncio.sleep(1)
            
            if payload is not None:
                # Process and validate the payload
                processed_data = self._process_payload(payload)
                processing_time = (datetime.now() - start_time).total_seconds()
                self._update_metric(
                    "extraction_time",
                    processing_time,
                    SystemStatus.HEALTHY if processing_time < 2 else SystemStatus.DEGRADED
                )
                return processed_data
            else:
                self._update_metric("extraction_status", "no_data", SystemStatus.DEGRADED)
                return None
                
        except Exception as e:
            logger.error(f"Error in extractor: {e}")
            self._update_metric("extraction_error", str(e), SystemStatus.UNHEALTHY)
            return None

    def _process_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and validate the incoming payload
        Override this method in your implementation
        """
        return payload

    async def transformer(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform the extracted data
        Override this method in your implementation
        """
        try:
            start_time = datetime.now()
            if data is None:
                self._update_metric("transformation_status", "no_data", SystemStatus.DEGRADED)
                return None
                
            # Add your transformation logic here
            transformed_data = data.copy()
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_metric(
                "transformation_time",
                processing_time,
                SystemStatus.HEALTHY if processing_time < 1 else SystemStatus.DEGRADED
            )
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error in transformer: {e}")
            self._update_metric("transformation_error", str(e), SystemStatus.UNHEALTHY)
            return None

    async def loader(self, data: Dict[str, Any]) -> bool:
        """
        Load the transformed data to the destination
        Override this method in your implementation
        """
        try:
            start_time = datetime.now()
            if data is None:
                self._update_metric("loading_status", "no_data", SystemStatus.DEGRADED)
                return False
                
            # Add your loading logic here
            payload = json.dumps(data)
            await publish(self.output_topic, payload, retain=False)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_metric(
                "loading_time",
                processing_time,
                SystemStatus.HEALTHY if processing_time < 1 else SystemStatus.DEGRADED
            )
            return True
            
        except Exception as e:
            logger.error(f"Error in loader: {e}")
            self._update_metric("loading_error", str(e), SystemStatus.UNHEALTHY)
            return False

    async def run(self):
        """
        Main pipeline execution loop
        """
        while True:
            try:
                # Extract
                extracted_data = await self.extractor()
                
                # Transform
                transformed_data = await self.transformer(extracted_data)
                
                # Load
                success = await self.loader(transformed_data)
                
                if not success:
                    logger.warning("Pipeline cycle failed")
                    self._update_metric("pipeline_status", "failed", SystemStatus.DEGRADED)
                else:
                    self._update_metric("pipeline_status", "success", SystemStatus.HEALTHY)
                
                # Publish health metrics
                await self._publish_metrics()
                
                # Add appropriate sleep time based on your needs
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Error in pipeline execution: {e}")
                self._update_metric("pipeline_error", str(e), SystemStatus.UNHEALTHY)
                await self._publish_metrics()
                await asyncio.sleep(5)  # Back off on error

# Example usage:
"""
class MyCustomPipeline(DataPipeline):
    def _process_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Add custom payload processing
        return payload

    async def transformer(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Add custom transformation logic
        return data

    async def loader(self, data: Dict[str, Any]) -> bool:
        # Add custom loading logic
        return await super().loader(data)

# Usage in main.py:
pipeline = MyCustomPipeline()
await pipeline.run()
""" 