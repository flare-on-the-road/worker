import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

VISION_API_URL = os.getenv("VISION_API_URL", "")
VISION_TIMEOUT = int(os.getenv("VISION_TIMEOUT", "15"))


def call_vision_api(
    image_bytes: bytes,
    display_name: str,
    confidence: float = 0.25,
    max_detections: int = 100,
) -> Optional[dict]:
    """
    Vision FastAPI 서버 POST /predict 호출.
    성공 시 결과 dict 반환, 실패 시 None 반환 (파이프라인 중단 방지).
    """
    if not VISION_API_URL:
        logger.warning(f"  ⚠ [{display_name}] VISION_API_URL 미설정 → Vision API 생략")
        return None

    url = f"{VISION_API_URL.rstrip('/')}/predict"

    try:
        resp = requests.post(
            url,
            files={"image": ("frame.jpg", image_bytes, "image/jpeg")},
            data={"confidence": confidence, "max_detections": max_detections},
            timeout=VISION_TIMEOUT,
        )
        resp.raise_for_status()
        result = resp.json()

        summary = result.get("summary", {})
        risk_count = summary.get("risk_detection_count", 0)
        max_conf = summary.get("max_confidence", 0.0)

        if risk_count > 0:
            logger.info(
                f"  🔥 [{display_name}] 위험 탐지! "
                f"risk={risk_count}개 | max_conf={max_conf:.2f}"
            )
        else:
            logger.info(
                f"  ✓ [{display_name}] 탐지 정상 (위험 없음, "
                f"total={summary.get('total_detections', 0)})"
            )

        return result

    except requests.exceptions.ConnectionError:
        logger.error(f"  ❌ [{display_name}] Vision API 연결 실패: {url}")
    except requests.exceptions.Timeout:
        logger.error(f"  ❌ [{display_name}] Vision API 타임아웃 ({VISION_TIMEOUT}s): {url}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ❌ [{display_name}] Vision API HTTP 오류: {e}")
    except Exception as e:
        logger.error(f"  ❌ [{display_name}] Vision API 예외: {e}")

    return None
