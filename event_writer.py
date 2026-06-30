import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

BACKEND_API_URL = os.getenv("BACKEND_API_URL", "")
BACKEND_TIMEOUT = int(os.getenv("BACKEND_TIMEOUT", "10"))

KST = timezone(timedelta(hours=9))


def write_event(
    location: dict,
    vision_result: dict,
    snapshot_key: Optional[str] = None,
) -> Optional[int]:
    """
    Flask Backend POST /api/events 로 이벤트 저장.
    성공 시 event ID(int) 반환, 실패 시 None.
    """
    if not BACKEND_API_URL:
        logger.warning("  ⚠ BACKEND_API_URL 미설정 → 이벤트 저장 생략")
        return None

    url = f"{BACKEND_API_URL.rstrip('/')}/api/events"
    vlm_results = vision_result.get("vlm") or []
    detections = [
        {
            "label": d["class_name"],
            "confidence": round(d["confidence"], 4),
            "bbox": d.get("bbox", []),
        }
        for d in vision_result.get("detections", [])
    ]

    payload = {
        "cctv_id": location["id"],
        "cctv_name": location["display_name"],
        "location_name": location["location_name"],
        "detected_at": datetime.now(timezone.utc).astimezone(KST).isoformat(),
        "vlm_results": vlm_results,
        "detections": detections,
        "snapshot_key": snapshot_key,
    }

    try:
        resp = requests.post(url, json=payload, timeout=BACKEND_TIMEOUT)
        resp.raise_for_status()
        event_id = resp.json().get("id")
        logger.info(
            f"  ✓ [{location['display_name']}] 이벤트 저장 완료 "
            f"(id={event_id}, detections={len(payload['detections'])}개)"
        )
        return event_id

    except requests.exceptions.ConnectionError:
        logger.error(f"  ❌ [{location['display_name']}] Backend 연결 실패: {url}")
    except requests.exceptions.Timeout:
        logger.error(
            f"  ❌ [{location['display_name']}] Backend 타임아웃 ({BACKEND_TIMEOUT}s)"
        )
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ❌ [{location['display_name']}] Backend HTTP 오류: {e}")
    except Exception as e:
        logger.error(f"  ❌ [{location['display_name']}] Backend 예외: {e}")

    return None

