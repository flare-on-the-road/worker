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
    is_fire: Optional[bool] = None,
    vlm_reason: Optional[str] = None,
) -> bool:
    """
    Flask Backend POST /api/events 로 이벤트 저장.
    성공 시 True, 실패 시 False 반환.

    is_fire: VLM 2차 판단 결과. None이면 미판단(1차 탐지만 존재).
    """
    if not BACKEND_API_URL:
        logger.warning("  ⚠ BACKEND_API_URL 미설정 → 이벤트 저장 생략")
        return False

    url = f"{BACKEND_API_URL.rstrip('/')}/api/events"
    summary = vision_result.get("summary", {})
    detected_classes = list({
        d["class_name"] for d in vision_result.get("risk_detections", [])
    })

    payload = {
        "cctv_id": location["id"],
        "cctv_name": location["display_name"],
        "location_name": location["location_name"],
        "detected_at": datetime.now(timezone.utc).astimezone(KST).isoformat(),
        "risk_score": summary.get("risk_score", 0),
        "risk_candidate": summary.get("risk_candidate", False),
        "is_fire": is_fire,
        "detected_classes": detected_classes,
        "snapshot_key": snapshot_key,
        "vlm_reason": vlm_reason,
    }

    try:
        resp = requests.post(url, json=payload, timeout=BACKEND_TIMEOUT)
        resp.raise_for_status()
        logger.info(
            f"  ✓ [{location['display_name']}] 이벤트 저장 완료 "
            f"(risk_score={payload['risk_score']}, is_fire={is_fire})"
        )
        return True

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

    return False
