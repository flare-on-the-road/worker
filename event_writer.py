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
    summary = vision_result.get("summary", {})
    detected_classes = list({
        d["class_name"] for d in vision_result.get("risk_detections", [])
    })

    vlm = vision_result.get("vlm") or {}
    payload = {
        "cctv_id": location["id"],
        "cctv_name": location["display_name"],
        "location_name": location["location_name"],
        "detected_at": datetime.now(timezone.utc).astimezone(KST).isoformat(),
        "risk_score": summary.get("risk_score", 0),
        "risk_candidate": summary.get("risk_candidate", False),
        "is_fire": vlm.get("is_fire"),
        "detected_classes": detected_classes,
        "snapshot_key": snapshot_key,
        "vlm_reason": vlm.get("reason"),
    }

    try:
        resp = requests.post(url, json=payload, timeout=BACKEND_TIMEOUT)
        resp.raise_for_status()
        event_id = resp.json().get("id")
        logger.info(
            f"  ✓ [{location['display_name']}] 이벤트 저장 완료 "
            f"(id={event_id}, risk_score={payload['risk_score']})"
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


def patch_event_vlm(
    event_id: int,
    is_fire: bool,
    vlm_reason: Optional[str],
    display_name: str,
) -> bool:
    """
    Flask Backend PATCH /api/events/<id> 로 VLM 판단 결과 업데이트.
    성공 시 True, 실패 시 False.
    """
    if not BACKEND_API_URL:
        return False

    url = f"{BACKEND_API_URL.rstrip('/')}/api/events/{event_id}"
    payload = {"is_fire": is_fire, "vlm_reason": vlm_reason}

    try:
        resp = requests.patch(url, json=payload, timeout=BACKEND_TIMEOUT)
        resp.raise_for_status()
        logger.info(
            f"  ✓ [{display_name}] VLM 결과 업데이트 완료 "
            f"(id={event_id}, is_fire={is_fire})"
        )
        return True

    except requests.exceptions.ConnectionError:
        logger.error(f"  ❌ [{display_name}] Backend 연결 실패: {url}")
    except requests.exceptions.Timeout:
        logger.error(f"  ❌ [{display_name}] Backend 타임아웃 ({BACKEND_TIMEOUT}s)")
    except requests.exceptions.HTTPError as e:
        logger.error(f"  ❌ [{display_name}] Backend HTTP 오류 (PATCH): {e}")
    except Exception as e:
        logger.error(f"  ❌ [{display_name}] Backend 예외 (PATCH): {e}")

    return False
