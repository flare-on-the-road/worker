import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_VISION_API_URL = os.getenv("VISION_API_URL", "")
_TIMEOUT = int(os.getenv("VISION_TIMEOUT", "15"))


def call_vlm(img_bytes: bytes, display_name: str, detections: list) -> Optional[list]:
    """
    AI 서버의 /vlm 엔드포인트에 이미지를 전송해 항목별 오탐 여부를 판단한다.
    성공 시 [{"class_name": str, "is_false_positive": bool, "reason": str}, ...] 반환.
    실패 시 None.
    """
    if not _VISION_API_URL:
        logger.warning("  ⚠ VISION_API_URL 미설정 → VLM 판단 생략")
        return None

    import json as _json
    url = f"{_VISION_API_URL.rstrip('/')}/vlm"

    try:
        resp = requests.post(
            url,
            files={"image": ("frame.jpg", img_bytes, "image/jpeg")},
            data={"detections": _json.dumps(detections)},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list) or not all(
            isinstance(r, dict) and "class_name" in r and "is_false_positive" in r
            for r in data
        ):
            logger.warning("  ⚠ [%s] VLM 응답 구조 오류: %s", display_name, data)
            return None

        logger.info("  ✓ [%s] VLM 판단: %s", display_name, data)
        return data

    except requests.exceptions.Timeout:
        logger.error("  ❌ [%s] VLM 타임아웃 (%ds)", display_name, _TIMEOUT)
    except requests.exceptions.HTTPError as e:
        logger.error("  ❌ [%s] VLM HTTP 오류: %s", display_name, e)
    except Exception as e:
        logger.error("  ❌ [%s] VLM 예외: %s", display_name, e)

    return None
