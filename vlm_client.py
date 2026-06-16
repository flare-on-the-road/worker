import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_VISION_API_URL = os.getenv("VISION_API_URL", "")
_TIMEOUT = int(os.getenv("VISION_TIMEOUT", "15"))


def call_vlm(img_bytes: bytes, display_name: str) -> Optional[dict]:
    """
    AI 서버의 /vlm 엔드포인트에 이미지를 전송해 화재 여부를 판단한다.
    성공 시 {"is_fire": bool, "reason": str} 반환, 실패 시 None.
    """
    if not _VISION_API_URL:
        logger.warning("  ⚠ VISION_API_URL 미설정 → VLM 판단 생략")
        return None

    url = f"{_VISION_API_URL.rstrip('/')}/vlm"

    try:
        resp = requests.post(
            url,
            files={"image": ("frame.jpg", img_bytes, "image/jpeg")},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data.get("is_fire"), bool):
            logger.warning("  ⚠ [%s] VLM 응답 구조 오류: %s", display_name, data)
            return None

        logger.info(
            "  ✓ [%s] VLM 판단: is_fire=%s / %s",
            display_name,
            data["is_fire"],
            data["reason"],
        )
        return {"is_fire": data["is_fire"], "reason": str(data.get("reason", ""))}

    except requests.exceptions.Timeout:
        logger.error("  ❌ [%s] VLM 타임아웃 (%ds)", display_name, _TIMEOUT)
    except requests.exceptions.HTTPError as e:
        logger.error("  ❌ [%s] VLM HTTP 오류: %s", display_name, e)
    except Exception as e:
        logger.error("  ❌ [%s] VLM 예외: %s", display_name, e)

    return None
