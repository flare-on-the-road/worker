import base64
import time
import requests
import boto3
import subprocess
import tempfile
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import logging
from dotenv import load_dotenv
from botocore.config import Config
import urllib3

from vision_client import call_vision_api
from event_writer import write_event

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

ITS_API_KEY = os.getenv('ITS_API_KEY', '')
ITS_API_URL = os.getenv('ITS_CCTV_API_URL', '')

R2_ACCOUNT_ID = os.getenv('CF_ACCOUNT_ID', '')
R2_ACCESS_KEY = os.getenv('CF_ACCESS_KEY', '')
R2_SECRET_KEY = os.getenv('CF_SECRET_KEY', '')
R2_BUCKET_NAME = os.getenv('CF_BUCKET_NAME', '')

VLM_CONF_LOW = 0.6
VLM_CONF_HIGH = 0.8

# 선정된 5개 CCTV (전부 터널 CCTV로 구성)
# search_regions: 여러 후보 좌표를 순서대로 시도해 CCTV를 탐색
SELECTED_CCTV_LOCATIONS = [
    {
        'id': 'goduck_tunnel',
        'display_name': '[세종] 고덕터널',
        'location_name': '고덕터널',
        'search_names': ['고덕터널(세종 12)', '세종 12)'],
        'search_regions': [
            {'lat': 37.5472, 'lon': 127.1562, 'delta': 0.05},  # 세종포천선 고덕터널 확인된 좌표
        ],
    },
    {
        'id': 'dongtan_tunnel',
        'display_name': '[부산] 경부동탄터널',
        'location_name': '경부동탄터널',
        'search_names': ['경부동탄터널(부산3)', '부산3)'],
        'search_regions': [
            {'lat': 37.2001, 'lon': 127.0952, 'delta': 0.05},  # 경부선 동탄터널 확인된 좌표 (경기 화성)
        ],
    },
    {
        'id': 'pogok_tunnel',
        'display_name': '[세종] 포곡2터널',
        'location_name': '포곡2터널',
        'search_names': ['포곡2터널(포천 2)', '포천 2)'],
        'search_regions': [
            {'lat': 37.2816, 'lon': 127.2507, 'delta': 0.05},  # 세종포천선 포곡2터널 확인된 좌표
        ],
    },
    {
        'id': 'gwanggyo_tunnel',
        'display_name': '[영동선] 광교터널',
        'location_name': '광교터널',
        'search_names': ['[인천1]광교터널(인천1 1 고정)', '인천1 1 고정'],
        'search_regions': [
            {'lat': 37.306575, 'lon': 127.037347, 'delta': 0.05},  # 영동선 광교터널 확인된 좌표
        ],
    },
    {
        'id': 'maseong_tunnel',
        'display_name': '[영동선] 마성터널(인천)',
        'location_name': '마성터널',
        'search_names': ['[인천1]마성터널(인천1 4)', '인천1 4'],
        'search_regions': [
            {'lat': 37.285983, 'lon': 127.165356, 'delta': 0.05},  # 영동선 마성터널 확인된 좌표
        ],
    },
]


class CCTVCapturePipeline:
    """5개 CCTV 병렬 캡처 → Cloudflare R2 업로드 파이프라인 (24시간 운용)"""

    CYCLE_INTERVAL = 60    # 1분 사이클
    CAPTURE_TIMEOUT = 25   # CCTV당 ffmpeg 타임아웃 (초)
    CLEANUP_INTERVAL = 1440  # 매 1440사이클(24시간)마다 만료 이미지 정리
    RETENTION_DAYS = 1     # R2 이미지 보관 기간

    def __init__(self, api_key=None, duration_hours=24):
        self.api_key = api_key or ITS_API_KEY
        self.duration_hours = duration_hours
        self.duration_seconds = duration_hours * 3600
        self.start_time = time.time()
        self.is_running = True
        self.total_captures = 0
        self._lock = threading.Lock()

        self.s3 = self._init_r2()

    def _init_r2(self):
        if not (R2_ACCOUNT_ID and R2_ACCESS_KEY and R2_SECRET_KEY):
            logger.warning("⚠ R2 자격증명 없음 → 로컬(captures/) 저장")
            return None
        try:
            client = boto3.client(
                's3',
                endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
                aws_access_key_id=R2_ACCESS_KEY,
                aws_secret_access_key=R2_SECRET_KEY,
                config=Config(signature_version='s3v4', retries={'max_attempts': 3}),
                region_name='auto',
            )
            logger.info(f"✓ R2 클라이언트 초기화 완료 (버킷: {R2_BUCKET_NAME})")
            return client
        except Exception as e:
            logger.error(f"❌ R2 초기화 실패: {e}")
            return None

    # ── ITS API ──────────────────────────────────────────────────────────────

    def fetch_cctv_url(self, location):
        """여러 후보 좌표 범위를 순회하며 ITS API로 CCTV URL 탐색"""
        for region in location['search_regions']:
            try:
                params = {
                    'apiKey': self.api_key,
                    'type': 'all',
                    'cctvType': '1',
                    'minX': region['lon'] - region['delta'],
                    'maxX': region['lon'] + region['delta'],
                    'minY': region['lat'] - region['delta'],
                    'maxY': region['lat'] + region['delta'],
                    'getType': 'json',
                }
                resp = requests.get(ITS_API_URL, params=params, timeout=10, verify=False)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                if 'response' in data:
                    data = data['response']

                cctv_list = data.get('data', []) if isinstance(data, dict) else []
                if not isinstance(cctv_list, list) or not cctv_list:
                    continue

                for keyword in location['search_names']:
                    kw_lower = keyword.lower()
                    for cctv in cctv_list:
                        name = cctv.get('cctvname', '')
                        if kw_lower in name.lower():
                            url = cctv.get('cctvurl', '')
                            if url:
                                logger.info(f"  ✓ [{location['display_name']}] 매칭: {name}")
                                return url, name

            except requests.RequestException as e:
                logger.debug(f"  API 요청 실패 ({location['display_name']}): {e}")
                continue

        logger.warning(f"  ⚠ [{location['display_name']}] CCTV URL 탐색 실패")
        return None, None

    # ── 프레임 캡처 ──────────────────────────────────────────────────────────

    def capture_frame(self, stream_url, display_name):
        """ffmpeg으로 HLS 스트림에서 1프레임 캡처 (OpenCV fallback 포함)"""
        img = self._capture_ffmpeg(stream_url, display_name)
        if img is None:
            img = self._capture_opencv(stream_url, display_name)
        return img

    def _capture_ffmpeg(self, url, display_name):
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(suffix='.jpg')
            os.close(fd)

            cmd = [
                'ffmpeg', '-y',
                '-loglevel', 'error',
                '-timeout', '10000000',   # 연결 타임아웃 10초 (마이크로초 단위)
                '-i', url,
                '-vframes', '1',
                '-q:v', '3',
                tmp_path,
            ]
            result = subprocess.run(cmd, capture_output=True, timeout=self.CAPTURE_TIMEOUT)

            if result.returncode == 0 and os.path.exists(tmp_path):
                size = os.path.getsize(tmp_path)
                if size > 500:
                    with open(tmp_path, 'rb') as f:
                        data = f.read()
                    logger.info(f"  ✓ [{display_name}] ffmpeg 캡처 ({size // 1024} KB)")
                    return data

            if result.stderr:
                logger.debug(f"  ffmpeg: {result.stderr.decode(errors='ignore')[:200]}")

        except subprocess.TimeoutExpired:
            logger.warning(f"  ⚠ [{display_name}] ffmpeg 타임아웃")
        except FileNotFoundError:
            logger.warning("  ⚠ ffmpeg 없음 → OpenCV fallback 시도")
        except Exception as e:
            logger.error(f"  ❌ [{display_name}] ffmpeg 오류: {e}")
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
        return None

    def _capture_opencv(self, url, display_name):
        try:
            import cv2
            cap = cv2.VideoCapture(url)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            deadline = time.time() + self.CAPTURE_TIMEOUT
            while time.time() < deadline:
                ret, frame = cap.read()
                if ret and frame is not None:
                    cap.release()
                    ok, buf = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 90])
                    if ok:
                        logger.info(f"  ✓ [{display_name}] OpenCV 캡처")
                        return buf.tobytes()
                time.sleep(0.3)
            cap.release()
        except Exception as e:
            logger.error(f"  ❌ [{display_name}] OpenCV 오류: {e}")
        return None

    # ── R2 / 로컬 저장 ───────────────────────────────────────────────────────

    def upload_image(self, img_bytes, location, key: str = None):
        """R2에 업로드; R2 미설정 시 로컬 저장. key를 지정하지 않으면 raw/로 자동 생성."""
        display_name = location['display_name']

        if key is None:
            kst = timezone(timedelta(hours=9))
            kst_now = datetime.now(timezone.utc).astimezone(kst)
            timestamp = kst_now.strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{location['location_name']}.jpg"
            key = f"raw/{filename}"

        if self.s3:
            try:
                self.s3.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=key,
                    Body=img_bytes,
                    ContentType='image/jpeg',
                )
                logger.info(f"  ✓ [{display_name}] R2 업로드: {key}")
                return True
            except Exception as e:
                logger.error(f"  ❌ [{display_name}] R2 업로드 실패: {e}")

        # 로컬 fallback (prefix/filename 구조 유지)
        filename = key.split('/')[-1]
        prefix = key.split('/')[0]
        local_dir = os.path.join(os.path.dirname(__file__), prefix)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, filename)
        with open(local_path, 'wb') as f:
            f.write(img_bytes)
        logger.info(f"  ✓ [{display_name}] 로컬 저장: {local_path}")
        return True

    # ── R2 만료 이미지 정리 ──────────────────────────────────────────────────

    def cleanup_expired_images(self):
        """RETENTION_DAYS 이상 된 R2 이미지 일괄 삭제 (로컬도 동일 처리)"""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.RETENTION_DAYS)

        if self.s3:
            self._cleanup_r2(cutoff)
        else:
            self._cleanup_local(cutoff)

    def _cleanup_r2(self, cutoff):
        to_delete = []
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=R2_BUCKET_NAME, Prefix='raw/'):
                for obj in page.get('Contents', []):
                    if obj['LastModified'] < cutoff:
                        to_delete.append({'Key': obj['Key']})

            if not to_delete:
                logger.info("🧹 만료 이미지 없음")
                return

            # delete_objects는 한 번에 최대 1000개
            for i in range(0, len(to_delete), 1000):
                batch = to_delete[i:i + 1000]
                self.s3.delete_objects(
                    Bucket=R2_BUCKET_NAME,
                    Delete={'Objects': batch, 'Quiet': True},
                )
            logger.info(f"🧹 R2 만료 이미지 {len(to_delete)}개 삭제 완료 (기준: {cutoff.strftime('%Y-%m-%d')})")
        except Exception as e:
            logger.error(f"❌ R2 정리 실패: {e}")

    def _cleanup_local(self, cutoff):
        raw_dir = os.path.join(os.path.dirname(__file__), 'raw')
        if not os.path.exists(raw_dir):
            return
        deleted = 0
        cutoff_ts = cutoff.timestamp()
        for root, _, files in os.walk(raw_dir):
            for fname in files:
                fpath = os.path.join(root, fname)
                if os.path.getmtime(fpath) < cutoff_ts:
                    os.unlink(fpath)
                    deleted += 1
        if deleted:
            logger.info(f"🧹 로컬 만료 이미지 {deleted}개 삭제 완료")

    # ── fire/smoke 이벤트 처리 (write → VLM → patch) ─────────────────────────

    def _handle_fire_event(self, location: dict, vision_result: dict, snapshot_key: str):
        """이벤트 저장 (VLM 결과는 vision_result["vlm"]에 이미 포함)."""
        write_event(location, vision_result, snapshot_key)

    # ── 1개 CCTV 처리 ────────────────────────────────────────────────────────

    def process_location(self, location):
        """URL 갱신 → 캡처 → Vision AI 탐지 → 이벤트 기록 → R2 업로드"""
        name = location['display_name']
        logger.info(f"▶ {name} 처리 시작")

        url, _ = self.fetch_cctv_url(location)
        if not url:
            return False

        img = self.capture_frame(url, name)
        if not img:
            return False

        # snapshot_key를 스레드 분기 전에 미리 계산 (DB + R2 양쪽에 동일 키 사용)
        kst = timezone(timedelta(hours=9))
        timestamp = datetime.now(timezone.utc).astimezone(kst).strftime('%Y%m%d_%H%M%S')
        filename = f"{timestamp}_{location['location_name']}.jpg"
        snapshot_key = f"raw/{filename}"

        # R2 raw/ 업로드는 탐지 여부와 무관하게 항상 수행
        threading.Thread(
            target=self.upload_image,
            args=(img, location, snapshot_key),
            daemon=True,
        ).start()

        # 1차 탐지: Vision API (RT-DETRv2)
        vision_result = call_vision_api(img, name)

        if vision_result:
            vlm_candidates = [
                d for d in vision_result.get("detections", [])
                if VLM_CONF_LOW <= d["confidence"] <= VLM_CONF_HIGH
            ]

            if vlm_candidates:
                vlm_results = vision_result.get("vlm") or []

                # carlight 오탐아님만 있는 경우 저장 생략 (아무 의미 없는 탐지)
                should_save = any(
                    not (r["class_name"] == "carlight" and not r["is_false_positive"])
                    for r in vlm_results
                ) if vlm_results else True

                if should_save:
                    # R2 detection/ 업로드 — bbox 그린 이미지 사용 (없으면 raw fallback)
                    detection_key = f"detection/{filename}"
                    annotated_b64 = vision_result.get("annotated_image_b64")
                    detection_img = base64.b64decode(annotated_b64) if annotated_b64 else img
                    threading.Thread(
                        target=self.upload_image,
                        args=(detection_img, location, detection_key),
                        daemon=True,
                    ).start()

                    # 이벤트 저장 — snapshot_key는 bbox 이미지(detection/)를 가리켜야 함
                    threading.Thread(
                        target=self._handle_fire_event,
                        args=(location, vision_result, detection_key),
                        daemon=True,
                    ).start()
                else:
                    logger.info(f"  ⏭ [{name}] carlight 오탐아님 → 저장 생략")

        with self._lock:
            self.total_captures += 1

        return True

    # ── 1 사이클 ─────────────────────────────────────────────────────────────

    def run_cycle(self, cycle_no):
        """5개 CCTV 병렬 처리 (3분 이내 완료 목표)"""
        logger.info("=" * 65)
        logger.info(f"🔄 사이클 #{cycle_no} — 5개 CCTV 병렬 처리 시작")
        logger.info("=" * 65)

        t0 = time.time()
        results = {}

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                pool.submit(self.process_location, loc): loc
                for loc in SELECTED_CCTV_LOCATIONS
            }
            for future in as_completed(futures):
                loc = futures[future]
                try:
                    results[loc['display_name']] = future.result()
                except Exception as e:
                    logger.error(f"❌ {loc['display_name']} 예외: {e}")
                    results[loc['display_name']] = False

        elapsed = time.time() - t0
        ok_count = sum(results.values())
        logger.info(f"📊 사이클 #{cycle_no} 완료: {ok_count}/5 성공 | {elapsed:.1f}초 소요 | 누적 {self.total_captures}개")
        return elapsed

    # ── 메인 루프 ────────────────────────────────────────────────────────────

    def start(self):
        logger.info("=" * 65)
        logger.info(f"🚀 {self.duration_hours}시간 CCTV 모니터링 시작")
        logger.info(f"   사이클 간격 : {self.CYCLE_INTERVAL}초 (1분)")
        logger.info(f"   캡처 타임아웃: {self.CAPTURE_TIMEOUT}초/CCTV")
        logger.info(f"   저장 방식   : {'Cloudflare R2' if self.s3 else '로컬(captures/)'}")
        logger.info(f"   이미지 보관 : {self.RETENTION_DAYS}일 (매 {self.CLEANUP_INTERVAL}사이클마다 정리)")
        logger.info("=" * 65)

        cycle_no = 0
        self.cleanup_expired_images()  # 시작 시 1회 정리

        try:
            while self.is_running:
                elapsed_total = time.time() - self.start_time
                # if elapsed_total >= self.duration_seconds:
                #     logger.info(f"✅ {self.duration_hours}시간 경과 → 종료")
                #     break

                remaining_min = int((self.duration_seconds - elapsed_total) / 60)
                cycle_no += 1
                logger.info(f"\n⏱️  잔여 {remaining_min}분 ({elapsed_total / 60:.1f}분 경과)")

                cycle_elapsed = self.run_cycle(cycle_no)

                # 매 CLEANUP_INTERVAL 사이클마다 만료 이미지 정리
                if cycle_no % self.CLEANUP_INTERVAL == 0:
                    threading.Thread(target=self.cleanup_expired_images, daemon=True).start()

                wait = max(0.0, self.CYCLE_INTERVAL - cycle_elapsed)
                if wait > 0 and self.is_running:
                    logger.info(f"⏳ 다음 사이클까지 {wait:.0f}초 대기 중...")
                    time.sleep(wait)

        except KeyboardInterrupt:
            logger.info("\n👤 사용자 중단")
        except Exception as e:
            logger.error(f"❌ 메인 루프 예외: {e}", exc_info=True)

        logger.info("=" * 65)
        logger.info(f"✅ 종료 — 총 {self.total_captures}개 캡처 / {cycle_no}사이클")
        logger.info("=" * 65)


def main():
    import sys
    api_key = ITS_API_KEY
    duration = 5.0

    if len(sys.argv) > 1:
        api_key = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            duration = float(sys.argv[2])
        except ValueError:
            logger.warning("시간 인자 오류 → 기본값 5시간 사용")

    CCTVCapturePipeline(api_key=api_key, duration_hours=duration).start()


if __name__ == '__main__':
    main()