import os
from urllib.parse import quote

import requests


# 로컬 port-forward 기준. 클러스터 내부에서는 Service 주소로 변경하세요.
BASE_URL = os.getenv("MONITOR_API_URL", "http://127.0.0.1:8000").rstrip("/")
API_TOKEN = os.environ["MONITOR_API_TOKEN"]


def _request(method, path, *, authenticated=True, **kwargs):
    """입력: HTTP 메서드, 경로, 요청 옵션. 반환: JSON 응답. 실패 시 예외 발생."""
    headers = {"Authorization": f"Bearer {API_TOKEN}"} if authenticated else {}

    response = requests.request(
        method,
        f"{BASE_URL}{path}",
        headers=headers,
        timeout=(5, 60),  # 연결 제한 5초, 응답 대기 60초
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def _device_path(device_id):
    """입력: 장비 ID. 반환: URL 인코딩한 장비 API 경로."""
    return f"/api/v1/devices/{quote(str(device_id), safe='')}"


def get_devices(limit=100, offset=0, include_deleted=False):
    """입력: 조회 개수(1~500), 시작 위치, 삭제 포함 여부. 반환: 목록과 페이지 정보."""
    return _request(
        "GET",
        "/api/v1/devices",
        params={
            "limit": limit,
            "offset": offset,
            "include_deleted": str(include_deleted).lower(),
        },
    )


def get_device(device_id):
    """입력: 장비 ID. 반환: 해당 장비의 수신 상태와 관리 설정."""
    return _request("GET", _device_path(device_id))


def set_monitoring(device_id, enabled, reason=""):
    """입력: 장비 ID, 감시 활성화 여부(bool), 사유. 반환: 변경된 장비 정보."""
    if not isinstance(enabled, bool):
        raise TypeError("enabled에는 True 또는 False를 입력하세요.")

    return _request(
        "PATCH",
        f"{_device_path(device_id)}/monitoring",
        json={"enabled": enabled, "reason": reason},
    )


def delete_device(device_id, reason="관리자 삭제"):
    """입력: 장비 ID와 사유. 반환: 논리 삭제된 장비 정보. 자동 재등록을 차단한다."""
    return _request(
        "DELETE",
        _device_path(device_id),
        params={"reason": reason},
    )


def restore_device(device_id):
    """입력: 삭제된 장비 ID. 반환: 복원된 장비 정보. 감시는 제외 상태로 유지된다."""
    return _request("POST", f"{_device_path(device_id)}/restore")


def check_liveness():
    """입력: 없음. 반환: API 프로세스 생존 상태. 인증은 필요하지 않다."""
    return _request("GET", "/health/live", authenticated=False)


def check_readiness():
    """입력: 없음. 반환: API의 DB 연결 준비 상태. 인증은 필요하지 않다."""
    return _request("GET", "/health/ready", authenticated=False)


if __name__ == "__main__":
    # 기본 실행은 조회만 수행합니다.
    try:
        print("API 생존:", check_liveness())
        print("DB 연결:", check_readiness())

        for device in get_devices(limit=500)["items"]:
            print(
                f"장비: {device['deviceId']}, "
                f"수신: {device['receptionStatus']}, "
                f"감시: {device['enabled']}, "
                f"삭제: {device['deleted']}"
            )

        # 아래 변경 작업은 필요한 경우에만 실행하세요.
        # print(get_device("EQP-001"))
        # print(set_monitoring("EQP-001", False, "계획 작업"))
        # print(set_monitoring("EQP-001", True, "작업 완료"))
        # print(delete_device("EQP-001", "수집기 운영 종료"))
        # print(restore_device("EQP-001"))
        # print(set_monitoring("EQP-001", True, "복원 후 감시 재개"))

    except requests.HTTPError as exc:
        print(f"API 오류: HTTP {exc.response.status_code}")
        print(exc.response.text)
    except requests.RequestException as exc:
        print(f"API 통신 실패: {exc}")
