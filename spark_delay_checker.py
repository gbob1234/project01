#!/usr/bin/env python3
# spark_delay_checker.py

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from kubernetes import client, config


def load_kube_config() -> None:
    """
    Pod 안에서 실행되면 in-cluster config 사용.
    로컬/베스천에서 실행되면 ~/.kube/config 사용.
    """
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()


def parse_spark_time(value: Optional[str]) -> Optional[datetime]:
    """
    Spark REST API의 launchTime 파싱.
    예: 2026-07-27T10:20:30.123GMT
    예: 2026-07-27T10:20:30.123Z
    """
    if not value:
        return None

    s = value.strip()

    if s.endswith("GMT"):
        s = s[:-3] + "+00:00"
    elif s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def get_driver_pod_info(
    namespace: str,
    driver_pod_name: str,
    spark_app_label_key: str,
) -> Dict[str, Any]:
    v1 = client.CoreV1Api()
    pod = v1.read_namespaced_pod(name=driver_pod_name, namespace=namespace)

    labels = pod.metadata.labels or {}

    return {
        "namespace": namespace,
        "pod_name": pod.metadata.name,
        "node_name": pod.spec.node_name,
        "node_ip": pod.status.host_ip,
        "pod_ip": pod.status.pod_ip,
        "spark_app_selector": labels.get(spark_app_label_key),
        "labels": labels,
    }


def http_get_json(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout_sec: int = 5,
) -> Any:
    resp = session.get(url, params=params, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.json()


def resolve_spark_app_id(
    session: requests.Session,
    api_base: str,
    spark_app_selector: Optional[str],
) -> Dict[str, Any]:
    """
    spark-app-selector 라벨값을 app_id 후보로 보되,
    실제 Spark REST API app-id는 /applications 응답에서 확인한다.

    running application이 1개면 그걸 사용.
    여러 개면 id/name에서 spark_app_selector와 매칭되는 것을 우선 사용.
    """
    apps_url = f"{api_base}/applications"

    apps = http_get_json(
        session,
        apps_url,
        params={"status": "running"},
    )

    if not apps:
        apps = http_get_json(session, apps_url)

    if not apps:
        raise RuntimeError(f"Spark REST API에서 applications 목록을 찾지 못했습니다: {apps_url}")

    if len(apps) == 1:
        app = apps[0]
        return {
            "app_id": app.get("id"),
            "app_name": app.get("name"),
            "match_type": "single_application",
            "raw": app,
        }

    if spark_app_selector:
        for app in apps:
            if app.get("id") == spark_app_selector or app.get("name") == spark_app_selector:
                return {
                    "app_id": app.get("id"),
                    "app_name": app.get("name"),
                    "match_type": "matched_by_spark_app_selector",
                    "raw": app,
                }

    # 그래도 못 찾으면 첫 번째 running app 사용
    app = apps[0]
    return {
        "app_id": app.get("id"),
        "app_name": app.get("name"),
        "match_type": "fallback_first_application",
        "raw": app,
    }


def get_active_stages(
    session: requests.Session,
    api_base: str,
    app_id: str,
) -> List[Dict[str, Any]]:
    app_id_path = quote(app_id, safe="/")
    url = f"{api_base}/applications/{app_id_path}/stages"

    return http_get_json(
        session,
        url,
        params={"status": "active"},
    )


def get_running_tasks_from_task_list(
    session: requests.Session,
    api_base: str,
    app_id: str,
    stage_id: int,
    attempt_id: int,
    max_tasks: int,
) -> List[Dict[str, Any]]:
    app_id_path = quote(app_id, safe="/")

    url = (
        f"{api_base}/applications/{app_id_path}"
        f"/stages/{stage_id}/{attempt_id}/taskList"
    )

    return http_get_json(
        session,
        url,
        params={
            "status": "running",
            "sortBy": "-runtime",
            "offset": 0,
            "length": max_tasks,
        },
    )


def collect_slow_tasks(
    session: requests.Session,
    api_base: str,
    app_id: str,
    threshold_sec: int,
    max_tasks_per_stage: int,
) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    slow_tasks: List[Dict[str, Any]] = []

    stages = get_active_stages(session, api_base, app_id)

    for stage in stages:
        stage_id = stage.get("stageId")
        attempt_id = stage.get("attemptId", 0)

        if stage_id is None:
            continue

        try:
            tasks = get_running_tasks_from_task_list(
                session=session,
                api_base=api_base,
                app_id=app_id,
                stage_id=int(stage_id),
                attempt_id=int(attempt_id),
                max_tasks=max_tasks_per_stage,
            )
        except requests.HTTPError:
            # 일부 Spark 버전/상태에서 taskList가 실패할 경우를 대비한 fallback
            tasks = []

        for task in tasks:
            launch_time = parse_spark_time(task.get("launchTime"))

            runtime_sec = None
            if launch_time:
                runtime_sec = int((now - launch_time).total_seconds())

            # Spark taskList의 duration은 보통 ms 단위.
            # RUNNING task에서 duration이 들어오는 환경이면 보조값으로 사용.
            duration_ms = task.get("duration")
            if runtime_sec is None and isinstance(duration_ms, (int, float)):
                runtime_sec = int(duration_ms / 1000)

            if runtime_sec is None:
                continue

            if runtime_sec >= threshold_sec:
                task_metrics = task.get("taskMetrics") or {}
                input_metrics = task_metrics.get("inputMetrics") or {}
                shuffle_read_metrics = task_metrics.get("shuffleReadMetrics") or {}

                slow_tasks.append(
                    {
                        "app_id": app_id,
                        "stage_id": stage_id,
                        "stage_attempt_id": attempt_id,
                        "task_id": task.get("taskId"),
                        "task_index": task.get("index"),
                        "task_attempt": task.get("attempt"),
                        "status": task.get("status"),
                        "executor_id": task.get("executorId"),
                        "host": task.get("host"),
                        "launch_time": task.get("launchTime"),
                        "runtime_sec": runtime_sec,
                        "runtime_min": round(runtime_sec / 60, 2),
                        "task_locality": task.get("taskLocality"),
                        "speculative": task.get("speculative"),
                        "executor_run_time_ms": task_metrics.get("executorRunTime"),
                        "jvm_gc_time_ms": task_metrics.get("jvmGCTime"),
                        "input_records_read": input_metrics.get("recordsRead"),
                        "input_bytes_read": input_metrics.get("bytesRead"),
                        "shuffle_records_read": shuffle_read_metrics.get("recordsRead"),
                        "shuffle_fetch_wait_time_ms": shuffle_read_metrics.get("fetchWaitTime"),
                    }
                )

    slow_tasks.sort(key=lambda x: x["runtime_sec"], reverse=True)
    return slow_tasks


def send_webhook(webhook_url: str, payload: Dict[str, Any]) -> None:
    resp = requests.post(webhook_url, json=payload, timeout=5)
    resp.raise_for_status()


def check_once(args: argparse.Namespace) -> int:
    load_kube_config()

    pod_info = get_driver_pod_info(
        namespace=args.namespace,
        driver_pod_name=args.driver_pod_name,
        spark_app_label_key=args.spark_app_label_key,
    )

    node_ip = pod_info["node_ip"]
    if not node_ip:
        raise RuntimeError("driver pod의 status.host_ip를 찾지 못했습니다.")

    api_base = f"http://{node_ip}:{args.spark_ui_node_port}/api/v1"

    session = requests.Session()

    app_info = resolve_spark_app_id(
        session=session,
        api_base=api_base,
        spark_app_selector=pod_info["spark_app_selector"],
    )

    app_id = app_info["app_id"]
    if not app_id:
        raise RuntimeError("Spark REST API에서 app_id를 확인하지 못했습니다.")

    slow_tasks = collect_slow_tasks(
        session=session,
        api_base=api_base,
        app_id=app_id,
        threshold_sec=args.threshold_sec,
        max_tasks_per_stage=args.max_tasks_per_stage,
    )

    result = {
        "checked_at": datetime.now().astimezone().isoformat(),
        "namespace": args.namespace,
        "driver_pod_name": args.driver_pod_name,
        "node_name": pod_info["node_name"],
        "node_ip": pod_info["node_ip"],
        "pod_ip": pod_info["pod_ip"],
        "spark_app_selector_label": pod_info["spark_app_selector"],
        "spark_rest_api_base": api_base,
        "resolved_app_id": app_id,
        "resolved_app_name": app_info.get("app_name"),
        "app_match_type": app_info.get("match_type"),
        "threshold_sec": args.threshold_sec,
        "slow_task_count": len(slow_tasks),
        "slow_tasks": slow_tasks[: args.print_limit],
    }

    print(json.dumps(result, ensure_ascii=False, indent=2))

    if slow_tasks and args.webhook_url:
        send_webhook(args.webhook_url, result)

    return 2 if slow_tasks else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spark driver pod의 NodePort Spark REST API를 조회해서 장기 실행 RUNNING task를 감지합니다."
    )

    parser.add_argument("--namespace", required=True)
    parser.add_argument("--driver-pod-name", required=True)
    parser.add_argument("--spark-ui-node-port", required=True, type=int)

    parser.add_argument(
        "--spark-app-label-key",
        default="spark-app-selector",
        help="driver pod label에서 Spark app selector 값을 읽을 key",
    )
    parser.add_argument(
        "--threshold-sec",
        type=int,
        default=1800,
        help="이 시간 이상 RUNNING 중인 task를 지연으로 판단. 기본 1800초 = 30분",
    )
    parser.add_argument(
        "--interval-sec",
        type=int,
        default=60,
        help="반복 체크 주기",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="1회만 체크하고 종료",
    )
    parser.add_argument(
        "--max-tasks-per-stage",
        type=int,
        default=200,
        help="stage별 조회할 running task 최대 개수",
    )
    parser.add_argument(
        "--print-limit",
        type=int,
        default=20,
        help="출력할 slow task 최대 개수",
    )
    parser.add_argument(
        "--webhook-url",
        default=os.getenv("ALERT_WEBHOOK_URL"),
        help="알림을 보낼 webhook URL. 미지정 시 stdout 출력만 수행",
    )

    args = parser.parse_args()

    if args.once:
        code = check_once(args)
        sys.exit(code)

    while True:
        try:
            check_once(args)
        except Exception as e:
            err = {
                "checked_at": datetime.now().astimezone().isoformat(),
                "error": str(e),
            }
            print(json.dumps(err, ensure_ascii=False, indent=2), file=sys.stderr)

        time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()
