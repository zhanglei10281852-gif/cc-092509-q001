from __future__ import annotations

import sqlite3
import threading

import pytest

from app.database import database_path


BATCH_PAYLOAD = {"intake_code": "BATCH-RETRY-001", "project_code": "P-RETRY", "expected_count": 3}


def raw_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(database_path(), timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def fetch_batch(intake_code: str) -> dict:
    with raw_connection() as connection:
        row = connection.execute(
            "SELECT * FROM intake_batches WHERE intake_code=?", (intake_code,)
        ).fetchone()
    assert row is not None
    return dict(row)


def count_batches(intake_code: str) -> int:
    with raw_connection() as connection:
        return connection.execute(
            "SELECT COUNT(*) FROM intake_batches WHERE intake_code=?", (intake_code,)
        ).fetchone()[0]


def audit_events(client, admin, action: str) -> list[dict]:
    response = client.get(
        "/api/audit",
        headers=admin["headers"],
        params={"resource_type": "receipt_batch", "action": action, "size": 100},
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]


def test_safe_retry_returns_original_batch_with_replay_flag(client, admin):
    first = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert first.status_code == 201, first.text
    assert first.json()["replayed"] is False

    second = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert second.status_code == 200, second.text
    body = second.json()
    assert body["replayed"] is True
    # 返回最初的批次
    assert body["id"] == first.json()["id"]
    assert body["intake_code"] == BATCH_PAYLOAD["intake_code"]
    assert body["project_code"] == BATCH_PAYLOAD["project_code"]
    assert body["expected_count"] == BATCH_PAYLOAD["expected_count"]
    assert body["qr_payload"] == first.json()["qr_payload"]
    # 不新增批次
    assert count_batches(BATCH_PAYLOAD["intake_code"]) == 1
    # 原始批次只有一条创建审计，重放只留下重放审计
    assert len(audit_events(client, admin, "batch.receive")) == 1
    replays = audit_events(client, admin, "batch.receive.replay")
    assert len(replays) == 1
    assert replays[0]["resource_id"] == str(first.json()["id"])


def test_same_code_with_different_business_fields_is_stable_conflict(client, admin):
    first = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert first.status_code == 201, first.text
    original = first.json()

    conflicting = {**BATCH_PAYLOAD, "expected_count": 9}
    response = client.post("/api/dossiers/batches", headers=admin["headers"], json=conflicting)
    assert response.status_code == 409, response.text
    error = response.json()["error"]
    assert error["code"] == "conflict"
    context = error["context"]
    assert context["existing_batch_id"] == original["id"]
    assert context["conflict_fields"] == ["expected_count"]
    assert context["submitted"] == {"expected_count": 9}
    assert context["existing"] == {"expected_count": 3}

    # 冲突必须稳定：再来一次得到同样的 409 与同样的冲突字段
    again = client.post("/api/dossiers/batches", headers=admin["headers"], json=conflicting)
    assert again.status_code == 409
    assert again.json()["error"]["context"]["conflict_fields"] == ["expected_count"]

    # project_code 不同同样冲突
    other_project = client.post(
        "/api/dossiers/batches",
        headers=admin["headers"],
        json={**BATCH_PAYLOAD, "project_code": "P-OTHER"},
    )
    assert other_project.status_code == 409
    assert other_project.json()["error"]["context"]["conflict_fields"] == ["project_code"]

    # 冲突不允许新增批次，也不改变既有批次
    assert count_batches(BATCH_PAYLOAD["intake_code"]) == 1
    stored = fetch_batch(BATCH_PAYLOAD["intake_code"])
    assert stored["project_code"] == "P-RETRY"
    assert stored["expected_count"] == 3
    assert stored["accepted_count"] == 0
    # 每次冲突都可在审计中确认拒绝结果
    conflicts = audit_events(client, admin, "batch.receive.conflict")
    assert len(conflicts) == 3
    assert all(event["outcome"] == "denied" for event in conflicts)


def test_concurrent_identical_retries_create_single_batch(client, admin):
    results: list[tuple[int, dict]] = []
    barrier = threading.Barrier(8)

    def submit() -> None:
        barrier.wait()
        response = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
        results.append((response.status_code, response.json()))

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    status_codes = sorted(code for code, _ in results)
    assert status_codes == sorted([201] + [200] * 7), results
    batch_ids = {body["id"] for _, body in results}
    assert batch_ids == {results[0][1]["id"]}
    assert sum(1 for code, _ in results if code == 201) == 1
    assert all(body["replayed"] is True for code, body in results if code == 200)
    assert count_batches(BATCH_PAYLOAD["intake_code"]) == 1
    stored = fetch_batch(BATCH_PAYLOAD["intake_code"])
    assert stored["accepted_count"] == 0
    assert len(audit_events(client, admin, "batch.receive")) == 1


def test_concurrent_conflicting_retries_never_create_second_batch(client, admin):
    results: list[tuple[int, dict]] = []
    barrier = threading.Barrier(8)

    def submit(expected_count: int) -> None:
        barrier.wait()
        response = client.post(
            "/api/dossiers/batches",
            headers=admin["headers"],
            json={**BATCH_PAYLOAD, "intake_code": "BATCH-RACE-002", "expected_count": expected_count},
        )
        results.append((response.status_code, response.json()))

    # 每个线程携带不同的预期数量：除唯一的创建者外，其余必然是业务冲突
    threads = [threading.Thread(target=submit, args=(3 + i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    created = [item for item in results if item[0] == 201]
    conflicts = [item for item in results if item[0] == 409]
    assert len(created) == 1, results
    assert len(conflicts) == 7, results
    for _, body in conflicts:
        assert body["error"]["context"]["conflict_fields"] == ["expected_count"]
    assert count_batches("BATCH-RACE-002") == 1


def test_retry_after_service_restart_still_replays(client, admin, monkeypatch):
    first = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert first.status_code == 201, first.text
    original = fetch_batch(BATCH_PAYLOAD["intake_code"])

    # 模拟服务重启：丢弃全部线程内连接后重新初始化应用
    from app.database import close_connection
    from app.main import app
    from fastapi.testclient import TestClient

    close_connection()
    with TestClient(app) as restarted:
        response = restarted.post(
            "/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD
        )
    assert response.status_code == 200, response.text
    assert response.json()["replayed"] is True
    assert response.json()["id"] == original["id"]
    assert response.json()["qr_payload"] == original["qr_payload"]
    assert count_batches(BATCH_PAYLOAD["intake_code"]) == 1


def test_failed_first_attempt_rolls_back_and_retry_succeeds(client, admin, monkeypatch):
    from app.archives import repository as repository_module

    def broken_get(self, intake_id):
        raise RuntimeError("模拟批次写入后读取失败")

    monkeypatch.setattr(repository_module.IntakeRepository, "get", broken_get)
    with pytest.raises(Exception):
        client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    monkeypatch.undo()

    # 回滚后库里不留批次，也不留创建审计；重试可以正常建立
    assert count_batches(BATCH_PAYLOAD["intake_code"]) == 0
    retry = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert retry.status_code == 201, retry.text
    assert retry.json()["replayed"] is False
    assert count_batches(BATCH_PAYLOAD["intake_code"]) == 1
    assert len(audit_events(client, admin, "batch.receive")) == 1


def test_replay_preserves_counts_qr_and_downstream_flows(client, admin):
    batch = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert batch.status_code == 201, batch.text
    batch_id = batch.json()["id"]

    vault = client.post(
        "/api/dossiers/vaults",
        headers=admin["headers"],
        json={
            "code": "BR-01",
            "building": "档案楼",
            "room": "常温库",
            "cabinet": "一号柜",
            "shelf": "一层",
            "sensitivity": "normal",
            "capacity_units": 50,
        },
    )
    assert vault.status_code == 201, vault.text
    dossier = client.post(
        "/api/dossiers",
        headers=admin["headers"],
        json={
            "dossier_code": "BR-DOSSIER-01",
            "intake_id": batch_id,
            "asset_type": "工艺文档",
            "quantity": 1,
            "unit": "份",
            "vault_id": vault.json()["id"],
        },
    )
    assert dossier.status_code == 201, dossier.text

    before = fetch_batch(BATCH_PAYLOAD["intake_code"])
    assert before["accepted_count"] == 1

    # 档案登记之后重放批次：计数、状态、二维码均不变
    replay = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert replay.status_code == 200, replay.text
    assert replay.json()["accepted_count"] == 1
    after = fetch_batch(BATCH_PAYLOAD["intake_code"])
    assert after["accepted_count"] == before["accepted_count"] == 1
    assert after["status"] == "open"
    assert after["qr_payload"] == before["qr_payload"]

    # 后续批次核对流程继续成立
    reconciliation = client.get(
        f"/api/dossier-operations/batches/{batch_id}/reconciliation",
        headers=admin["headers"],
    )
    assert reconciliation.status_code == 200, reconciliation.text
    detail = reconciliation.json()
    assert detail["dossier_count"] == 1
    assert [item["id"] for item in detail["dossiers"]] == [dossier.json()["id"]]

    open_batches = client.get(
        "/api/dossier-operations/batches/open", headers=admin["headers"]
    )
    assert open_batches.status_code == 200
    assert batch_id in {item["id"] for item in open_batches.json()}


def test_batch_creation_requires_permission(client, admin):
    researcher = client.post(
        "/api/users",
        headers=admin["headers"],
        json={
            "username": "researcher01",
            "password": "Researcher!1",
            "display_name": "研究人员甲",
            "role_codes": ["researcher"],
        },
    )
    assert researcher.status_code == 201, researcher.text
    login = client.post(
        "/api/auth/login",
        json={"username": "researcher01", "password": "Researcher!1", "client_label": "tests"},
    )
    assert login.status_code == 200, login.text
    headers = {"Authorization": f"Bearer {login.json()['token']}"}

    denied = client.post("/api/dossiers/batches", headers=headers, json=BATCH_PAYLOAD)
    assert denied.status_code == 403, denied.text
    assert count_batches(BATCH_PAYLOAD["intake_code"]) == 0
