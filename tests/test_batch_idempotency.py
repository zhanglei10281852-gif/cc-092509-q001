from __future__ import annotations

import threading

BATCH_PAYLOAD = {"intake_code": "BATCH-IDEM-001", "project_code": "P-IDEM", "expected_count": 3}


def _audit_batch_events(client, admin):
    response = client.get(
        "/api/audit?resource_type=receipt_batch&size=100",
        headers=admin["headers"],
    )
    assert response.status_code == 200, response.text
    return response.json()["data"]


def test_identical_retry_replays_original_batch(client, admin):
    first = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert first.status_code == 201, first.text
    assert first.json()["replayed"] is False

    second = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert second.status_code == 201, second.text

    first_body, second_body = first.json(), second.json()
    assert second_body["replayed"] is True
    assert second_body["id"] == first_body["id"]
    assert second_body["qr_payload"] == first_body["qr_payload"]
    assert second_body["accepted_count"] == 0

    # 只有一个批次、一条接收审计
    open_batches = client.get("/api/dossier-operations/batches/open", headers=admin["headers"])
    matching = [b for b in open_batches.json() if b["intake_code"] == BATCH_PAYLOAD["intake_code"]]
    assert len(matching) == 1
    events = [e for e in _audit_batch_events(client, admin) if e["resource_id"] == str(first_body["id"])]
    assert len(events) == 1
    assert events[0]["action"] == "batch.receive"


def test_retry_after_restart_still_replays(client, admin):
    first = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert first.status_code == 201, first.text

    # 服务重启后（新数据库连接）重试仍安全返回同一批次
    from app.database import close_connection

    close_connection()
    retry = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert retry.status_code == 201, retry.text
    assert retry.json()["id"] == first.json()["id"]
    assert retry.json()["replayed"] is True


def test_same_code_with_different_project_is_business_conflict(client, admin):
    first = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert first.status_code == 201, first.text

    conflict = client.post(
        "/api/dossiers/batches",
        headers=admin["headers"],
        json={**BATCH_PAYLOAD, "project_code": "P-OTHER"},
    )
    assert conflict.status_code == 409, conflict.text
    body = conflict.json()["error"]
    assert body["context"]["conflict_fields"] == ["project_code"]
    assert body["context"]["existing_batch_id"] == first.json()["id"]


def test_same_code_with_different_expected_count_is_business_conflict(client, admin):
    assert client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD).status_code == 201

    conflict = client.post(
        "/api/dossiers/batches",
        headers=admin["headers"],
        json={**BATCH_PAYLOAD, "expected_count": 99},
    )
    assert conflict.status_code == 409, conflict.text
    assert conflict.json()["error"]["context"]["conflict_fields"] == ["expected_count"]


def test_concurrent_identical_submissions_create_single_batch(client, admin):
    results: list = []
    barrier = threading.Barrier(4)

    def submit() -> None:
        barrier.wait()
        response = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
        results.append((response.status_code, response.json()))

    threads = [threading.Thread(target=submit) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(results) == 4
    assert all(status == 201 for status, _ in results)
    batch_ids = {body["id"] for _, body in results}
    assert batch_ids == {results[0][1]["id"]}
    assert sum(1 for _, body in results if body["replayed"] is False) == 1
    assert sum(1 for _, body in results if body["replayed"] is True) == 3

    open_batches = client.get("/api/dossier-operations/batches/open", headers=admin["headers"])
    assert len([b for b in open_batches.json() if b["intake_code"] == BATCH_PAYLOAD["intake_code"]]) == 1


def test_failed_dossier_registration_keeps_batch_count_intact(client, admin):
    batch = client.post("/api/dossiers/batches", headers=admin["headers"], json=BATCH_PAYLOAD)
    assert batch.status_code == 201, batch.text
    batch_id = batch.json()["id"]

    dossier_payload = {
        "dossier_code": "BIDEM-D-01",
        "intake_id": batch_id,
        "asset_type": "土壤",
        "quantity": 1,
        "unit": "份",
    }
    first = client.post("/api/dossiers", headers=admin["headers"], json=dossier_payload)
    assert first.status_code == 201, first.text

    duplicate = client.post("/api/dossiers", headers=admin["headers"], json=dossier_payload)
    assert duplicate.status_code == 409, duplicate.text

    reconciliation = client.get(
        f"/api/dossier-operations/batches/{batch_id}/reconciliation",
        headers=admin["headers"],
    )
    assert reconciliation.status_code == 200, reconciliation.text
    assert reconciliation.json()["dossier_count"] == 1
    assert reconciliation.json()["batch"]["accepted_count"] == 1
