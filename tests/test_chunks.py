"""分片续传端到端：身份核对、缺口隔离、来源错配、原子切换、幂等与版本守卫。

这些用例直接用 TestClient 打 HTTP，模拟「网络中断后续传 / 两人并发续传 /
旧版本补传」等值班场景。
"""
from __future__ import annotations

import hashlib
import json

import pytest

from app import settings
from app.main import app as fastapi_app
from fastapi.testclient import TestClient


def _make_manifest(raw: bytes, *, filename: str, version: str, segment_size: int,
                   amounts: list[dict], total_rows: int) -> dict:
    total_size = len(raw)
    total_segments = (total_size + segment_size - 1) // segment_size
    return {
        "filename": filename,
        "version": version,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "total_size": total_size,
        "segment_size": segment_size,
        "total_segments": total_segments,
        "total_rows": total_rows,
        "amounts": amounts,
    }


def _amounts_from_csv(raw: bytes) -> tuple[list[dict], int]:
    """从样例 CSV 直接算出「正确」的金额清单与行数。"""
    text = raw.decode("utf-8-sig")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    header = [c.strip() for c in lines[0].split(",")]
    idx = {name: header.index(name) for name in header}
    buckets: dict[str, dict] = {}
    rows = 0
    for line in lines[1:]:
        cells = next(__import__("csv").reader([line]))
        rows += 1
        site = cells[idx["site"]].strip()
        cents = round(float(cells[idx["amount"]]) * 100)
        qty = int(cells[idx["qty"]])
        bucket = buckets.setdefault(site, {"site": site, "amount_cents": 0,
                                           "qty": 0, "row_count": 0})
        bucket["amount_cents"] += cents
        bucket["qty"] += qty
        bucket["row_count"] += 1
    return list(buckets.values()), rows


def _segment_checksum(part: bytes) -> str:
    return hashlib.sha256(part).hexdigest()


def _put_segment(client, manifest: dict, index: int, part: bytes, checksum: str | None = None):
    checksum = checksum or _segment_checksum(part)
    return client.put(
        "/api/chunks/segment",
        data={"manifest": json.dumps(manifest), "index": str(index), "checksum": checksum},
        files={"file": (f"{index}.part", part, "application/octet-stream")},
    )


def _open(client, manifest: dict):
    return client.post("/api/chunks/session", data={"manifest": json.dumps(manifest)})


def _split(raw: bytes, size: int) -> list[bytes]:
    return [raw[i:i + size] for i in range(0, len(raw), size)]


def _fresh_client(tmp_path, monkeypatch) -> TestClient:
    db = tmp_path / "chunk.sqlite3"
    monkeypatch.setattr(settings, "DB_PATH", db)
    monkeypatch.setattr(settings, "CHUNK_STAGING_DIR", tmp_path / "chunks")
    # chunks 模块在调用时读 settings，故无需补丁模块属性
    client = TestClient(fastapi_app)
    client.__enter__()   # 触发生命周期：建表 + seed
    return client


@pytest.fixture()
def client(tmp_path, monkeypatch):
    test_client = _fresh_client(tmp_path, monkeypatch)
    yield test_client
    test_client.__exit__(None, None, None)


def _normal_csv(sample_dir):
    return (sample_dir / "normal_500.csv").read_bytes()


def test_manifest_identity_and_coverage_validation():
    from app import manifest
    raw = b"x" * 1000
    good = _make_manifest(raw, filename="o.csv", version="v1", segment_size=100,
                          amounts=[{"site": "beijing", "amount_cents": 100,
                                    "qty": 1, "row_count": 1}], total_rows=1)
    normalized = manifest.validate_manifest(good)
    assert normalized["file_id"] == good["sha256"]
    assert normalized["total_segments"] == 10
    assert manifest.segment_span(normalized, 9) == (900, 1000)

    bad_coverage = dict(good, total_segments=9)
    try:
        manifest.validate_manifest(bad_coverage)
        assert False, "覆盖范围矛盾应拒绝"
    except manifest.ManifestError as exc:
        assert "覆盖范围" in exc.message

    bad_rows = dict(good, total_rows=2)  # amounts 行数和=1
    normalized_rows = manifest.validate_manifest(bad_rows)
    self_errors = manifest.manifest_self_check(normalized_rows)
    assert self_errors and self_errors[0]["code"] == "total_rows"


def test_resumable_upload_publishes_after_all_segments(client, sample_dir):
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="orders.csv", version="v1",
                              segment_size=4096, amounts=amounts, total_rows=rows)
    parts = _split(raw, 4096)

    opened = _open(client, manifest).json()
    assert opened["status"] == "open"
    assert opened["total_segments"] == len(parts)

    before = client.get("/health").json()["fact_rows"]

    # 只传前两段 → 未收齐，不能 finalize
    for i in range(2):
        view = _put_segment(client, manifest, i, parts[i]).json()
    assert view["received_count"] == 2
    assert view["complete"] is False

    gap = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert gap.status_code == 409
    body = gap.json()
    assert body["status"] == "quarantined"
    assert body["missing_segments"] == list(range(2, len(parts)))   # 明确指出缺口
    # 隔离期间当前结果未被污染
    assert client.get("/health").json()["fact_rows"] == before
    assert client.get("/api/published").json()["published"] == []

    # 续传剩余段（模拟网络恢复），重传第 0 段应幂等不新增
    again = _put_segment(client, manifest, 0, parts[0])
    assert again.status_code == 200
    for i in range(2, len(parts)):
        _put_segment(client, manifest, i, parts[i])

    done = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert done.status_code == 200, done.text
    published = done.json()
    assert published["status"] == "published"
    assert published["written"] == rows
    assert published["rows_after"] == before + rows

    pointer = client.get("/api/published").json()["published"]
    assert len(pointer) == 1
    assert pointer[0]["filename"] == "orders.csv"
    assert pointer[0]["version"] == "v1"
    assert pointer[0]["batch_id"] == published["batch_id"]


def test_retry_same_file_does_not_create_new_batch(client, sample_dir):
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="dup.csv", version="v1",
                              segment_size=8192, amounts=amounts, total_rows=rows)
    parts = _split(raw, 8192)
    for i, part in enumerate(parts):
        _put_segment(client, manifest, i, part)
    first = client.post("/api/chunks/finalize",
                        data={"manifest": json.dumps(manifest)}).json()
    rows_after = first["rows_after"]

    # 再开一次会话（同 file_id）→ 复用；再 finalize → already_published，不新增批次
    reopened = _open(client, manifest).json()
    assert reopened["status"] == "published"
    second = client.post("/api/chunks/finalize",
                         data={"manifest": json.dumps(manifest)}).json()
    assert second["status"] == "already_published"
    assert second["batch_id"] == first["batch_id"]
    assert client.get("/health").json()["fact_rows"] == rows_after


def test_corrupt_segment_checksum_is_rejected_and_quarantined(client, sample_dir):
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="bad.csv", version="v1",
                              segment_size=4096, amounts=amounts, total_rows=rows)
    parts = _split(raw, 4096)
    _open(client, manifest)

    # 第 1 段内容被改坏，却带「正确内容的旧校验和」→ 来源不一致
    tampered = bytes(b ^ 0xFF for b in parts[1])
    res = _put_segment(client, manifest, 1, tampered, checksum=_segment_checksum(parts[1]))
    assert res.status_code == 409
    body = res.json()
    assert body["code"] == "segment_mismatch"
    assert body["session"]["status"] == "quarantined"

    # 坏字节没有落暂存：用正确字节补传第 1 段后解除隔离并能收齐发布
    fixed = _put_segment(client, manifest, 1, parts[1])
    assert fixed.status_code == 200
    for i, part in enumerate(parts):
        if i == 1:
            continue
        _put_segment(client, manifest, i, part)
    done = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert done.status_code == 200
    assert done.json()["status"] == "published"


def test_wrong_segment_size_marks_gap_and_quarantine(client, sample_dir):
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="size.csv", version="v1",
                              segment_size=4096, amounts=amounts, total_rows=rows)
    parts = _split(raw, 4096)
    _open(client, manifest)

    # 第 0 段少传几个字节 → 覆盖范围对不上
    res = _put_segment(client, manifest, 0, parts[0][:-10])
    assert res.status_code == 409
    assert res.json()["code"] == "segment_mismatch"
    # 正确补传后恢复
    assert _put_segment(client, manifest, 0, parts[0]).status_code == 200


def test_two_sources_same_segment_quarantine_until_consistent(client, sample_dir):
    """两人续传同一批，同段号发来不同来源内容：先留痕隔离，正确来源补齐才放行。"""
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="mix.csv", version="v1",
                              segment_size=4096, amounts=amounts, total_rows=rows)
    parts = _split(raw, 4096)
    _open(client, manifest)

    assert _put_segment(client, manifest, 0, parts[0]).status_code == 200
    # 另一来源：同段号、不同字节、自带匹配的新校验和
    foreign = parts[0] + b" " if len(parts[0]) < manifest["segment_size"] else b" " + parts[0][1:]
    res = _put_segment(client, manifest, 0, foreign)
    # 长度若恰好等于期望会被接收但标记 source_conflict 隔离
    if res.status_code == 200:
        status = client.get(f"/api/chunks/session/{manifest['sha256']}").json()
        assert status["status"] == "quarantined"
        assert any(e["code"] == "segment_source_conflict" for e in status["mismatch"])
    else:
        assert res.status_code == 409
    # 用权威字节覆盖回正确内容
    assert _put_segment(client, manifest, 0, parts[0]).status_code == 200
    for i in range(1, len(parts)):
        _put_segment(client, manifest, i, parts[i])
    done = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert done.json()["status"] == "published"


def test_manifest_totals_mismatch_quarantines_without_publishing(client, sample_dir):
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    # 清单谎报总行数 + 金额
    wrong_amounts = [dict(a, amount_cents=a["amount_cents"] + 100) for a in amounts]
    manifest = _make_manifest(raw, filename="lie.csv", version="v1",
                              segment_size=8192, amounts=wrong_amounts,
                              total_rows=rows + 5)
    parts = _split(raw, 8192)
    for i, part in enumerate(parts):
        _put_segment(client, manifest, i, part)
    res = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert res.status_code == 409
    body = res.json()
    assert body["status"] == "quarantined"
    codes = {e["code"] for e in body["mismatch"]}
    assert {"total_rows", "total_amount"} & codes
    # 当前版本指针为空，明细未被该批污染
    assert client.get("/api/published").json()["published"] == []


def test_switch_failure_keeps_previous_version(client, sample_dir):
    """发布事务注入故障：整体回滚，已发布的上一版指针与明细行数保持不变。"""
    from app import chunks, store
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="atomic.csv", version="v1",
                              segment_size=8192, amounts=amounts, total_rows=rows)
    parts = _split(raw, 8192)
    for i, part in enumerate(parts):
        _put_segment(client, manifest, i, part)
    first = client.post("/api/chunks/finalize",
                        data={"manifest": json.dumps(manifest)}).json()
    assert first["status"] == "published"
    pointer_before = client.get("/api/published").json()["published"][0]
    fact_before = client.get("/health").json()["fact_rows"]

    # 直接对同 file_id 再 finalize，但用 fail_after 注入故障（通过 service 层）
    conn = store.connect()
    try:
        from app import manifest as mmod
        normalized = mmod.validate_manifest(manifest)
        result = chunks.finalize(conn, normalized, fail_after=3)
    finally:
        conn.close()
    # 已发布会话走 already_published 分支；为真正验证回滚，构造第二个文件并注入故障
    raw2_lines = raw.decode("utf-8-sig").splitlines()
    raw2 = ("\n".join(raw2_lines[:20]) + "\n").encode("utf-8")
    amounts2, rows2 = _amounts_from_csv(raw2)
    manifest2 = _make_manifest(raw2, filename="atomic.csv", version="v2",
                               segment_size=4096, amounts=amounts2, total_rows=rows2)
    parts2 = _split(raw2, 4096)
    for i, part in enumerate(parts2):
        _put_segment(client, manifest2, i, part)

    conn = store.connect()
    try:
        normalized2 = __import__("app.manifest", fromlist=["validate_manifest"]).validate_manifest(manifest2)
        outcome = chunks.finalize(conn, normalized2, fail_after=3)
    finally:
        conn.close()
    assert outcome["status"] == "quarantined"
    # 上一版指针与明细行数纹丝不动
    pointer_after = client.get("/api/published").json()["published"][0]
    assert pointer_after["batch_id"] == pointer_before["batch_id"]
    assert pointer_after["version"] == "v1"
    assert client.get("/health").json()["fact_rows"] == fact_before


def test_older_version_cannot_overwrite_newer_published(client, sample_dir):
    raw_new = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw_new)

    # 先发 v2
    manifest_v2 = _make_manifest(raw_new, filename="ver.csv", version="v10",
                                 segment_size=8192, amounts=amounts, total_rows=rows)
    for i, part in enumerate(_split(raw_new, 8192)):
        _put_segment(client, manifest_v2, i, part)
    published_v2 = client.post("/api/chunks/finalize",
                               data={"manifest": json.dumps(manifest_v2)}).json()
    assert published_v2["status"] == "published"

    # 同一 filename 的较早版本 v9（不同字节身份）尝试补传发布
    raw_old = raw_new[: len(raw_new) // 2]
    # 保证仍是合法 CSV：截断到完整行
    text = raw_old.decode("utf-8-sig")
    text = text[: text.rfind("\n")] + "\n"
    raw_old = text.encode("utf-8")
    amounts_old, rows_old = _amounts_from_csv(raw_old)
    manifest_v1 = _make_manifest(raw_old, filename="ver.csv", version="v9",
                                 segment_size=4096, amounts=amounts_old, total_rows=rows_old)
    for i, part in enumerate(_split(raw_old, 4096)):
        _put_segment(client, manifest_v1, i, part)
    res = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest_v1)})
    assert res.status_code == 409
    assert res.json()["code"] == "stale_version"

    # 当前版本仍是 v10，明细行数未变
    pointer = client.get("/api/published").json()["published"][0]
    assert pointer["version"] == "v10"
    assert pointer["batch_id"] == published_v2["batch_id"]
    status = client.get(f"/api/chunks/session/{manifest_v1['sha256']}").json()
    assert status["status"] == "stale_rejected"


def test_chunk_finalize_after_regular_import_points_to_existing_batch(client, sample_dir):
    """同一文件先经整传入库，再收齐分片：不新增批次，只把当前版本指针指向既有批次。"""
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    # 先走普通整传管道入库
    regular = client.post(
        "/api/import",
        files={"file": ("shared.csv", raw, "text/csv")},
        data={"extra_policy": "reject"},
    )
    assert regular.status_code == 200
    existing_batch = regular.json()["batch_id"]

    manifest = _make_manifest(raw, filename="shared.csv", version="v1",
                              segment_size=8192, amounts=amounts, total_rows=rows)
    for i, part in enumerate(_split(raw, 8192)):
        _put_segment(client, manifest, i, part)
    done = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert done.status_code == 200
    body = done.json()
    assert body["status"] == "published"
    assert body["batch_id"] == existing_batch       # 复用既有批次，未新增
    assert body["written"] == 0
    pointer = client.get("/api/published").json()["published"][0]
    assert pointer["batch_id"] == existing_batch


def test_guard_rejected_in_transaction_keeps_previous_version(client, sample_dir):
    """importer 的 pre_commit_check 失败：整事务回滚，明细行数与当前版本均不变。"""
    from app import aggregate, importer, store
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="guard.csv", version="v1",
                              segment_size=8192, amounts=amounts, total_rows=rows)
    normalized = __import__("app.manifest", fromlist=["validate_manifest"]).validate_manifest(manifest)
    # 直接构造已解析行与聚合
    from app import encoding, pipeline, contract, rows as rows_mod
    probe = encoding.probe(raw)
    header, data, _ = pipeline.parse_csv(probe.text)
    cr = contract.check(header, "reject")
    validation = rows_mod.validate(data, cr["header"], cr["columns_used"])
    summary = aggregate.summarize(validation.rows)

    conn = store.connect()
    try:
        batches_before = conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"]
        facts_before = conn.execute("SELECT COUNT(*) AS c FROM fact_orders").fetchone()["c"]
        outcome = importer.import_batch(
            conn, filename="guard.csv", sha256=normalized["sha256"],
            rows=validation.rows, aggregates=summary, encoding=probe.encoding, bom=probe.bom,
            publish={"filename": "guard.csv", "file_id": normalized["file_id"], "version": "v1"},
            pre_commit_check=lambda txn: "模拟清单复核失败",
        )
        batches_after = conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"]
        facts_after = conn.execute("SELECT COUNT(*) AS c FROM fact_orders").fetchone()["c"]
        published = conn.execute("SELECT COUNT(*) AS c FROM published_files").fetchone()["c"]
        same_sha = conn.execute(
            "SELECT COUNT(*) AS c FROM batches WHERE sha256 = ?",
            (normalized["sha256"],)).fetchone()["c"]
    finally:
        conn.close()
    assert outcome["status"] == "guard_rejected"
    assert batches_after == batches_before          # 守卫失败整体回滚，未新增批次
    assert facts_after == facts_before                # 明细行数不变（仍只有 seed）
    assert published == 0                            # 当前版本指针未被写入
    assert same_sha == 0


def test_quarantine_recovers_after_missing_segments_are_filled(client, sample_dir):
    """因缺段被隔离的会话，补齐且逐段复核通过后解除隔离并正常发布。"""
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="recover.csv", version="v1",
                              segment_size=4096, amounts=amounts, total_rows=rows)
    parts = _split(raw, 4096)
    _open(client, manifest)
    # 先传除最后一段外的全部
    for i in range(len(parts) - 1):
        _put_segment(client, manifest, i, parts[i])
    gap = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert gap.status_code == 409
    assert gap.json()["status"] == "quarantined"

    # 补齐最后一段后再 finalize：隔离解除、成功发布
    _put_segment(client, manifest, len(parts) - 1, parts[-1])
    done = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert done.status_code == 200
    assert done.json()["status"] == "published"
    view = client.get(f"/api/chunks/session/{manifest['sha256']}").json()
    assert view["status"] == "published"
    assert view["gaps"] == []


def test_concurrent_identical_segment_uploads_are_idempotent(client, sample_dir):
    """两人同时续传同一批的同一段：内容一致时只保留一份，不产生重复/脏拼。"""
    import threading

    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="concurrent.csv", version="v1",
                              segment_size=4096, amounts=amounts, total_rows=rows)
    parts = _split(raw, 4096)
    _open(client, manifest)
    errors = []

    def uploader():
        try:
            for i, part in enumerate(parts):
                res = _put_segment(client, manifest, i, part)
                if res.status_code != 200:
                    errors.append((i, res.status_code, res.json()))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=uploader) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    done = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert done.status_code == 200
    assert done.json()["status"] == "published"
    # 同一段在并发下只有一条记录
    from app import store
    conn = store.connect()
    try:
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM chunk_segments WHERE file_id = ?",
            (manifest["sha256"],)).fetchone()["c"]
    finally:
        conn.close()
    assert count == len(parts)


def test_whole_file_checksum_mismatch_quarantines(client, sample_dir):
    """拼装后整文件 sha 与清单不符（不同来源片段混传的最终防线）。"""
    raw = _normal_csv(sample_dir)
    amounts, rows = _amounts_from_csv(raw)
    manifest = _make_manifest(raw, filename="whole.csv", version="v1",
                              segment_size=4096, amounts=amounts, total_rows=rows)
    parts = _split(raw, 4096)
    _open(client, manifest)
    # 全部段都按「各自正确校验和」上传，但最后一段偷偷换内容并给出匹配的新校验和
    for i, part in enumerate(parts[:-1]):
        _put_segment(client, manifest, i, part)
    last = parts[-1]
    swapped = last[:-1] + bytes([last[-1] ^ 0x01])
    _put_segment(client, manifest, len(parts) - 1, swapped)
    res = client.post("/api/chunks/finalize", data={"manifest": json.dumps(manifest)})
    assert res.status_code == 409
    body = res.json()
    assert body["status"] == "quarantined"
    assert any(e["code"] == "whole_checksum" for e in body["mismatch"])
    assert client.get("/api/published").json()["published"] == []
