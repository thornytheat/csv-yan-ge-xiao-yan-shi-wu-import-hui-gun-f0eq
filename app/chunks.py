"""模块 8 · 分片续传、整批隔离与原子发布。

设计原则对应值班场景里的四类事故：
1. 网络中断只能整份重传
   → 先到的片段先落暂存（chunk_segments + 暂存文件），会话按 file_id 持久化，可随时续传。
2. 两人续传同一批把不同来源片段拼在一起
   → 会话由「完整文件 sha256」唯一确定；每段核对段号/覆盖范围/段体校验和，
     来源不一致的片段拒收并累计错配，绝不拼进结果。
3. 未收齐/来源不一致的内容进入当前结果
   → 缺段或错配即把整批会话置为 quarantined 并给出缺口清单；隔离态不允许发布。
4. 切换失败污染线上、旧补传覆盖新版本
   → 收齐后在单事务内写 明细+聚合+当前版本指针，失败 ROLLBACK 保留上一版；
     发布前比对版本自然序，较早版本一律拒绝改写。
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

from . import (
    aggregate,
    encoding,
    importer,
    manifest as manifest_mod,
    pipeline,
    settings,
    store,
)
from .manifest import version_key


class ChunkConflict(Exception):
    """HTTP 409 级冲突：会话状态不允许该操作，或片段来源不一致。"""

    def __init__(self, message: str, *, code: str = "conflict", extra: dict | None = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.extra = extra or {}


def staging_root() -> Path:
    root = Path(settings.CHUNK_STAGING_DIR)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _session_dir(file_id: str) -> Path:
    # file_id 是 64 位十六进制，不含路径分隔符；每个文件身份独立目录
    directory = staging_root() / file_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _segment_path(file_id: str, index: int) -> Path:
    return _session_dir(file_id) / f"{index:06d}.part"


def _atomic_write(path: Path, data: bytes) -> None:
    """同目录临时文件 + os.replace，保证并发/崩溃后不会读到写了一半的片段。"""
    tmp = path.with_suffix(f".part.{os.getpid()}.tmp")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def open_session(conn: sqlite3.Connection, manifest: dict) -> dict:
    """按 file_id 打开（或幂等复用）会话；重试同一文件不新增会话/批次。"""
    session = store.create_session(conn, manifest)
    # 既有会话必须与本次清单完全同身份同范围，否则说明 file_id 撞了不同声明
    if not _manifest_matches_session(manifest, session):
        raise ChunkConflict(
            "同一 file_id 已存在但清单声明不一致，拒绝混用来源。",
            code="manifest_conflict", extra={"existing": public_session(session)},
        )
    return session


def _manifest_matches_session(manifest: dict, session: dict) -> bool:
    return (
        session["filename"] == manifest["filename"]
        and session["total_size"] == manifest["total_size"]
        and session["segment_size"] == manifest["segment_size"]
        and session["total_segments"] == manifest["total_segments"]
        and session["total_rows"] == manifest["total_rows"]
    )


def receive_segment(conn: sqlite3.Connection, manifest: dict, index: int,
                    checksum: str, data: bytes) -> dict:
    """接收单段并返回会话视图。坏段拒收（不写暂存）；缺段/错配驱动整批隔离。"""
    session = open_session(conn, manifest)

    if session["status"] == "published":
        # 已发布的同文件再传片段：幂等返回，不产生任何新写入
        return public_session(session, note="该文件已发布，片段上传被忽略（幂等）。")
    if session["status"] == "stale_rejected":
        raise ChunkConflict(
            "该文件身份是较早版本，线上已有更新版本；补传被拒绝，不能改写当前结果。",
            code="stale_version", extra={"published": _published_hint(conn, manifest["filename"])},
        )

    check = manifest_mod.check_segment(manifest, index, checksum, data)
    if not check["ok"]:
        # 来源不一致：记录错配并把整批隔离，坏字节不落暂存
        mismatches = list(session.get("mismatch") or [])
        mismatches.extend(check["errors"])
        gaps = compute_gaps(manifest, store.received_indexes(conn, manifest["file_id"]))
        store.update_session_status(
            conn, manifest["file_id"], "quarantined",
            quarantine_reason="片段来源不一致（段号/覆盖范围/校验和错配），整批隔离。",
            gaps=gaps, mismatches=mismatches,
        )
        raise ChunkConflict(
            "；".join(e["message"] for e in check["errors"]),
            code="segment_mismatch",
            extra={"mismatch": check["errors"], "gaps": gaps},
        )

    start, end = manifest_mod.segment_span(manifest, index)
    path = _segment_path(manifest["file_id"], index)

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = store.get_segment(conn, manifest["file_id"], index)
        _atomic_write(path, data)
        store.upsert_segment(
            conn, file_id=manifest["file_id"], index=index, checksum=checksum.lower(),
            size_bytes=len(data), start_offset=start, end_offset=end,
            stored_path=str(path),
        )
        conn.execute("COMMIT")
        if existing and existing["checksum"] != checksum.lower():
            # 同段号此前收过不同内容：说明来源曾不一致，留痕并隔离，等正确来源补齐
            refreshed_for_conflict = store.get_session(conn, manifest["file_id"])
            prior_mismatch = list(
                (refreshed_for_conflict or {}).get("mismatch") or [])
            prior_mismatch.append({
                "code": "segment_source_conflict",
                "message": (f"第 {index} 段曾收到不同校验和的内容（旧 {existing['checksum'][:16]}…/"
                            f"新 {checksum.lower()[:16]}…），已按最新字节覆盖并整批隔离待复核。"),
            })
            store.update_session_status(
                conn, manifest["file_id"], "quarantined",
                quarantine_reason="同段号出现不同来源内容，整批隔离待复核。",
                gaps=compute_gaps(manifest, store.received_indexes(conn, manifest["file_id"])),
                mismatches=prior_mismatch,
            )
    except Exception:
        conn.execute("ROLLBACK")
        raise

    refreshed = store.get_session(conn, manifest["file_id"])
    return public_session(refreshed)  # type: ignore[arg-type]


def compute_gaps(manifest: dict, received: list[int]) -> list[int]:
    wanted = set(range(manifest["total_segments"]))
    return sorted(wanted.difference(set(received)))


def _reassemble(conn: sqlite3.Connection, manifest: dict) -> bytes:
    parts: list[bytes] = []
    for index in range(manifest["total_segments"]):
        segment = store.get_segment(conn, manifest["file_id"], index)
        if segment is None:  # 理论上不会发生，调用方已先算缺口
            raise ChunkConflict(f"第 {index} 段缺失，无法拼装。", code="gap")
        parts.append(Path(segment["stored_path"]).read_bytes())
    return b"".join(parts)


def finalize(conn: sqlite3.Connection, manifest: dict, *, fail_after: int | None = None) -> dict:
    """收齐后核对并一次性切换；返回 {status, ...}，不合法一律隔离且不触碰当前结果。"""
    session = open_session(conn, manifest)

    if session["status"] == "published":
        published = store.get_published(conn, manifest["filename"])
        return {
            "status": "already_published", "file_id": manifest["file_id"],
            "batch_id": session.get("batch_id"), "published": published,
            "note": "同一文件已发布，重试不新增批次。",
        }
    if session["status"] == "stale_rejected":
        raise ChunkConflict(
            "较早版本的补传不能改写已经发布的较新版本。", code="stale_version",
            extra={"published": _published_hint(conn, manifest["filename"])},
        )

    received = store.received_indexes(conn, manifest["file_id"])
    gaps = compute_gaps(manifest, received)
    if gaps:
        reason = f"尚有 {len(gaps)} 个片段未收齐，整批隔离，不进入当前结果。"
        store.update_session_status(conn, manifest["file_id"], "quarantined",
                                    quarantine_reason=reason, gaps=gaps,
                                    mismatches=session.get("mismatch") or [])
        return {"status": "quarantined", "reason": reason, "gaps": gaps,
                "missing_segments": gaps, "file_id": manifest["file_id"]}

    # 来源一致性复核：暂存段的校验和与库内声明逐段再比一次
    seg_errors = _verify_stored_checksums(conn, manifest)
    if seg_errors:
        store.update_session_status(
            conn, manifest["file_id"], "quarantined",
            quarantine_reason="暂存片段复核失败，整批隔离。", gaps=[], mismatches=seg_errors)
        return {"status": "quarantined", "reason": "暂存片段复核失败。",
                "mismatch": seg_errors, "gaps": [], "file_id": manifest["file_id"]}

    # 片段已收齐且逐段复核通过：此前因缺段/坏段造成的隔离可以解除，
    # 后续若整文件身份或清单口径再失败，仍会重新隔离。
    if session["status"] == "quarantined":
        store.update_session_status(conn, manifest["file_id"], "open",
                                    quarantine_reason="片段已补齐且复核通过，解除隔离继续发布核对。",
                                    gaps=[], mismatches=session.get("mismatch") or [])

    raw = _reassemble(conn, manifest)

    # 身份核对：拼好的整文件 sha256 必须等于清单声明
    whole_sha = manifest_mod.reassembled_checksum([raw])
    if whole_sha != manifest["sha256"]:
        mismatch = [{
            "code": "whole_checksum",
            "message": (f"拼装后整文件 sha256={whole_sha} 与清单声明 {manifest['sha256']} 不一致，"
                        "片段来源不是同一份文件，整批隔离。"),
        }]
        store.update_session_status(conn, manifest["file_id"], "quarantined",
                                    quarantine_reason="整文件身份核对失败，整批隔离。",
                                    gaps=[], mismatches=mismatch)
        return {"status": "quarantined", "reason": mismatch[0]["message"],
                "mismatch": mismatch, "gaps": [], "file_id": manifest["file_id"]}

    # 版本守卫：较早补传不能改写已发布的较新版本
    published = store.get_published(conn, manifest["filename"])
    if published is not None and version_key(manifest["version"]) < version_key(published["version"]):
        store.update_session_status(
            conn, manifest["file_id"], "stale_rejected",
            quarantine_reason=(f"清单版本 {manifest['version']} 早于已发布版本 "
                               f"{published['version']}，拒绝改写当前结果。"),
            gaps=[], mismatches=[])
        raise ChunkConflict(
            f"版本 {manifest['version']} 早于当前已发布版本 {published['version']}，拒绝发布。",
            code="stale_version", extra={"published": _published_hint(conn, manifest["filename"])})

    # 走既有管道的纯函数部分：编码 → 契约 → 行校验 → 聚合（此时不写库）
    self_errors = manifest_mod.manifest_self_check(manifest)
    if self_errors:
        store.update_session_status(
            conn, manifest["file_id"], "quarantined",
            quarantine_reason="清单总行数与金额清单自相矛盾，整批隔离。",
            gaps=[], mismatches=self_errors)
        return {"status": "quarantined", "reason": self_errors[0]["message"],
                "mismatch": self_errors, "gaps": [], "file_id": manifest["file_id"]}

    parsed = _parse_only(raw, manifest["filename"])
    if parsed["status"] != "ok":
        reason = parsed.get("detail", "内容校验未通过，整批隔离。")
        store.update_session_status(conn, manifest["file_id"], "quarantined",
                                    quarantine_reason=reason, gaps=[],
                                    mismatches=parsed.get("errors", [])[:50])
        return {"status": "quarantined", "reason": reason,
                "pipeline": {k: parsed.get(k) for k in ("status", "stage", "encoding")},
                "gaps": [], "file_id": manifest["file_id"]}

    summary = parsed["aggregate"]
    checklist_errors = _check_manifest_totals(manifest, summary)
    if checklist_errors:
        # 还没写库：直接隔离，明细与当前版本都不会被触碰
        store.update_session_status(
            conn, manifest["file_id"], "quarantined",
            quarantine_reason="总行数/金额清单与实际内容不一致，整批隔离，未切换当前版本。",
            gaps=[], mismatches=checklist_errors)
        return {"status": "quarantined",
                "reason": "清单声明与实际解析内容不一致，未切换当前版本。",
                "mismatch": checklist_errors, "gaps": [], "file_id": manifest["file_id"]}

    # 一次性原子切换：明细 + 聚合 + 当前版本指针在同一事务；
    # 清单复核（pre_commit_check）与版本守卫在事务内执行，失败整体回滚，保留上一版。
    publish_meta = {"filename": manifest["filename"], "file_id": manifest["file_id"],
                    "version": manifest["version"]}
    previous_published = store.get_published(conn, manifest["filename"])

    def guard(txn: sqlite3.Connection) -> str:
        current = store.get_published(txn, manifest["filename"])
        if current is not None and version_key(manifest["version"]) < version_key(current["version"]):
            return (f"并发切换：线上已存在更新版本 {current['version']}，"
                    f"本次版本 {manifest['version']} 放弃发布。")
        return ""

    outcome = importer.import_batch(
        conn, filename=manifest["filename"], sha256=manifest["sha256"],
        rows=parsed["rows"], aggregates=summary, encoding=parsed["encoding"],
        bom=parsed["bom"], force_new_batch=False, fail_after=fail_after,
        note=f"分片续传发布（{manifest['total_segments']} 段，version={manifest['version']}）",
        publish=publish_meta, pre_commit_check=guard,
    )

    if outcome["status"] == "duplicate_file":
        # 同 sha256 已通过整传等路径入库：不新增批次，只在版本守卫通过后
        # 把当前版本指针原子指向既有相同批次（重试同一文件不新增批次）。
        existing_batch_id = outcome["batch_id"]
        switched = _point_published(conn, manifest=manifest, batch_id=existing_batch_id,
                                    row_count=summary["row_count"])
        if not switched["ok"]:
            store.update_session_status(conn, manifest["file_id"], "stale_rejected",
                                        quarantine_reason=switched["reason"], gaps=[],
                                        mismatches=[])
            raise ChunkConflict(switched["reason"], code="stale_version",
                                extra={"published": _published_hint(conn, manifest["filename"])})
        store.update_session_status(
            conn, manifest["file_id"], "published",
            quarantine_reason="", gaps=[], mismatches=[], batch_id=existing_batch_id)
        return {
            "status": "published", "file_id": manifest["file_id"],
            "batch_id": existing_batch_id, "written": 0,
            "note": "同 sha256 批次已存在，未新增批次，仅切换当前版本指针。",
            "rows_before": outcome.get("rows_before"), "rows_after": outcome.get("rows_after"),
            "aggregate": summary, "previous_published": previous_published,
        }

    if outcome["status"] != "committed":
        reason = outcome.get("detail", f"发布事务未提交（{outcome['status']}），整批隔离。")
        store.update_session_status(conn, manifest["file_id"], "quarantined",
                                    quarantine_reason=reason, gaps=[], mismatches=[])
        return {"status": "quarantined", "reason": reason, "gaps": [],
                "file_id": manifest["file_id"], "import": outcome}

    batch_id = outcome["batch_id"]
    store.update_session_status(
        conn, manifest["file_id"], "published",
        quarantine_reason="", gaps=[], mismatches=[], batch_id=batch_id)
    return {
        "status": "published", "file_id": manifest["file_id"], "batch_id": batch_id,
        "attempt_id": outcome.get("attempt_id"), "written": outcome.get("written"),
        "rows_before": outcome.get("rows_before"), "rows_after": outcome.get("rows_after"),
        "aggregate": summary, "previous_published": previous_published,
        "superseded_batch_id": outcome.get("superseded_batch_id"),
    }


def _parse_only(raw: bytes, filename: str) -> dict:
    """复用管道的纯函数阶段做完整内容校验，不产生任何写库副作用。"""
    try:
        probe = encoding.probe(raw)
    except encoding.EncodingError as exc:
        return {"status": "encoding_failed", "detail": exc.message, "errors": []}

    header, data, _blanks = pipeline.parse_csv(probe.text)
    from . import contract, report, rows as rows_mod
    contract_result = contract.check(header, "reject")
    if not contract_result["ok"]:
        detail = "；".join(e["message"] for e in contract_result["errors"])
        return {"status": "rejected", "stage": "contract", "detail": detail,
                "encoding": probe.encoding,
                "errors": [{"code": e["code"], "message": e["message"]}
                           for e in contract_result["errors"]]}

    validation = rows_mod.validate(data, contract_result["header"],
                                   contract_result["columns_used"])
    if not validation.ok:
        rep = report.build_report(validation.error_dicts())
        return {"status": "rejected", "stage": "rows",
                "detail": f"{validation.error_count} 处行级错误，整批隔离。",
                "encoding": probe.encoding, "errors": rep["rows"]}

    return {"status": "ok", "rows": validation.rows,
            "aggregate": aggregate.summarize(validation.rows),
            "encoding": probe.encoding, "bom": probe.bom}


def _verify_stored_checksums(conn: sqlite3.Connection, manifest: dict) -> list[dict]:
    errors: list[dict] = []
    for index in range(manifest["total_segments"]):
        segment = store.get_segment(conn, manifest["file_id"], index)
        if segment is None:
            errors.append({"code": "gap", "message": f"第 {index} 段缺失。"})
            continue
        data = Path(segment["stored_path"]).read_bytes()
        actual = hashlib.sha256(data).hexdigest()
        if actual != segment["checksum"]:
            errors.append({"code": "stored_checksum",
                           "message": f"第 {index} 段暂存字节与登记校验和不一致。"})
        start, end = manifest_mod.segment_span(manifest, index)
        if (segment["start_offset"], segment["end_offset"]) != (start, end):
            errors.append({"code": "span",
                           "message": f"第 {index} 段登记覆盖范围与清单不符。"})
    return errors


def _check_manifest_totals(manifest: dict, summary: dict) -> list[dict]:
    errors: list[dict] = []
    if manifest["total_rows"] != summary["row_count"]:
        errors.append({
            "code": "total_rows",
            "message": (f"总行数错配：清单声明 {manifest['total_rows']} 行，"
                        f"实际解析 {summary['row_count']} 行。"),
        })
    if manifest["amount_total_cents"] != summary["total_amount_cents"]:
        errors.append({
            "code": "total_amount",
            "message": (f"总金额错配：清单声明 {manifest['amount_total_cents']} 分，"
                        f"实际汇总 {summary['total_amount_cents']} 分。"),
        })
    if manifest["amount_total_qty"] != summary["total_qty"]:
        errors.append({
            "code": "total_qty",
            "message": (f"总件数错配：清单声明 {manifest['amount_total_qty']} 件，"
                        f"实际汇总 {summary['total_qty']} 件。"),
        })
    actual_by_site = {g["site"]: g for g in summary["groups"]}
    for declared in manifest["amounts"]:
        site = declared["site"]
        actual = actual_by_site.get(site)
        if actual is None:
            errors.append({"code": "amount_site_missing",
                           "message": f"金额清单中的站点 {site} 在实际数据里没有任何行。"})
            continue
        if actual["amount_cents"] != declared["amount_cents"]:
            errors.append({
                "code": "amount_site",
                "message": (f"站点 {site} 金额错配：清单 {declared['amount_cents']} 分，"
                            f"实际 {actual['amount_cents']} 分。"),
            })
        if actual["row_count"] != declared["row_count"]:
            errors.append({
                "code": "amount_site_rows",
                "message": (f"站点 {site} 行数错配：清单 {declared['row_count']} 行，"
                            f"实际 {actual['row_count']} 行。"),
            })
        if actual["qty"] != declared["qty"]:
            errors.append({
                "code": "amount_site_qty",
                "message": (f"站点 {site} 件数错配：清单 {declared['qty']} 件，"
                            f"实际 {actual['qty']} 件。"),
            })
    return errors


def _point_published(conn: sqlite3.Connection, *, manifest: dict, batch_id: str,
                     row_count: int) -> dict:
    """把当前版本指针指向一个已存在的相同批次；事务内含版本守卫，失败保留上一版。"""
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = store.get_published(conn, manifest["filename"])
        if current is not None and version_key(manifest["version"]) < version_key(current["version"]):
            conn.execute("ROLLBACK")
            return {"ok": False,
                    "reason": (f"版本 {manifest['version']} 早于当前已发布版本 "
                               f"{current['version']}，拒绝切换。")}
        store.upsert_published(conn, filename=manifest["filename"], file_id=manifest["file_id"],
                               version=manifest["version"], batch_id=batch_id,
                               total_rows=row_count)
        conn.execute("COMMIT")
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001 - 切换失败必须保留上一版
        conn.execute("ROLLBACK")
        return {"ok": False, "reason": f"当前版本切换失败，已保留上一版：{exc}"}


def _published_hint(conn: sqlite3.Connection, filename: str) -> dict | None:
    published = store.get_published(conn, filename)
    return published


def public_session(session: dict, note: str = "") -> dict:
    received = session.get("received_count", 0)
    total = session["total_segments"]
    view = {
        "file_id": session["file_id"],
        "filename": session["filename"],
        "version": session["version"],
        "status": session["status"],
        "total_segments": total,
        "received_count": received,
        "complete": received >= total,
        "total_rows": session["total_rows"],
        "total_size": session["total_size"],
        "quarantine_reason": session.get("quarantine_reason", ""),
        "gaps": session.get("gap", []),
        "mismatch": session.get("mismatch", []),
        "amounts": session.get("amounts", []),
        "batch_id": session.get("batch_id"),
        "updated_at": session.get("updated_at"),
    }
    if note:
        view["note"] = note
    return view


def session_status(conn: sqlite3.Connection, file_id: str) -> dict:
    session = store.get_session(conn, file_id)
    if session is None:
        raise ChunkConflict(f"file_id {file_id} 没有对应会话。", code="unknown_session")
    view = public_session(session)
    view["received_indexes"] = store.received_indexes(conn, file_id)
    return view
