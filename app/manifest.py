"""模块 7 · 分片清单（manifest）与单段核对（纯函数）。

同一份文件的「身份」由完整文件 sha256 决定，清单把身份和可核对的范围钉死：
- filename / version：业务文件名与来源版本标签（较早的补传不能改写较新版本）
- sha256：完整文件 64 位十六进制摘要，file_id 即取它
- total_size / segment_size / total_segments：逐段字节覆盖范围（段号 + 起止偏移）
- total_rows：完整文件的数据行总数
- amounts：按站点的金额/件数/行数清单，清单内行数之和必须等于 total_rows

任何片段都必须能在这份清单里找到唯一的段号、偏移与长度；对不上就是来源不一致，
调用方据此整批隔离（quarantine），而不是把片段拼进当前结果。
"""
from __future__ import annotations

import hashlib
import math
import re

from . import contract, settings


class ManifestError(ValueError):
    """清单本身不合法（缺字段/类型错/范围自相矛盾）；面向调用方返回 400。"""

    def __init__(self, message: str, field: str = "") -> None:
        super().__init__(message)
        self.message = message
        self.field = field


def _require_int(payload: dict, key: str, *, minimum: int | None = None,
                 maximum: int | None = None) -> int:
    if key not in payload:
        raise ManifestError(f"清单缺少必需字段 {key}。", key)
    value = payload[key]
    # bool 是 int 的子类，显式排除，避免 true 被当成 1
    if isinstance(value, bool) or not isinstance(value, int):
        raise ManifestError(f"字段 {key} 必须是整数，当前是 {type(value).__name__}。", key)
    if minimum is not None and value < minimum:
        raise ManifestError(f"字段 {key}={value} 小于下限 {minimum}。", key)
    if maximum is not None and value > maximum:
        raise ManifestError(f"字段 {key}={value} 超过上限 {maximum}。", key)
    return value


def _require_str(payload: dict, key: str, *, max_length: int) -> str:
    if key not in payload:
        raise ManifestError(f"清单缺少必需字段 {key}。", key)
    value = payload[key]
    if not isinstance(value, str):
        raise ManifestError(f"字段 {key} 必须是字符串。", key)
    value = value.strip()
    if not value:
        raise ManifestError(f"字段 {key} 不能为空。", key)
    if len(value) > max_length:
        raise ManifestError(f"字段 {key} 长度 {len(value)} 超过上限 {max_length}。", key)
    return value


def validate_manifest(payload: dict) -> dict:
    """校验并归一化客户端清单；非法直接抛 ManifestError。返回归一化副本。"""
    if not isinstance(payload, dict):
        raise ManifestError("清单必须是 JSON 对象。")

    filename = _require_str(payload, "filename", max_length=settings.MAX_FILENAME)
    if any(sep in filename for sep in ("/", "\\")) or filename in (".", ".."):
        raise ManifestError("filename 不能包含路径分隔符或相对路径片段。", "filename")
    version = _require_str(payload, "version", max_length=settings.MAX_FILE_VERSION)
    sha256 = _require_str(payload, "sha256", max_length=64).lower()
    if not re.match(settings.HEX64_RE, sha256):
        raise ManifestError("sha256 必须是 64 位十六进制字符串。", "sha256")

    total_size = _require_int(payload, "total_size", minimum=1)
    segment_size = _require_int(payload, "segment_size", minimum=1,
                                maximum=settings.MAX_CHUNK_BYTES)
    total_segments = _require_int(payload, "total_segments", minimum=1,
                                  maximum=settings.MAX_SEGMENTS)
    total_rows = _require_int(payload, "total_rows", minimum=1)

    expected_segments = math.ceil(total_size / segment_size)
    if expected_segments != total_segments:
        raise ManifestError(
            f"覆盖范围自相矛盾：total_size={total_size}、segment_size={segment_size} "
            f"应推出 {expected_segments} 段，清单却声明 {total_segments} 段。",
            "total_segments",
        )

    amounts = _validate_amounts(payload.get("amounts"), total_rows)

    normalized = {
        "filename": filename,
        "version": version,
        "sha256": sha256,
        "file_id": sha256,
        "total_size": total_size,
        "segment_size": segment_size,
        "total_segments": total_segments,
        "total_rows": total_rows,
        "amounts": amounts,
        "amount_total_cents": sum(int(a["amount_cents"]) for a in amounts),
        "amount_total_qty": sum(int(a["qty"]) for a in amounts),
    }
    return normalized


def _validate_amounts(raw: object, total_rows: int) -> list[dict]:
    if not isinstance(raw, list) or not raw:
        raise ManifestError("amounts 必须是非空数组（按站点的金额清单）。", "amounts")
    if len(raw) > settings.MAX_AMOUNT_LIST:
        raise ManifestError(
            f"amounts 条目数 {len(raw)} 超过上限 {settings.MAX_AMOUNT_LIST}。", "amounts")

    seen: set[str] = set()
    out: list[dict] = []
    for i, item in enumerate(raw):
        prefix = f"amounts[{i}]"
        if not isinstance(item, dict):
            raise ManifestError(f"{prefix} 必须是对象。", "amounts")
        site = item.get("site")
        if not isinstance(site, str) or not site.strip():
            raise ManifestError(f"{prefix}.site 不能为空。", "amounts")
        site = site.strip()
        if site not in contract.SITES:
            raise ManifestError(
                f"{prefix}.site=「{site}」不在契约站点内：{', '.join(contract.SITES)}。",
                "amounts")
        if site in seen:
            raise ManifestError(f"{prefix}.site=「{site}」在金额清单里重复。", "amounts")
        seen.add(site)
        amount_cents = item.get("amount_cents")
        qty = item.get("qty")
        row_count = item.get("row_count")
        for name, val in (("amount_cents", amount_cents), ("qty", qty),
                          ("row_count", row_count)):
            if isinstance(val, bool) or not isinstance(val, int):
                raise ManifestError(f"{prefix}.{name} 必须是整数。", "amounts")
        if amount_cents < 0 or qty < 0 or row_count < 1:
            raise ManifestError(
                f"{prefix} 金额/件数不能为负，row_count 至少为 1。", "amounts")
        out.append({"site": site, "amount_cents": int(amount_cents),
                    "qty": int(qty), "row_count": int(row_count)})

    listed_rows = sum(a["row_count"] for a in out)
    # 注意：这里不强制 listed_rows == total_rows。清单自相矛盾属于「来源不一致」，
    # 要允许会话先建立、片段先留存，等到 finalize 再和实际解析结果一起整批隔离。
    return out


def segment_span(manifest: dict, index: int) -> tuple[int, int]:
    """返回第 index 段（0 起）的 [start, end) 字节偏移与该段应有长度。"""
    total_segments = int(manifest["total_segments"])
    if not 0 <= index < total_segments:
        raise ManifestError(
            f"段号 {index} 越界：合法范围 0..{total_segments - 1}。", "index")
    start = index * int(manifest["segment_size"])
    end = min(start + int(manifest["segment_size"]), int(manifest["total_size"]))
    return start, end


def expected_segment_size(manifest: dict, index: int) -> int:
    start, end = segment_span(manifest, index)
    return end - start


def check_segment(manifest: dict, index: int, checksum: str, data: bytes) -> dict:
    """核对单段：段号范围、实际长度与覆盖范围、段体 sha256 与随段校验和。

    返回 {ok, errors:[{code,message}], expected_size, actual_size}；不抛异常，
    由调用方决定拒收（409）还是累计后整批隔离。
    """
    errors: list[dict] = []
    total_segments = int(manifest["total_segments"])
    if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < total_segments:
        errors.append({"code": "segment_index",
                       "message": f"段号 {index!r} 越界，合法范围 0..{total_segments - 1}。"})
        return {"ok": False, "errors": errors, "expected_size": None,
                "actual_size": len(data)}

    start, end = segment_span(manifest, index)
    expected = end - start
    actual = len(data)
    if actual != expected:
        errors.append({
            "code": "segment_size",
            "message": (f"第 {index} 段长度错配：清单覆盖 [{start},{end}) 应为 {expected} 字节，"
                        f"实际收到 {actual} 字节。"),
        })

    if not isinstance(checksum, str) or not re.match(settings.HEX64_RE, checksum):
        errors.append({"code": "checksum_format",
                       "message": f"第 {index} 段随段 checksum 不是 64 位十六进制。"})
    else:
        actual_sum = hashlib.sha256(data).hexdigest()
        if actual_sum != checksum.lower():
            errors.append({
                "code": "segment_checksum",
                "message": (f"第 {index} 段内容 sha256 与随段校验和不一致："
                            f"实际 {actual_sum[:16]}… vs 声明 {checksum[:16]}…，来源字节对不上。"),
            })
    return {"ok": not errors, "errors": errors, "expected_size": expected,
            "actual_size": actual, "span": [start, end]}


def reassembled_checksum(parts: list[bytes]) -> str:
    """按段号顺序拼接后的整文件 sha256（与 manifest.sha256 做最终身份核对）。"""
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part)
    return digest.hexdigest()


def manifest_self_check(manifest: dict) -> list[dict]:
    """清单内部自洽性（总行数 vs 金额清单行数和）；在 finalize 阶段用于整批隔离。"""
    listed_rows = sum(int(a["row_count"]) for a in manifest["amounts"])
    if listed_rows != manifest["total_rows"]:
        return [{
            "code": "total_rows",
            "message": (f"清单自相矛盾：total_rows={manifest['total_rows']}，"
                        f"但 amounts 各行数之和={listed_rows}。"),
        }]
    return []


def version_key(version: str) -> tuple:
    """把版本标签拆成可比较的自然序（数字段按整数比，非数字段按字符串比）。

    例：v2 < v10；2026-11-14 < 2026-11-15。无法解析时退化为字符串比较，
    保证「较早的补传不能改写已经发布的较新版本」有确定性判定。
    """
    tokens = re.findall(r"\d+|[^\d]+", version or "")
    return tuple((0, int(t)) if t.isdigit() else (1, t) for t in tokens)
