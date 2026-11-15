# csvgate · CSV 严格校验与事务导入回滚

上游每天给一份站点订单 CSV，要求「**全对才入库，错了要知道第几行错哪列**」。
本工具把这份人工核对变成 6 个可复核的后端模块，任一行非法则整份回滚，库内明细行数不变。

使用者：数据平台组入库值班同学。

## 能力

| 模块 | 落地位置 | 做什么 |
|---|---|---|
| 1 上传与编码探测 | `app/encoding.py` | BOM → UTF-8-SIG；严格 UTF-8 失败 → GB18030；都失败 → 400 + 建议。响应记录是否 BOM、前 16 字节摘要、逐次尝试 |
| 2 表头契约 | `app/contract.py` | 必需列缺失=错误；额外列 `reject`/`warn`；列顺序无关；表头去首尾空白；重复列名=错误 |
| 3 行级校验 | `app/rows.py` | 主键严格递增（重复 `duplicate` / 倒序 `out_of_order`，逐行与上一行比较）、int/date/decimal/enum、必填、长度上限、日期真实性 |
| 4 分组聚合 | `app/aggregate.py` | 按 `site` 求和，并用三条独立口径自证一致 |
| 5 事务导入 | `app/importer.py` | 单事务写 批次+明细+聚合；异常即 ROLLBACK；sha256 重复 → `duplicate_file`，可 `force_new_batch` |
| 6 错误报告 | `app/report.py` | 按错误类型汇总 + 逐行依据（行号/列名/原值/原因）+ 修正建议 |
| 7 分片清单与单段核对 | `app/manifest.py` | 以完整文件 sha256 钉死文件身份；核对段号、字节覆盖范围、总行数、按站点金额清单；段体 sha256 逐段校验 |
| 8 续传·隔离·原子发布 | `app/chunks.py` | 先到片段落暂存可续传；缺段/错调整批隔离并指出缺口；收齐后单事务切换明细+站点汇总+当前版本指针，失败保留上一版；重试不新增批次，旧版本不改写新版本 |

编排在 `app/pipeline.py`（整传）与 `app/chunks.py`（分片），路由 `app/main.py` 只做参数解析与渲染。

## 核心规则

1. **编码探测**（证据可查）
   - 前 3 字节是 `EF BB BF` → 按 `utf-8-sig` 解码，BOM 剥离；
   - 否则严格 UTF-8；失败再试 `gb18030`（GBK 超集）；
   - 都失败 → HTTP 400，附三次尝试的失败字节偏移与修复建议。
   - 判定是**确定性**的：纯 ASCII 文件一律算 `utf-8`。
2. **行级错误只报第一处**：同一行同一列按「必填 → 长度 → 类型/枚举 → 业务」取第一个失败原因，
   避免一个超长 site 值同时被报成长度错和枚举错、干扰计数。
3. **主键严格递增**：与「上一条主键可解析的行」比较；相等 → `duplicate`，更小 → `out_of_order`，更大 → 通过并更新基线。
4. **日期真实性**：先匹配 `YYYY-MM-DD`，再用 `date.fromisoformat` 判真实存在（`2026-02-30` → `date_invalid`）。
5. **聚合自证**：分组累加 cents、明细单趟线性累加、`Decimal` 原始字符串复核、组内行数和=明细行数，四条必须同时成立。
6. **事务与重复文件**
   - 全对：`BEGIN IMMEDIATE` 写批次 → 明细 → 聚合 → `COMMIT`；中途异常 `ROLLBACK`，明细行数保持写入前值。
   - 同 `sha256` 已入库且未勾选 `force_new_batch` → 返回 `duplicate_file`，写入 0 行，返回既有批号。
   - 勾选 `force_new_batch` → 新建批次（`-F2`/`-F3` 后缀），旧批次保留且 `superseded_by` 指向新批次，
     语义是「同一份文件被有意重灌，重灌痕迹可追溯」，不是覆盖删除。
7. **分片续传与文件身份（模块 7/8）**
   - 身份 = 完整文件 `sha256`（即 `file_id`）；manifest 同时声明 `filename/version/total_size/segment_size/
     total_segments/total_rows/amounts`，`total_size` 与分段数自相矛盾或段号越界直接 400。
   - 片段先落 `chunk_segments` + 暂存目录（`data/chunks/<file_id>/`），**未收齐绝不进入 `fact_orders`/聚合**；
     每段核对段号、覆盖范围（起止偏移与长度）、段体 sha256，坏段 409 拒收且坏字节不落暂存。
   - 缺段或来源错配 → 会话置 `quarantined`，响应给出 `missing_segments` 缺口段号与逐条 `mismatch` 依据；
     同段号收到不同来源内容会留 `segment_source_conflict`，正确来源补齐后才能发布。
   - 收齐后逐段复核暂存校验和 → 拼接整文件再算 sha256 与 manifest 比对（`whole_checksum` 是混传最终防线）
     → 复用模块 1–4 的纯函数校验 → 比对总行数/金额清单 → 单事务写 明细+聚合+`published_files` 指针。
   - **原子切换**：当前版本指针与明细/聚合同事务提交；任一步失败（含注入故障、`pre_commit_check` 守卫）
     整体 `ROLLBACK`，已发布的上一版与明细行数保持不变。
   - **幂等**：同 `file_id` 重开会话复用、同段重传覆盖不新增；已发布再 finalize 返回 `already_published`，不新增批次。
   - **版本守卫**：按版本自然序（`v2 < v10`）比较；较早版本的补传一律 `stale_version`（409）拒绝，
     会话标记 `stale_rejected`，不能改写已经发布的较新版本。

## 启动

```bash
cd batch-20261114b/projects/gb-623
cp .env.example .env          # 可选，默认端口 18212
docker compose up -d --build
curl -s http://127.0.0.1:18212/health
```

- 页面：<http://127.0.0.1:18212/>
- 数据卷：`csvgate-data:/app/data`（SQLite `csvgate.sqlite3`）
- 启动即幂等 seed：`meta.seed_version = seed-2026-11-07.1`，批次 `B-20261107-001`（60 行历史周批次）；
  重复启动不会重复写入。**重建数据库**：`docker compose down -v && docker compose up -d --build`。

## 演示步骤

1. `/workbench` 下载 `normal_500.csv` 并上传 → 结论「已入库」，得批号、站点聚合表、明细前 20 行。
2. 上传 `broken.csv` → HTTP 422，结论「整体回滚」，列出 5 条逐行错误（重复主键 / 倒序 / 非法日期 / 超长 / 枚举越界）。
   刷新首页或 `/health` 看 `fact_rows` 未变。
3. 上传 `gbk_120.csv` → 编码探测显示 `gb18030`，120 行入库。
4. 再上传一次 `normal_500.csv` → `duplicate_file`，写入 0 行；到 `/review` 点「验证重复导入」看只读判定。
5. 勾选 `force_new_batch` 再传 `normal_500.csv` → 新批次，旧批次标 superseded。
6. `/workbench` 的「分片续传」面板选同一文件 → 浏览器算 sha256 自动分片，可在中途断网点续传；
   缺段时 `/review` 看到 `quarantined` 与缺口段号，收齐点「核对并切换」后 `published_files` 出现当前版本。

命令行等价（验收脚本用的就是这些）：

```bash
curl -s http://127.0.0.1:18212/health
curl -s -F "file=@sample_data/normal_500.csv" -F "extra_policy=reject" http://127.0.0.1:18212/api/import
curl -s -o /tmp/broken.json -w '%{http_code}\n' -F "file=@sample_data/broken.csv" http://127.0.0.1:18212/api/import
python3 -c "import json;d=json.load(open('/tmp/broken.json'));print(d['report']['total'],[e['code'] for e in d['report']['rows']])"
```

分片续传的 `manifest`（JSON 字符串，作为表单字段 `manifest` 提交）：

```json
{
  "filename": "orders.csv",
  "version": "v1",
  "sha256": "<完整文件 64 位 sha256，同时作为 file_id>",
  "total_size": 22564,
  "segment_size": 4096,
  "total_segments": 6,
  "total_rows": 500,
  "amounts": [
    {"site": "beijing", "amount_cents": 2966292, "qty": 336, "row_count": 84}
  ]
}
```

逐段 `PUT /api/chunks/segment` 带 `index`（0 起）、该段 `checksum`（段体 sha256）与段字节；
全部收齐后 `POST /api/chunks/finalize`。`tests/test_chunks.py` 覆盖断网续传、两人混传隔离、
切换失败保留上一版、重试不新增批次、旧版本拦截五类场景。

## API / 页面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 + 当前明细行数 + seed 版本 |
| POST | `/api/import` | multipart：`file`、`extra_policy`、`force_new_batch`；200 已入库/重复文件、400 编码失败、422 校验拒绝 |
| POST | `/workbench` | 页面表单同管道；通过 → 303 跳结果页，拒绝 → 422 直接渲染错误表 |
| POST | `/api/contract-check` | 只检查表头（含 `warn` 策略演练） |
| POST | `/api/replay-check` | 按 sha256 判定「再次上传会不会重复」 |
| GET | `/api/batches` | 批次列表 + 统计 |
| POST | `/api/chunks/session` | multipart `manifest`：按文件身份打开/幂等复用续传会话 |
| PUT | `/api/chunks/segment` | multipart `manifest`+`index`+`checksum`+`file`：上传单段；坏段/来源不一致 409 并整批隔离 |
| POST | `/api/chunks/finalize` | 收齐后核对并一次性原子切换；缺段/错配/清单不符/旧版本 → 409 隔离，不动当前版本 |
| GET | `/api/chunks/session/{file_id}` | 会话状态：已收段、缺口段号、隔离原因与错配依据 |
| GET | `/api/published` | 当前对外发布的文件版本指针（一次切换只改这里，失败保留上一版） |
| GET | `/api/samples/{name}` | 样例下载 |
| 页面 | `/`、`/workbench`、`/result/{batch_id}`、`/result/attempt/{id}`、`/review` | 首页/工作台/结果/错误明细/历史复核（含分片会话与发布版本面板） |

## 目录

```text
app/            main.py settings.py pipeline.py seed.py
                encoding.py contract.py rows.py aggregate.py importer.py report.py store.py
                manifest.py chunks.py
                templates/ static/
sample_data/    normal_500.csv gbk_120.csv seed_20261107.csv broken.csv contract.json make_samples.py
tests/          test_encoding.py test_rows.py test_importer.py test_api.py test_chunks.py conftest.py
Dockerfile docker-compose.yml requirements.txt .env.example
```

## 已知限制

- CSV 解析用标准库 `csv.reader`：支持引号包裹字段，但**行号**按「数据行 + 1」换算，
  字段内含换行时行号会偏移（当前样例与常见导出不含换行字段）。
- 编码只覆盖 UTF-8 / UTF-8-BOM / GB18030；UTF-16/UTF-32 会走到 400 并给出提示，不做自动识别。
- 分隔符固定逗号，不做 `;`/`\t` 自动猜测；SJIS、Big5 等会按 GB18030 误判成功的概率存在（GBK 超集覆盖广），
  需要在契约里约定来源编码。
- 整份上传（`/api/import`）上限 8MB；大文件走 `/api/chunks/*` 分片续传，单段默认上限 2MB
  （`CSVGATE_MAX_CHUNK_BYTES` 可调），片段暂存于数据卷 `data/chunks/`，随 SQLite 命名卷持久化。
- 版本先后用版本标签的自然序判定；标签需可按数字/文本分段比较，纯随机字符串会退化为字典序，
  建议统一 `vN` 或日期格式。
- 明细与批次不提供删除接口；重灌靠 `force_new_batch` + `superseded_by` 追溯；
  分片会话的隔离记录保留在 `chunk_sessions`，用于人工复核缺口与来源错配。
