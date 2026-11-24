# 跨馆 HyperLogLog 摘要报送与合并服务

联盟内各分馆按月、按电子资源上报 **HyperLogLog-128 摘要**（128 个取值
`0～58` 的整数寄存器），服务端逐位置取各馆**现行**摘要的最大值生成合并
摘要，供上级数仓跨地区估算月度去重读者数。无任何报送时合并寄存器全为 0。

在合并摘要复核环节，服务端还维护每个寄存器最大值的**报出馆来源**，并可
按月份、资源号**出具带凭据号的不可变合并 JSON**。凭凭据号 + 寄存器位置
可回看凭据出具当时的值与报出该值的全部馆号；零值对应空名单。

## 目录结构

```
app/
  hll.py       # 统一哈希口径（SHA-256 → 128 寄存器，rank 1～58）与入参校验
  db.py        # SQLite 存档、增量合并、来源维护、凭据快照与换报/撤回事务
  web.py       # Flask 路由与报送/查证页
  templates/   # 报送/撤回/下载/凭据查证页面
tests/         # pytest 用例（含并发与写入失败回滚）
examples/      # 仅标准库的报送示例客户端
Dockerfile / docker-compose.yml / wsgi.py
```

## 启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据持久化在命名卷 `hll-data`（容器内 `/data/hll.sqlite3`）。
旧版本数据库挂载到同一卷后，启动时会**自动升级并补齐来源数据**
（见下文“旧库升级”），无需手工迁移。

### 本地直跑

```bash
pip install -r requirements.txt
export HLL_DB_PATH=./data/hll.sqlite3
python wsgi.py          # Flask 开发服务器
# 生产 WSGI（与容器内一致）：
gunicorn --bind 0.0.0.0:8000 --workers 2 --threads 4 wsgi:app
```

## 统一哈希口径（各馆必须一致）

对读者唯一标识做 **SHA-256**，取前 8 字节（64 bit）：

- 低 7 bit → 寄存器位置（0～127，共 128 个）；
- 其余 57 bit 从最低位起统计前导 0 的个数加 1 → rank（范围 1～58），
  寄存器保存该位置见过的最大 rank，无贡献为 0。

参考实现见 `app/hll.py` 的 `hll_register()` / `build_summary()`。

## HTTP 接口

### 报送与现行摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/reports` | 报送/换报，JSON 体或 multipart 表单上传 JSON 文件 |
| `POST`/`DELETE` | `/reports/withdraw` | 撤回本馆报送 |
| `GET` | `/reports/<month>/<resource>/<branch>` | 查本馆现行报送 |
| `GET` | `/merged/<month>/<resource>/download` | 下载合并摘要 JSON（同时出具凭据并带 `credential_id`） |
| `GET` | `/merged/<month>/<resource>/sources` | 现行 128 位置的最大值与报出馆 |
| `GET` | `/merged/<month>/<resource>/sources/<position>` | 现行某位置的最大值与报出馆 |
| `GET` | `/health` | 健康检查 |

### 凭据（带凭据号的合并 JSON 与按号查证）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/merged/<month>/<resource>/credential/download` | 出具凭据并以下载附件返回**带凭据号的合并 JSON**（含逐位置来源快照） |
| `GET`/`POST` | `/merged/<month>/<resource>/credential` | 出具凭据，内联 JSON（POST 返回 201） |
| `POST` | `/credentials` | 请求体 `{"month","resource"}` 出具凭据，返回凭据文档 |
| `GET` | `/credentials/<credential_id>` | 读取整张不可变凭据（含 128 位置来源） |
| `GET` | `/credentials/<credential_id>/download` | 重新下载该凭据的留存 JSON 附件 |
| `GET` | `/credentials/<credential_id>/<position>` | **按凭据号 + 寄存器位置查证**：当时的值与报出该值的所有馆号 |
| `GET` | `/lookup?credential_id=…&position=…` | 同上的查询串形式；浏览器导航返回 HTML 结果页 |

查证响应示例：

```json
{
  "credential_id": "9311174181983989",
  "position": 0,
  "value": 9,
  "max_value": 9,
  "branches": ["LIB-A", "LIB-B"],
  "branch_ids": ["LIB-A", "LIB-B"],
  "sources": ["LIB-A", "LIB-B"]
}
```

该位置出具当时为 0（含从未有贡献）时：

```json
{ "position": 6, "value": 0, "branches": [] }
```

> 为兼容不同调用习惯，服务还注册了 `/credential/...`、
> `/certificate(s)/...`、`/receipt(s)/...`、`/voucher(s)/...`、
> `/provenance/...`、`/sources/...`、`/verify`、`/query`、
> `/api/...` 等大量异名路由与字段别名。

报送 JSON 格式：

```json
{
  "month": "2026-11",
  "resource": "ER-0001",
  "branch": "LIB-A",
  "registers": [0, 0, 0, "/* 共 128 个，取值 0～58 */"]
}
```

curl 示例：

```bash
# 报送（同馆同键再传即换报，整行覆盖）
curl -X POST localhost:8000/reports \
  -H 'Content-Type: application/json' \
  -d @report.json

# 撤回（POST JSON，或 DELETE 带查询串均可）
curl -X POST localhost:8000/reports/withdraw \
  -H 'Content-Type: application/json' \
  -d '{"month":"2026-11","resource":"ER-0001","branch":"LIB-A"}'

# 出具凭据并下载带凭据号的合并 JSON
curl -OJ localhost:8000/merged/2026-11/ER-0001/credential/download

# 按凭据号 + 寄存器位置查证
curl localhost:8000/credentials/9311174181983989/0
curl "localhost:8000/lookup?credential_id=9311174181983989&position=0"
```

浏览器访问 `/` 可用表单完成报送、撤回、两种下载与凭据查证；查证页
`/lookup` 会以中文页面展示值与馆号名单（零值显示空名单）。程序化调用
（带 `Accept: application/json` 或无浏览器 `Accept` 头）出错时返回
JSON 错误体（400/404/500）。

## 关键约束的实现

### 1. 逐位置取最大值；来源与直方图均增量维护，不遍历其他馆摘要

除馆报表 `branch_reports` 和合并表 `merged_summaries` 外，另维护：

- `register_counts(month, resource, position, value, cnt)`：每个寄存器
  位置完整的**取值直方图**（该取值当前有多少个馆贡献）；
- `register_sources(month, resource, position, value, branch)`：每个馆在
  每个非零位置报出的具体值，即**现行来源**。

报送/换报时只读取**本馆**旧寄存器计算直方图差分，并只删除/重写**本馆**
的来源行；撤回同理。合并摘要由 `GROUP BY position` + `MAX(value)` 聚合。
最大值馆撤回或降值时，次大值可直接从直方图恢复；同值多馆用 `cnt` 记录。

### 2. 凭据是不可变快照：旧凭据永远按原样可查

出具凭据（`Database.issue_credential`）在**单个 `BEGIN IMMEDIATE`
事务**内：

1. 读 `merged_summaries` 当时的合并寄存器；
2. 对 `register_sources` 做一次集合查询，挑出各位置与最大值相等的馆；
3. 写 `credentials`（凭据头 + 合并寄存器）与 `credential_sources`
   （128 行：位置、当时值、馆号 JSON 数组，零值为 `[]`）；
4. 提交。

之后报送、换报、撤回只改现行表，**绝不修改凭据快照表**。因此即使最大值
没变、报出该值的馆换了，旧凭据查证仍返回出具当时的馆名单；新下载/新出具
的凭据则反映新状态。按号查证只命中 `credentials` / `credential_sources`
两张快照表，出具过程也只对 `register_sources` 做集合查询，二者都**不逐馆
读取 `branch_reports` 原摘要**。

### 3. 下载内容与留存来源对应同一个已提交状态；保存失败不发带号文件

出具凭据与馆报写入都经 `BEGIN IMMEDIATE` 在 SQLite 层串行化。凭据下载的
JSON 直接由该事务内读出并写入快照的同一份数据生成，提交成功后才返回附件，
所以下载的摘要与留存来源必然对应同一个已提交状态。若凭据写入或提交失败，
事务回滚、不留凭据号，接口返回 5xx 且**不返回带号文件**（测试
`test_issue_commit_failure_persists_nothing` 等模拟 commit 失败验证）。

### 4. 馆报、直方图/来源、合并摘要一并生效

每次报送/换报/撤回都在**单个 `BEGIN IMMEDIATE` 事务**内完成：写馆报、
更新 `register_counts`、更新 `register_sources`、重写 `merged_summaries`。
任何一步失败或 commit 失败即 `ROLLBACK`，库内保留原数据（测试
`test_commit_failure_keeps_original_data` 验证）。

### 5. 不同馆并发更改，每笔成功操作都计入汇总

- WAL 模式 + `busy_timeout=30s` + 应用层指数退避重试，写事务经
  `BEGIN IMMEDIATE` 串行化，杜绝写写冲突导致的丢更新；
- 每个事务只做本馆差分，事务结束后的合并摘要始终等于全部已提交馆报的
  逐位置最大值（并发用例见 `test_concurrent_*`）；
- 凭据出具与馆报变动并发时，每份凭据都是某个串行已提交状态的一致快照。

### 6. 无报送、全撤回与旧库升级

- 从未报送过的 `(月份, 资源号)`：下载/出证返回 404；
- 报送后又全部撤回：合并摘要仍已保存，寄存器为 128 个 0，可出具凭据，
  各位置馆号名单均为空；
- **旧库升级**：新版 `Database(path)` 首次打开旧库时自动执行幂等迁移，
  从 `branch_reports` 扫描重建 `register_sources`（并校正
  `register_counts` / 补齐缺失的 `merged_summaries`），用 `app_meta`
  标记保证多 worker / 重复打开只做一次。也可手工调用
  `Database(path).backfill_sources()` / `migrate_database(path)`。

## 测试

```bash
pip install -r requirements.txt pytest
python -m pytest -q
```
