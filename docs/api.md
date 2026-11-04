# MessageCenter API 接口文档

本文对应合并 PR #4/#5 及 URL、可见性匹配、终态分页修正后的实现。接口基路径为 `/api/message-center`。时间字段采用 ISO 8601；本文示例中的地址、用户、原键和令牌均为占位值。

## 1. 调用与身份

业务请求统一携带 `Authorization: Bearer <access_token>`，JSON 请求另带 `Content-Type: application/json`。没有外层 `code/data` 包装，响应字段直接位于 JSON 根对象；认证失败可能返回无 JSON 正文的 401。

| 调用身份 | 接口范围 | 身份来源 |
|---|---|---|
| 已登记来源服务 | send、发送状态、retry、resolve-trigger、lifecycle、operations | JWT 的 preferred_username 经 SourceSystemNames 映射为来源值 |
| 人员账号 | my、summary、单条已读/未读、read-all | JWT 的 preferred_username 为本人 subscriberId |

服务端覆盖发送正文中的 sourceSystem；客户端不能自行指定可信来源或 workflow。来源值配置了 SourceServiceIdentities 时，issuer/sub/azp 必须精确匹配，aud 必须包含配置受众；显式空配置也拒绝。上述六类服务接口共用来源准入。未登记来源返回 403，身份约束失败返回 403/SOURCE_SERVICE_IDENTITY_DENIED。不能用人员令牌替代受保护来源的服务令牌。

个人消息接口不接受 subscriberId 查询参数指定他人。生命周期路径中的 subscriberId 由已认证来源按原 requestId 操作，不等于个人接口的身份选择方式。

## 2. 接口索引

以下业务路径均相对于 `/api/message-center`。

| 方法 | 路径 | 用途 | 正常响应 |
|---|---|---|---|
| POST | /send | 事务性受理发送 | 202 |
| GET | /send/{requestId} | 查询来源原请求 | 200；无记录 404 |
| POST | /send/{requestId}/retry | 重试 failed 收件人 | 202；无 failed 行 404 |
| POST | /send/{requestId}/resolve-trigger | 人工对账 uncertain | 200 |
| POST | /send/{requestId}/recipients/{subscriberId}/lifecycle | 消费、撤回、过期 | 202 |
| GET | /send/{requestId}/operations/{operationId} | 查询生命周期操作及外部补偿 | 200；无记录 404 |
| GET | /my | 本人消息分页 | 200；重建中 202 |
| GET | /summary | 本人消息总数/未读数 | 200；重建中 202 |
| POST | /messages/{messageId}/read | 标记本人消息已读 | 200 |
| POST | /messages/{messageId}/unread | 标记本人消息未读 | 200 |
| POST | /my/read-all | 创建全部已读任务 | 202 |
| GET | /my/read-all/{jobId} | 查询本人全部已读任务 | 200；无记录 404 |

## 3. 发送与原请求查询

### POST /send

```json
{
  "requestId": "origin-request-key",
  "businessType": "todo",
  "businessId": "business-item-id",
  "title": "待处理事项",
  "content": "请打开关联页面查看事项。",
  "url": "https://business.example.invalid/todos/open?ref=tdl_example",
  "receivers": [{ "type": "user", "id": "subscriber-id" }]
}
```

| 字段 | 要求 |
|---|---|
| requestId | 必填，最长 128，调用来源负责生成稳定原键 |
| businessType | 必填，最长 128 |
| businessId | 可空 |
| title | 必填，最长 500 |
| content | 可空 |
| url | 可空；保留发送方原始 URL，不应预先 HTML 实体化 |
| receivers | 必填，至少一项；type/id 必填，id 最长 128 |

当前只处理 type=user，类型不区分大小写；相同类型和 id 去重。其他类型列入 skipped；没有任何可处理收件人时返回 400。来源对应 workflow 为空时返回 503/SOURCE_WORKFLOW_UNAVAILABLE。

202 示例：

```json
{
  "requestId": "origin-request-key",
  "state": "pending",
  "recipients": [{
    "sendId": "stable-send-id",
    "subscriberId": "subscriber-id",
    "state": "pending",
    "retryCount": 0,
    "transactionId": null,
    "error": null,
    "triggerAttempts": 0,
    "uncertainAt": null,
    "uncertainResolution": null,
    "businessState": null,
    "businessVersion": null,
    "terminalAt": null,
    "terminalReason": null
  }],
  "skipped": []
}
```

202 只表示 DM8 事务已受理，不代表 Novu 已送达。相同来源/requestId、相同业务载荷和收件人集合重放复用原行及 sendId；同键异载荷返回 409。不要因超时、5xx 或查询暂时 404 而换 requestId 重发。

### GET /send/{requestId}?include=terminal

返回 requestId、聚合 state 和 recipients，结构与发送响应一致但没有 skipped。include 可省略；仅 include=terminal 时填充 recipients 中的 businessState/businessVersion/terminalAt/terminalReason，否则这些字段为 null。没有本来源记录返回 404，不能把 404 当作外部平台永久未发送的证明。

| 收件人 state | 含义 |
|---|---|
| pending | 待 Worker 处理 |
| accepted | 已取得平台受理回执，等待消息及投影可见 |
| visible | 消息已进入有效 Redis 投影，不等于人员已读或业务已处理 |
| failed | 达到失败重试上限 |
| uncertain | 触发结果不确定，禁止自动重新 POST |
| suppressed | 业务已终态，抑制有效投递 |

聚合 state：全部 visible 时为 visible；否则依次优先 uncertain、failed、suppressed、accepted，最后为 pending。混合收件人结果仍需逐个查看 recipients。

triggerAttempts 表示持久记录的触发尝试/意图次数，不是成功送达次数；transactionId 为已记录平台事务关联键。Worker 使用原 sendId 作为 transactionId 和 Idempotency-Key。

### POST /send/{requestId}/retry

不需要请求正文。只将本来源原请求的 failed 行恢复 pending，并沿用原 sendId/载荷。不会恢复 uncertain；没有 failed 行返回 404。202 正文为更新后的发送状态。

### POST /send/{requestId}/resolve-trigger

```json
{ "sendId": "stable-send-id", "resolution": "CONFIRM_SENT" }
```

仅允许原来源按 requestId/sendId 对账 uncertain：

- CONFIRM_SENT：按原键进入 accepted，继续可见性核对，不重新触发发送。
- CONFIRM_NOT_SENT：调用方必须先核实原请求确实未发送；恢复原行 pending，业务已经终态的行则保持 suppressed。缓存过期或没有查到消息不是未发送证明。

有效 Worker 租约尚未结束、行不为 uncertain 或重复收敛返回 409；本来源原键不存在返回 404；非法 resolution/空 sendId 返回 400；乐观并发冲突返回 503/MESSAGE_CONCURRENT_UPDATE，应查询原键后重试同一对账操作。

可选 note 字段当前不参与状态裁决，也不作为持久审计凭证；对账证据应由调用方另行保存。200 返回原请求当前状态。该接口不会延长 Novu 幂等缓存，也不提供永久去重保证。

## 4. 生命周期与外部补偿

### POST /send/{requestId}/recipients/{subscriberId}/lifecycle

```json
{
  "operationId": "stable-operation-key",
  "action": "REVOKE",
  "businessVersion": 2,
  "reason": "原业务事项已撤回",
  "occurredAt": "2026-01-01T00:00:00Z"
}
```

operationId 必填，最长 128；action 必须为大写 CONSUME、REVOKE 或 EXPIRE；businessVersion 必须为正整数且大于原收件人当前版本；reason 可空，最长 2000。occurredAt 可省略，缺省取服务端受理时间；它是信息性时间，不参与幂等指纹。

幂等范围是来源/operationId。同 operationId、相同 requestId/subscriberId/action/businessVersion/归一化 reason 重放返回原操作；异载荷返回 409。reason 会去除首尾空白，空白视为 null。对不同业务动作应使用新 operationId 和更高 businessVersion。

| action | 业务状态 | 外部动作 |
|---|---|---|
| CONSUME | CONSUMED | 按原消息标记已读，并移出有效收件箱 |
| REVOKE | REVOKED | 删除原消息，并移出有效收件箱 |
| EXPIRE | EXPIRED | 删除原消息，并移出有效收件箱 |

CONSUME 在原收件人行不存在时返回 404。REVOKE/EXPIRE 可先于 send 按原键保存 deferred 终止标记；迟到 send 会套用最高业务版本、进入 suppressed，不产生有效通知。版本不递增返回 422；并发冲突返回 503/MESSAGE_CONCURRENT_UPDATE，使用同一原操作查询/重试。

202 响应字段：operationId、requestId、subscriberId、action、businessVersion、status、businessState、sendId、occurredAt、recordedAt、externalState。status 为 applied、deferred 或 replayed；sendId 在发送尚未到达时可空。

本地业务终态与外部补偿任务共同提交，202 不代表外部删除/已读已经完成。外部故障不撤销业务终态；Worker 保存目标消息 ID 和分页进度，沿用原键补偿，不再次调用 trigger。跨批次扫描完整结束前不删除消息、不确认外部收敛。

### GET /send/{requestId}/operations/{operationId}

返回操作字段：operationId、requestId、subscriberId、sendId、action、businessVersion、reason、applyStatus、resultBusinessState、occurredAt、recordedAt、appliedAt，以及 externalState、lastExternalConfirmedAt、externalError。无本来源/原请求对应操作返回 404。

| externalState | 含义 |
|---|---|
| waiting-for-send | deferred 操作尚未关联原发送行 |
| pending | 外部同步等待、失败退避或分页扫描中 |
| watching | 本轮找到并处理目标，仍周期检查迟到消息 |
| not-required | 有持久证据表明受理时即被抑制、未触发 |
| superseded | 已被更高业务版本替代 |
| not-tracked | 历史操作没有对应补偿任务，不能认定外部已成功 |

lastExternalConfirmedAt 是最近一次成功检查的观察时间，不是未来不会出现迟到消息的保证。externalError 是安全错误码。读取操作记录不会触发补偿或发送。

## 5. 本人收件箱

### GET /my?view=all&page=1&limit=100

view 为 all、unread 或 read（不区分大小写），缺省 all；page 从 1 开始；limit 为 1～100，缺省 100。非法值返回 400。过滤和分页在服务端执行，totalCount 是当前视图的总数，unreadCount 是本人全局未读数，均不能用当前页条数代替。

200 示例：

```json
{
  "state": "ready",
  "items": [{
    "messageId": "message-id",
    "sourceSystem": "source-name",
    "businessType": "todo",
    "businessId": "business-item-id",
    "title": "待处理事项",
    "content": "请打开关联页面查看事项。",
    "url": "https://business.example.invalid/todos/open?ref=tdl_example",
    "read": false,
    "seen": false,
    "createdAt": "2026-01-01T00:00:00Z"
  }],
  "page": 1,
  "limit": 100,
  "totalCount": 1,
  "unreadCount": 1
}
```

url 优先取发送方原 payload.url；仅 null/缺失时回退 cta.data.url。空字符串仍表示无关联页面。API 不做 HTML 解码或 URL 重编码；调用方应保留 `?ref=` 等查询结构，只按需要编码参数值，不能把分隔符 `=` 改为 `%3D`。该字段保真不代表目标业务链接仍有效，也不代表已经通过协议/域名白名单校验。

### GET /summary

200 返回 `{ "state": "ready", "totalCount": 1, "unreadCount": 1 }`；totalCount 为本人全部有效消息总数。

### 投影构建与 p10 升级

缓存缺失、终态版本过时或旧投影格式失效时，my/summary 返回 202/state=building。my 的 items 为空、计数暂为 0；summary 同样暂返回 0。它们表示尚未就绪，不表示本人没有消息。前端稍后继续请求同一接口，不切换到废弃的 unread-count 契约。

p10 自动拒绝旧 p9 就绪值，由 Worker 从 Novu 重建，修复历史实体化 CTA 的公开 URL；不需要重新发送消息、修改历史消息或清空业务表。重建不更改 Novu 已读状态。API 与 Worker 必须统一升级，不能混跑旧格式写入者。

终态消息按原 sendId/事务键和本人收件人记录过滤；已终态的旧缓存不能在重建后重新进入有效收件箱。无法绑定本人原发送记录的 micip 消息暂不展示；其他来源的未关联历史消息保留兼容行为。

### POST /messages/{messageId}/read 或 /unread

不需要正文。200 返回 `{ "messageId": "message-id", "read": true }`，unread 时 read 为 false。先更新 Novu，再更新投影；投影尚未就绪时返回 409。该接口表示消息阅读状态，不等价于 CONSUME 业务处理。

### POST /my/read-all 与 GET /my/read-all/{jobId}

read-all 不需要正文；投影未就绪返回 409，否则 202 返回 `{ "jobId": "job-id", "state": "queued" }`。同一人员已存在进行中的全部已读任务时复用该任务。

任务查询 200 返回 jobId、state、total、processed、error。state 为 queued、running、retrying、completed 或 failed；仅本人任务可见，无记录返回 404。Worker 每批处理 100 条，写入并发 8；processed 为任务处理进度，不能替代 summary 的正式计数。

## 6. 错误处理

| HTTP | 常见含义 | 调用方处理 |
|---|---|---|
| 400 | 必填字段、长度、动作或分页参数不合法 | 修正请求，保留业务原键 |
| 401 | 未认证、令牌无效或缺少 preferred_username | 修正人员/服务认证 |
| 403 | 来源未登记或 SOURCE_SERVICE_IDENTITY_DENIED | 核对来源映射及服务身份绑定，不借用其他来源 |
| 404 | 本来源原键/人员任务不存在，或没有 failed 行 | 核对原键，不推断外部永久未发送 |
| 409 | 原键异载荷、状态不允许对账、投影尚未就绪 | 查原状态；同键重放不得换业务内容 |
| 422 | 生命周期业务版本不大于当前值 | 查询当前版本后进行业务裁决 |
| 502 | 同步 Novu 请求失败，可能带 novuStatus | 保留原键并查询实际状态 |
| 503 | 来源工作流为空或 MESSAGE_CONCURRENT_UPDATE；依赖也可能不可用 | 修正配置或按原操作查询/退避重试 |
| 504 | 同步 Novu 请求超时 | 超时不等于未生效，先查原状态 |

数据标注验证的 400 使用 ASP.NET Core 验证响应；业务分支多返回 error，部分分支另有 code/currentVersion/attemptedVersion 等字段，不保证统一错误包装。

## 7. 健康检查与调用示例

健康路径位于 API 根目录，不加 `/api/message-center`，当前实现不要求业务 Bearer Token：

| GET 路径 | 200 条件 | 503 条件 |
|---|---|---|
| /health | API 进程可响应 | 不承担依赖检查 |
| /health/outbox | DM8 CanConnect 成功 | DM8 连接检查失败 |
| /health/worker | 最近 30 秒收到独立 Worker 心跳 | 心跳缺失或超时 |

健康成功不代表通知已送达、已读或业务已处理。

```bash
curl -H "Authorization: Bearer $MC_ACCESS_TOKEN" \
  "$MC_BASE_URL/api/message-center/my?view=unread&page=1&limit=100"

curl -H "Authorization: Bearer $MC_SERVICE_TOKEN" \
  "$MC_BASE_URL/api/message-center/send/$REQUEST_ID?include=terminal"
```

部署地址、来源登记、服务受众、订阅者集合和工作流由环境配置提供，文档示例不构成实际来源授权或测试发送许可。本版本接口能力与联调的完整 M8-5 业务验收结果应分别记录。
