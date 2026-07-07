# MessageCenter API

ASP.NET Core 6 Web API。它以 Novu 作为应用内消息的权威数据源，并对外暴露一套简洁的、面向业务的消息中心 API。

## 当前环境

| 项目 | 值 |
|---|---|
| .NET SDK | 6.x |
| Novu Dashboard | `http://192.168.124.2:4000` |
| Novu API | `http://192.168.124.2:13000` |
| Novu WS | `http://192.168.124.2:13002` |
| Novu 工作流触发器 | `system-notification` |
| Novu 通道 | `in_app` |
| Keycloak 认证服务器 | `http://192.168.124.2:18085/realms/master` |
| 测试客户端 | `cooper` |
| 测试用户 | `196045` |

`system-notification` 的 in-app 模板必须使用以下变量：

```text
{{payload.title}}
{{payload.content}}
{{payload.url}}
```

## 配置

当前内网配置已提交在 [appsettings.json](MessageCenter.Api/appsettings.json) 中：

```json
{
  "Novu": {
    "BaseUrl": "http://192.168.124.2:13000",
    "ApiKey": "13452c72c03e51f5da2433a989008e67",
    "DefaultWorkflowId": "system-notification",
    "InAppChannel": "in_app",
    "TimeoutSeconds": 10
  },
  "Jwt": {
    "Authority": "http://192.168.124.2:18085/realms/master",
    "RequireHttpsMetadata": false
  }
}
```

仍可通过环境变量覆盖配置：

```powershell
$env:Novu__ApiKey="13452c72c03e51f5da2433a989008e67"
$env:Jwt__Authority="http://192.168.124.2:18085/realms/master"
$env:Jwt__RequireHttpsMetadata="false"
```

## 运行

```powershell
dotnet build .\MessageCenter.Api\MessageCenter.Api.csproj
dotnet run --project .\MessageCenter.Api\MessageCenter.Api.csproj --urls http://localhost:5000
```

健康检查：

```bash
curl http://localhost:5000/health
# {"status":"ok"}
```

## 认证

所有 `/api/message-center/*` 接口都需要携带 `Authorization: Bearer <token>` 请求头。

API 会从 Keycloak Token 中读取 `preferred_username` 字段：

- 发送接口：`preferred_username` 作为 `sourceSystem` 使用。
- 读取接口：`preferred_username` 作为 Novu 的 `subscriberId` 使用。
- 请求体中的 `sourceSystem` 为可选字段，若传入则会被忽略。

### Keycloak 获取 JWT 完整链路

Token 地址：

```text
POST http://192.168.124.2:18085/realms/master/protocol/openid-connect/token
Content-Type: application/x-www-form-urlencoded
```

cooper 客户端参数：

```text
client_id=cooper
client_secret=oEllGz2IOsrMY07YnY1hZo6RzrFBZnjD
```

获取 cooper 服务账号 JWT。该 Token 的 `preferred_username` 为 `service-account-cooper`，用于调用发送接口；服务账号已在 `SourceSystemNames` 中映射为 `协同工作平台`：

```bash
TOKEN=$(curl -s -X POST \
  http://192.168.124.2:18085/realms/master/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=cooper" \
  -d "client_secret=oEllGz2IOsrMY07YnY1hZo6RzrFBZnjD" \
  | jq -r .access_token)
```

获取测试用户 JWT。该 Token 的 `preferred_username` 为 `196045`，用于查询自己的消息、未读数、标记已读/未读、一键已读：

```bash
TOKEN=$(curl -s -X POST \
  http://192.168.124.2:18085/realms/master/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=cooper" \
  -d "client_secret=oEllGz2IOsrMY07YnY1hZo6RzrFBZnjD" \
  -d "username=196045" \
  -d "password=cacjszx.132" \
  | jq -r .access_token)
```

调用 MessageCenter API 时统一放入请求头：

```bash
-H "Authorization: Bearer $TOKEN"
```

## 接口列表

### 发送消息

`POST /api/message-center/send`

```bash
curl -X POST http://localhost:5000/api/message-center/send \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "businessType": "process_task",
    "businessId": "TASK_001",
    "title": "您有一条新的工作流任务",
    "content": "请处理检验审批工作流",
    "url": "/process/tasks/TASK_001",
    "receivers": [{ "type": "user", "id": "196045" }]
  }'
```

**响应 `201`**：

```json
{
  "transactionId": "txn_xxx",
  "status": "processed",
  "acknowledged": true,
  "accepted": ["196045"],
  "skipped": []
}
```

MVP 阶段仅支持 `receivers[].type == "user"` 的接收者，其他类型会被跳过。若所有接收者都被跳过，API 将返回 `400` 错误。

### 我的消息

`GET /api/message-center/my?page=1&limit=100&read=false`

`page` 对外按业务习惯从 `1` 开始计数；API 内部会转换为 Novu 的 `0` 起始页码。

`read` 为可选过滤条件：

- 不传：查询全部消息。
- `read=false`：只查询未读消息。
- `read=true`：只查询已读消息。

当传入 `read` 过滤条件时，API 会先分页拉取当前用户全部消息，再按 `read` 状态过滤并对过滤后的结果做业务分页，避免只过滤 Novu 当前页导致漏掉后续页消息。

```bash
curl "http://localhost:5000/api/message-center/my?page=1&limit=10&read=false" \
  -H "Authorization: Bearer $TOKEN"
```

**响应 `200`**：

```json
[
  {
    "messageId": "6a27771843f156a3e5a9891f",
    "sourceSystem": "协同工作平台",
    "businessType": "process_task",
    "businessId": "TASK_001",
    "title": "您有一条新的工作流任务",
    "content": "请处理检验审批工作流",
    "url": "/process/tasks/TASK_001",
    "read": false,
    "seen": false,
    "createdAt": "2026-06-09T01:45:42.993Z"
  }
]
```

### 未读数量

`GET /api/message-center/unread-count`

```bash
curl http://localhost:5000/api/message-center/unread-count \
  -H "Authorization: Bearer $TOKEN"
```

**响应 `200`**：

```json
{
  "unreadCount": 4
}
```

未读数量通过分页调用 Novu `GET /v1/messages?page=&limit=100&pageSize=100` 得出：每页查询 100 条，并根据 Novu 返回的 `hasMore` 自动翻页，直到没有更多数据后统计 `read == false` 的条目。标记已读/未读后的 `unreadCount` 回查也使用同一套全量分页统计逻辑。

### 标记已读

`POST /api/message-center/messages/{messageId}/read`

```bash
curl -X POST http://localhost:5000/api/message-center/messages/6a27771843f156a3e5a9891f/read \
  -H "Authorization: Bearer $TOKEN"
```

**响应 `200`**：

```json
{
  "messageId": "6a27771843f156a3e5a9891f",
  "read": true,
  "unreadCount": 3
}
```

### 标记未读

`POST /api/message-center/messages/{messageId}/unread`

```bash
curl -X POST http://localhost:5000/api/message-center/messages/6a27771843f156a3e5a9891f/unread \
  -H "Authorization: Bearer $TOKEN"
```

**响应 `200`**：

```json
{
  "messageId": "6a27771843f156a3e5a9891f",
  "read": false,
  "unreadCount": 4
}
```

### 一键已读

`POST /api/message-center/my/read-all`

将当前登录用户的所有未读消息标记为已读。

```bash
curl -X POST http://localhost:5000/api/message-center/my/read-all \
  -H "Authorization: Bearer $TOKEN"
```

**响应 `200`**：

```json
{
  "markedCount": 29,
  "unreadCount": 0
}
```

`markedCount` 表示本次实际提交给 Novu 标记已读的未读消息数量；`unreadCount` 为标记后重新回查得到的未读数量。

## 错误处理

| 状态码 | 含义 |
|---|---|
| `400` | 参数校验失败，或没有有效的用户接收者 |
| `401` | JWT 缺失/无效，或 Token 中缺少 `preferred_username` |
| `502` | Novu 请求失败，响应中包含 `novuStatus` |
| `504` | Novu 请求超时 |

示例：

```json
{
  "error": "Novu request failed.",
  "novuStatus": 0
}
```

## 审计日志

每次成功触发接收者时，会写入一条结构化审计日志：

```text
AUDIT send. TransactionId=txn_xxx SourceSystem=196045 BusinessType=process_task BusinessId=TASK_001 SubscriberId=196045 NovuHttpStatus=200 Status=processed Acknowledged=True Timestamp=...
```

`IAuditSink` 是扩展点。若需将审计记录持久化到数据库，可实现 `DbAuditSink : IAuditSink`，并在 `Program.cs` 中替换依赖注入注册。

## 使用的 Novu 接口

本 API 仅使用以下 Novu 接口：

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/v1/events/trigger` | 发送通知 |
| `GET` | `/v1/messages?page=&limit=&pageSize=` | 查询消息列表并计算未读数 |
| `POST` | `/v1/subscribers/{subscriberId}/messages/mark-as` | 标记已读/未读 |

**重要说明**：
- `GET /v1/messages` 返回的字段中，标题为 `subject`，内容为 `content`，跳转链接为 `cta.data.url`。
- 标记已读/未读使用请求体 `{ "messageId": "...", "markAs": "read" }` 或 `{ "messageId": "...", "markAs": "unread" }`。
- 基于 subscriber 的通知 feed / unseen 接口未经验证，本 API 未使用。

## 项目结构

```text
MessageCenter.Api/
  Audit/
    IAuditSink.cs
    LoggerAuditSink.cs
  Controllers/
    MessageCenterController.cs
  HttpClients/
    NovuClient.cs
    Dtos/
      NovuDtos.cs
  Middleware/
    NovuExceptionMiddleware.cs
  Models/
    SendMessageRequest.cs
    SendMessageResponse.cs
  Options/
    NovuOptions.cs
  Services/
    MessageMapper.cs
    NovuTriggerPayload.cs
  appsettings.json
  Program.cs
```

## MVP 冒烟测试

```bash
BASE=http://localhost:5000

TOKEN=$(curl -s -X POST \
  http://192.168.124.2:18085/realms/master/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=cooper" \
  -d "client_secret=oEllGz2IOsrMY07YnY1hZo6RzrFBZnjD" \
  -d "username=196045" \
  -d "password=cacjszx.132" \
  | jq -r .access_token)

curl -s -X POST "$BASE/api/message-center/send" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"businessType":"process_task","businessId":"TASK_SMOKE_001","title":"Smoke test","content":"Hello","url":"/tasks/TASK_SMOKE_001","receivers":[{"type":"user","id":"196045"}]}'

curl -s "$BASE/api/message-center/my?page=1&limit=10" \
  -H "Authorization: Bearer $TOKEN"

curl -s "$BASE/api/message-center/my?page=1&limit=10&read=false" \
  -H "Authorization: Bearer $TOKEN"

curl -s "$BASE/api/message-center/unread-count" \
  -H "Authorization: Bearer $TOKEN"

curl -s -X POST "$BASE/api/message-center/my/read-all" \
  -H "Authorization: Bearer $TOKEN"
```
