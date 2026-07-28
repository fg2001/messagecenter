# MessageCenter API

ASP.NET Core 6 API，使用 Novu 保存消息正文和最终读写状态，使用 Redis 保存可重建的查询投影与后台任务。Redis 不是第二消息源；投影可以从 Novu 重新构建。

## 运行拓扑

| 服务 | 地址 |
|---|---|
| MessageCenter API | `http://localhost:5000` |
| Novu API | `http://192.168.124.2:13000` |
| Keycloak | `http://192.168.124.2:18085/realms/master` |
| Redis | `192.168.124.2:16379`，DB 7 |

API 与后台任务 Worker 在同一个 ASP.NET Core 进程中启动。没有第二个可执行程序或第二个启动入口。

```powershell
dotnet build .\MessageCenter.Api\MessageCenter.Api.csproj
dotnet run --project .\MessageCenter.Api\MessageCenter.Api.csproj --urls http://localhost:5000
```

健康检查：

```bash
curl http://localhost:5000/health
```

## 状态与一致性

- Novu 是消息正文及最终已读状态的权威数据源。
- Redis 保存消息轻量投影、`all/unread/read` 有序索引、后台任务和任务状态。
- 查询只访问 Redis，不在 HTTP 请求中遍历 Novu 全部消息。
- Redis 投影不存在时，查询返回 HTTP 202 和 `state=building`，同进程 Worker 从 Novu 重建投影。
- 新消息只能通过 `POST /send` 进入正式链路；该接口会登记订阅者并触发增量同步。直接绕过 MessageCenter 调用 Novu 不属于受支持的生产契约。

## 鉴权

所有 `/api/message-center/*` 接口要求：

```text
Authorization: Bearer <token>
```

发送使用已注册服务账号。个人查询和状态操作从 JWT 的 `preferred_username` 解析订阅者，不接受客户端传入 `subscriberId`。

## API 契约

### 发送消息

`POST /api/message-center/send`

```json
{
  "businessType": "process_task",
  "businessId": "TASK_001",
  "title": "待办提醒",
  "content": "请处理审批任务",
  "url": "/process/tasks/TASK_001",
  "receivers": [
    { "type": "user", "id": "196045" }
  ]
}
```

成功返回 HTTP 201。Novu 工作流异步处理消息，201 表示触发已被接受。

### 查询消息

`GET /api/message-center/my?view=all&page=1&limit=100`

`view` 必须是 `all`、`unread` 或 `read`。分页和状态过滤全部在服务端 Redis 投影完成。

投影就绪时返回 HTTP 200：

```json
{
  "state": "ready",
  "items": [
    {
      "messageId": "message-id",
      "sourceSystem": "协同工作平台",
      "businessType": "process_task",
      "businessId": "TASK_001",
      "title": "待办提醒",
      "content": "请处理审批任务",
      "url": "/process/tasks/TASK_001",
      "read": false,
      "seen": false,
      "createdAt": "2026-07-28T06:00:00Z"
    }
  ],
  "page": 1,
  "limit": 100,
  "totalCount": 100000,
  "unreadCount": 50000
}
```

投影构建中返回 HTTP 202：

```json
{
  "state": "building",
  "items": [],
  "page": 1,
  "limit": 100,
  "totalCount": 0,
  "unreadCount": 0
}
```

### 消息摘要

`GET /api/message-center/summary`

用于菜单角标和首页统计，返回正式全量未读数，不是当前页未读数：

```json
{
  "state": "ready",
  "totalCount": 100000,
  "unreadCount": 50000
}
```

投影构建中同样返回 HTTP 202 和 `state=building`。

### 单条已读和未读

```text
POST /api/message-center/messages/{messageId}/read
POST /api/message-center/messages/{messageId}/unread
```

响应不再包含 `unreadCount`：

```json
{
  "messageId": "message-id",
  "read": true
}
```

服务端先更新 Novu，再原子更新 Redis 投影。前端按本次状态切换调整角标。

### 一键已读后台任务

`POST /api/message-center/my/read-all`

返回 HTTP 202：

```json
{
  "jobId": "job-id",
  "state": "queued"
}
```

查询任务：

`GET /api/message-center/my/read-all/{jobId}`

```json
{
  "jobId": "job-id",
  "state": "running",
  "total": 50000,
  "processed": 1200,
  "error": null
}
```

状态为 `queued`、`running`、`completed` 或 `failed`。Worker 每批处理 100 条，Novu 写并发固定为 8，避免一键已读制造请求风暴。

## 前端调用示例

```ts
const page = await getMyMessages('unread', 1, 100)
if (page.state === 'building') {
    // 稍后重新请求新契约；不要调用旧接口或在当前页估算正式未读数。
}

store.messages = page.items
store.unreadCount = page.unreadCount
```

## 配置

```json
{
  "Redis": {
    "ConnectionString": "192.168.124.2:16379,abortConnect=false,connectTimeout=5000,syncTimeout=5000",
    "Database": 7,
    "KeyPrefix": "message-center"
  }
}
```

Redis 不可用时服务应视为不可用，不回退到逐页扫描 Novu。
