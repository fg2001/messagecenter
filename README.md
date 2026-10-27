# MessageCenter API

ASP.NET Core 6 API，使用 DM8 Outbox 可靠接收待发送消息，使用 Novu 保存消息正文和最终读写状态，使用 Redis 保存可重建的查询投影与后台任务。Redis 不是第二消息源；投影可以从 Novu 重新构建。

既有 PR1～3 数据库升级到本候选版本前，先使用本版本修正后的 `scripts/message-center-outbox-pr4-dm.sql`、`pr5`、`pr7`、`pr8`、`pr9`（按此顺序）；全新环境使用完整 `scripts/message-center-outbox-dm.sql`。原 PR4/5 脚本重复执行会报列已存在，不能沿用旧副本。修正脚本支持重复及部分执行后恢复，PR4 重跑保留 PR5 的 suppressed 状态范围；请先核对实际表结构属于这些已知版本，字段同名但类型/约束不同的漂移库须先人工核对。

迁移须备份并停止全部 API/Worker 写入。DM8 数据定义语句（DDL）会提交，脚本不是原子事务；中断后保持写入暂停，使用同版本脚本完整重跑，核对约束、历史原键和终态后再启动统一版本实例。PR4/5 的匿名块以独占一行 `/` 结束，使用 DIsql 或支持该分隔符的执行器，不得直接按分号拆分。保留所有消息、生命周期、原键锁、补偿任务和订阅人版本表；不提供删除表/列的降级脚本。出现 UNKNOWN 或终态后，暂停发送并保留新版本读/补偿能力，旧程序的兼容回退仍须另行验证，不能直接恢复旧状态约束。

## 运行拓扑

| 服务 | 地址 |
|---|---|
| MessageCenter API | `http://localhost:5000` |
| MessageCenter Worker | 独立常驻进程，无对外业务端口 |
| Novu API | `http://192.168.124.2:13000` |
| Keycloak | `http://192.168.124.2:18085/realms/master` |
| Redis | `192.168.124.2:16379`，DB 7 |
| DM8 | 通过 `ConnectionStrings__MessageCenter` 注入 |

API 与后台任务 Worker 使用两个独立启动入口。发送 API 事务性写入 DM8 Outbox，查询 API 读取 Redis 投影；Worker 独立投递 Novu、确认消息可见，并消费 Redis 投影任务。生产环境必须同时部署二者。

```powershell
$env:ConnectionStrings__MessageCenter = '<DM8 connection string>'
$env:Novu__ApiKey = '<Novu API key>'
dotnet build .\MessageCenter.Worker\MessageCenter.Worker.csproj
dotnet run --project .\MessageCenter.Api\MessageCenter.Api.csproj --urls http://localhost:5000
dotnet run --project .\MessageCenter.Worker\MessageCenter.Worker.csproj
```

健康检查：

```bash
curl http://localhost:5000/health
curl http://localhost:5000/health/outbox
curl http://localhost:5000/health/worker
```

`/health/outbox` 验证 DM8 可连接；`/health/worker` 在最近 30 秒没有收到独立 Worker 心跳时返回 HTTP 503。

## 状态与一致性

- Novu 是消息正文及最终已读状态的权威数据源。
- DM8 `message_send_outbox` 是待发送消息和发送状态的持久记录；建表脚本为 `scripts/message-center-outbox-dm.sql`。
- Redis 保存消息轻量投影、`all/unread/read` 有序索引、Streams 任务和任务状态。
- 查询只访问 Redis，不在 HTTP 请求中遍历 Novu 全部消息。
- Redis 投影不存在时，查询返回 HTTP 202 和 `state=building`，独立 Worker 从 Novu 重建投影。
- 重建写入任务专属 staging 投影；全部分页成功后原子切换。已有正式投影在重建和重试期间继续可读。
- Worker 使用 Consumer Group 确认任务；进程退出后的 pending 任务由其他 Worker 回收，不会因出队而丢失。
- Novu 分页瞬时异常最多重试 3 次，整个任务最多尝试 5 次，并使用指数退避。
- 新消息只能通过 `POST /send` 进入正式链路；该接口事务性写入 DM8 Outbox，由 Worker 触发 Novu 并更新 Redis 投影。直接绕过 MessageCenter 调用 Novu 不属于受支持的生产契约。

## 鉴权

所有 `/api/message-center/*` 接口要求：

```text
Authorization: Bearer <token>
```

发送使用已注册服务账号。个人查询和状态操作从 JWT 的 `preferred_username` 解析订阅者，不接受客户端传入 `subscriberId`。

## API 契约

### 受保护来源的服务身份

原有`SourceSystemNames`登记保持。需要限制为专用服务Token的来源可增加：

```json
{
  "SourceServiceIdentities": {
    "micip": {
      "Issuer": "<实际issuer>",
      "Subject": "<实际服务账号sub>",
      "ClientId": "<实际服务客户端azp>",
      "Audience": "<MC API受众>"
    }
  }
}
```

以上是占位示例，不能直接上线。字典键是`SourceSystemNames`的来源值，不是服务用户名或正文自报值。对应环境变量如`SourceServiceIdentities__micip__Subject`。启用前核对实际已验证服务Token的四项声明；不从客户端名推导sub，不输出完整Token或凭据。

发送、发送查询与重试共用来源准入：在现有JWT签名/issuer/有效期认证后，受保护来源要求精确匹配issuer/sub/azp，aud必须包含配置的MC受众。缺失/重复身份声明、未认证人员或不完整配置返回403/`SOURCE_SERVICE_IDENTITY_DENIED`，不访问业务存储或Novu。不采用用户名相同、`client_id`或人员会话作为回退。仅身份配置不能代替来源登记，正文不能改变来源。

显式声明来源节点为`{}`、`null`或空字符串同样视为已启用保护但配置不完整，三个来源入口均拒绝访问；不会因.NET 6字典绑定忽略空项而恢复用户名放行。只有最终合并配置中没有该来源节点时，才视为未配置保护。

未配置保护的其他来源保留原行为；这不是全局JWT受众加固，个人消息接口及未配置来源仍有原认证边界。既有客户端需要逐个盘点后迁移，不能据此宣称所有调用已经安全。HTTP测试issuer可精确配置，不改变现有HTTPS元数据设置；生产传输要求另行验收。

配置错误或凭据/客户端变化后的403不证明原消息未发送；保留requestId与UNKNOWN，修正绑定后查询原键。配置不会创建用户、会话、订阅者、工作流或角色，不同步人员/组织。不新增表或依赖。

回退不得直接删除已生效的身份保护并恢复仅用户名放行。应暂停该来源新发送，必要时撤销其来源登记来封闭所有服务入口，再使用仍具身份约束的兼容代码/配置恢复原键查询；保留Outbox和历史消息。不得通过改名、借用其他来源或换requestId绕过拒绝。真实身份/共享实例兼容、API管道和渠道送达仍需部署验收。



### 按来源选择工作流

默认继续使用`Novu:DefaultWorkflowId`。需要独立渠道的已登记来源可设置：

```json
{
  "Novu": {
    "SourceWorkflows": { "micip": "micip-reminder" }
  }
}
```

环境变量对应`Novu__SourceWorkflows__micip=micip-reminder`。键是MC服务端`SourceSystemNames`映射的来源值，不是客户端自报的`sourceSystem`或服务用户名。发送鉴权和来源登记保持，正文不能选择工作流；未配置的来源继续使用原默认工作流。显式配置空值返回503/`SOURCE_WORKFLOW_UNAVAILABLE`，不会降回共享工作流。

工作流须在Novu预先配置并验证渠道及测试接收人；此配置不创建工作流，也不保证主动提醒额度、截止时间或最终送达。API与Worker无需新增表或请求字段，Worker继续使用Outbox已保存的工作流。配置调整仅影响新受理请求；同键同载荷重放返回原结果，重试使用原行的工作流，不会改投新工作流。配置切换前须排查未确定请求并保留原键，不能换键规避不确定状态。

回退时先暂停受影响来源的新发送，保留已入账任务和原工作流；撤销本项配置不会撤回已发送消息或重写Outbox。共享默认工作流及其他来源配置无需修改。

### 发送消息

`POST /api/message-center/send`

```json
{
  "requestId": "process-task-TASK_001-created",
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

请求必须携带稳定的 `requestId`。同一来源系统使用相同 `requestId` 和相同内容重试时返回原任务；相同 `requestId` 携带不同内容返回 HTTP 409。

成功落库返回 HTTP 202：

```json
{
  "requestId": "process-task-TASK_001-created",
  "state": "pending",
  "recipients": [
    {
      "sendId": "stable-send-id",
      "subscriberId": "196045",
      "state": "pending",
      "retryCount": 0,
      "transactionId": null,
      "error": null
    }
  ],
  "skipped": []
}
```

HTTP 202 表示完整消息已事务性写入 DM8，Worker 会使用 `sendId` 作为 Novu `transactionId` 和 `Idempotency-Key` 重试发送。发送状态通过 `GET /api/message-center/send/{requestId}` 查询；失败任务可由 `POST /api/message-center/send/{requestId}/retry` 人工重放。最终状态为 `visible` 时，消息已写入 Redis 查询投影。

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

状态为 `queued`、`running`、`retrying`、`completed` 或 `failed`。Worker 每批处理 100 条，Novu 写并发固定为 8，避免一键已读制造请求风暴。

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
  },
  "Novu": {
    "TimeoutSeconds": 10,
    "WorkerTimeoutSeconds": 30
  },
  "MessageWorker": {
    "PageRetryCount": 3,
    "JobRetryCount": 5,
    "RetryBaseDelaySeconds": 2,
    "LockSeconds": 60,
    "PendingIdleSeconds": 90,
    "ActiveSyncIntervalSeconds": 30
  }
}
```

Redis 不可用时服务应视为不可用，不回退到逐页扫描 Novu。

## 测试与 Linux 发布

Redis 集成测试使用独立随机前缀，并在结束时删除测试键：

```powershell
$env:MESSAGE_CENTER_TEST_REDIS = '192.168.124.2:16379,abortConnect=false,connectTimeout=5000,syncTimeout=5000'
dotnet test .\MessageCenter.Tests\MessageCenter.Tests.csproj
```

测试覆盖 staging 原子切换、重建期间读状态一致性以及 pending 任务跨 Consumer 回收。

测试服务器没有安装 .NET 运行时时，可发布 Linux 自包含 Worker：

```powershell
dotnet restore .\MessageCenter.Worker\MessageCenter.Worker.csproj -r linux-x64
dotnet publish .\MessageCenter.Worker\MessageCenter.Worker.csproj `
  -c Release -r linux-x64 --self-contained true --no-restore
```
## Novu 触发回执与原键

触发客户端要求成功 HTTP 响应中的 `data.transactionId` 与本次发送的 `sendId` 精确一致（区分大小写，不裁剪空白）。缺失、空值或不匹配均抛出固定错误 `Novu trigger transaction mismatch.`，不把其他事务的回执写成本事务 accepted。Worker 继续使用既有失败路径、原 Outbox 行、工作流、载荷和原键；不新建请求、不返还 MICIP 额度。错误日志不包含对端返回的错误事务 ID。

此检查只证明回执关联，不证明送达、已读或业务处理，也不能证明异常响应对应的原请求从未受理。现有 MC 自动原键重试仍依赖 Novu 幂等保留窗口；配置和实际版本须单独核验，窗口过期、截止执行、跨日和 UNKNOWN 结算不由本补丁保证。`AcceptedAt` 是本地收到成功回执的观察时间，不能代替真实平台受理日；MC 202 和查询404均不作最终送达/未发送证明。无请求字段、表、依赖或部署配置变更。

## 生命周期与原请求并发（本地候选补丁）

在PR4/5/6草案之上，PR7新增持久原请求锁表message_request_guard。已有PR5结构先执行scripts/message-center-outbox-pr7-dm.sql，再联合更新全部MC API与Worker实例；全新环境使用当前完整bootstrap。锁作用于所有来源的发送/生命周期受理；不能只给某个启用严格触发的实例迁移，也不能在仍有旧API实例处理生命周期时宣称并发保证生效。

发送与CONSUME/REVOKE/EXPIRE按精确sourceSystem+requestId共用数据库行锁，直到受理/终态事务提交。即使消息行还不存在，撤回标记与迟到send也不会交错留下有效发送；相同来源/请求的收件人共享串行化，其他原键不共用锁。锁记录保留，不按请求结束删除；备份与回退须保留原请求、终态历史和锁表，不通过清表或重置版本回退。缺行终止标记也拒绝相等/更低businessVersion；同operationId重放和CONSUME缺行404语义不变。

严格来源仍按PR6在POST前持久uncertain，只有确切未发送证明才可原键人工恢复；有限缓存不是永久去重保证。PR7不实现外部Novu/收件箱终态的持久重试或页面显示，不能据此宣告整条生命周期完成。当前只有本地候选实现和隔离验证，部署与联调仍待评审及具体授权。
# 终态持久补偿（PR8，本地候选）

消费、撤回或过期与 `message_terminal_sync` 原sendId/业务版本任务共同提交，API不等待Novu或Redis。
独立Worker从DM8认领，按原收件人/事务键查找消息，删除前保存目标ID；Novu删除成功但投影失败后，换进程仍可使用原ID继续清理。
CONSUME标记Novu已读，REVOKE/EXPIRE删除目标；三类终态均清除当前及重建中的Redis投影。不会再次调用trigger。

先执行 `scripts/message-center-outbox-pr8-dm.sql`，再统一升级API和Worker（依赖PR4～PR7）。脚本可重复执行，按原版本补齐历史终态任务，不重置已有任务/租约，也不改写原消息与生命周期记录。
保留补偿表、原消息、原键锁及终态审计；不能通过删表或退回旧API来宣称安全回退。

接口原 `applied/deferred/replayed` 仍表示本地操作状态。新增 `externalState` 区分 `pending`（未确认或故障）、`watching`（已清理当前已知目标，仍巡检迟到消息）、`not-required`（已证明新send受理即抑制，从未触发）、`superseded`、`waiting-for-send` 及 `not-tracked`（历史操作无对应任务）。操作回读另附 `lastExternalConfirmedAt/externalError`。不能把202、一次空feed、历史触发计数为0或一次清理成功当作永久收敛。

每轮使用既有Outbox批量、租约、可见性页数和重试选项；保存分页位置，下一轮继续深分页。已清理任务使用ActiveSyncIntervalSeconds继续巡检，发现迟到目标后仍按原键补偿。租约过期owner不能登记目标或回写结果。历史任务持续巡检的容量、保留期、监控告警和实际同步时限仍须D6确定；没有擅自设定“查不到即成功”的截止时间。

PR8本地验证含真实隔离DM8、进程间恢复和受控HTTP/Redis故障夹具；当时尚未处理页面/全量投影重建重新导入旧消息的问题，后续由PR9补齐。T-INT-05尚未整体完成，真实Novu与页面联合验收仍待。

## 收件箱终态过滤与代际保护（PR9，本地候选）

`message_subscriber_version` 按收件人保存单调终态版本，与终态/补偿同事务更新；重放不重复增加，回滚不改变版本。API读取列表、未读数和未读ID时前后校验版本及Redis代际，失效投影沿用building/202语义，等待重建后展示。每次重建尝试使用独立代际；全量和增量均按原sendId/事务键批量核对MC发送记录，过滤CONSUMED/REVOKED/EXPIRED。无法绑定本人的micip消息也暂不展示；旧来源未关联历史消息维持兼容，因此实际Novu必须保留来源与原发送关联字段，不能把元数据全部丢失的旧消息认作已核验MICIP消息。

Redis内部使用p9版本信封保留原发送关联键，公开列表DTO不变。Lua将代际检查、条目与索引写入合并为原子操作，已切换的旧增量/重建写入不能恢复旧条目或未读计数；读状态修改保留关联键且不创建不存在的消息。终态发生于重建期间时拒绝发布旧版本，即使发生在检查与Redis切换之间，读侧版本校验也会拒绝旧投影。接口新增固定主键版本查询与列表批量原键查询，不逐条回查全历史。

已有PR8环境先执行 `scripts/message-center-outbox-pr9-dm.sql`（可重复执行，不重置已有版本），再统一升级所有API和Worker。新环境用完整bootstrap；旧格式ready投影自动要求重建，无需清空Redis或业务表。不得混用旧Worker，否则旧实例仍可能写旧格式。回退保留原发送、终态、任务及版本表；暂停外发/外部生命周期并停止新旧实例混跑，不能以恢复旧ready格式作为业务安全回退。正式维护窗口与回退验收仍需D6批准。

本地验证：106项非Redis单测、10项真实独立Redis专项及API/Worker构建通过；15项真实隔离DM8检查包含PR8→PR9重复升级保留历史、并发版本增加与回滚。严格触发跨进程7项、原键并发5项回归通过。Redis专项的业务数据库为EF内存库，DM8专项的外部HTTP/Redis为夹具，两类证据分别归档，不能合称真实全链路。未进行真实Novu、MC浏览器展示或整体升级/回退验收；未推送补丁、部署业务服务或发送通知。
