# MessageCenter Redis projection plan

## Goal

Remove all request-path full-feed scans and make unread count, unread/read filtering, and one-click read stable with 100k+ messages.

## Architecture

```text
business service -> MessageCenter API -> Novu
web              -> MessageCenter API -> Redis projection
                                      -> Novu state mutation
MessageCenter API -> Redis Streams -> MessageCenter Worker -> Novu
```

- Novu owns message content and final read state.
- Redis owns a rebuildable query projection and durable work queue.
- The API and worker use independent startup entries and deployment lifecycles.
- Redis Streams consumer groups provide acknowledgement and pending-job recovery.
- Rebuilds use staging keys and atomically replace the live projection only after success.
- Subscriber locks prevent concurrent rebuild/sync/read-all work across worker replicas.
- ClickHouse is not used: this workload is high-frequency state mutation and point counting, not append-only analytics.

## Contracts

- `GET /my?view=all|unread|read&page=&limit=` returns server-filtered page plus official total/unread counts.
- `GET /summary` returns official total/unread counts.
- `POST /messages/{id}/read|unread` returns only the updated state.
- `POST /my/read-all` creates a Redis-backed background job.
- `GET /my/read-all/{jobId}` returns job progress.
- Missing projection returns HTTP 202 with `state=building`; no synchronous Novu scan fallback exists.

## Acceptance

1. No controller query or mark endpoint loops through all Novu pages.
2. Frontend never derives official unread count from the loaded page.
3. Read-all HTTP request returns immediately and processing is bounded.
4. Redis loss can be repaired by background rebuild from Novu.
5. Backend build, frontend type-check focused delta, live auth, projection build, page query, mark and job flow pass.
6. A stopped worker leaves the stream entry pending; a replacement worker reclaims and completes it.
7. Rebuild timeout/retry never deletes the last ready projection.
