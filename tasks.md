# MessageCenter Redis projection tasks

- [x] Add Redis connection and projection indexes.
- [x] Replace page-local filtering with `view=all|unread|read`.
- [x] Replace `/unread-count` with `/summary`.
- [x] Remove unread full scans from mark endpoints.
- [x] Replace the lossy Redis list with an acknowledged Redis Streams consumer group.
- [x] Add an independent `MessageCenter.Worker` startup entry.
- [x] Add bounded page/job retries, subscriber locks, heartbeat and pending recovery.
- [x] Rebuild into staging keys and atomically switch the live projection.
- [x] Add background rebuild and incremental synchronization.
- [x] Update `E:\Web\lnbox\web` to the new API contract.
- [x] Complete automated and live integration verification.
