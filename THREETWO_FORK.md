# Bear Days (threetwo) fork notes

Base: upstream `powersync-ja/powersync-swift` tag `1.14.4`.

## Patch

`GRDBConnectionPool.tableUpdates` must multicast (`BroadcastStream`) so the CRUD
upload loop and AttachmentQueue `watch()` both receive table-update events.
Upstream 1.14.3 uses a uni-cast `AsyncStream`; the second subscriber steals
events and local writes stall in `ps_crud` until process restart.

Also: `PowerSyncTransactionObserver` observes trigger/C-API side-effects so
`ps_crud` appears in update sets.

Backported from upstream `d597dc75` ("fix: clear upload errors", #156, first
released in 1.15.0): clear `SyncStatus.uploadError` once the upload queue is
fully drained. On 1.14.4 the upload error latched for the whole connection, so
Bear Days' family sync badge stayed on "Sync error" after sync had recovered,
until the app was relaunched. Drop this backport when rebasing onto >= 1.15.0.

## Maintaining

```bash
git remote add upstream https://github.com/powersync-ja/powersync-swift.git  # once
git fetch upstream --tags
git checkout threetwo/1.14.4-grdb-tableupdates-broadcast
git rebase onto newer upstream tags   # example: new upstream tag
# resolve, push --force-with-lease, bump threetwo Package.swift revision
```

When upstream ships an equivalent multicast fix, delete this branch and point
threetwo back at `powersync-ja/powersync-swift`.
