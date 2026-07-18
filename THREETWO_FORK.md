# Bear Days (threetwo) fork notes

Base: upstream `powersync-ja/powersync-swift` tag `1.14.3`.

## Patch

`GRDBConnectionPool.tableUpdates` must multicast (`BroadcastStream`) so the CRUD
upload loop and AttachmentQueue `watch()` both receive table-update events.
Upstream 1.14.3 uses a uni-cast `AsyncStream`; the second subscriber steals
events and local writes stall in `ps_crud` until process restart.

Also: `PowerSyncTransactionObserver` observes trigger/C-API side-effects so
`ps_crud` appears in update sets.

## Maintaining

```bash
git remote add upstream https://github.com/powersync-ja/powersync-swift.git  # once
git fetch upstream --tags
git checkout threetwo/1.14.3-grdb-tableupdates-broadcast
git rebase 1.15.0   # example: new upstream tag
# resolve, push --force-with-lease, bump threetwo Package.swift revision
```

When upstream ships an equivalent multicast fix, delete this branch and point
threetwo back at `powersync-ja/powersync-swift`.
