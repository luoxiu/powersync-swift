import Foundation
import GRDB
import PowerSync
import SQLite3

/// Adapts a GRDB `DatabasePool` for use with the PowerSync SDK.
///
/// This class implements `SQLiteConnectionPoolProtocol` and provides
/// integration between GRDB's connection pool and PowerSync's requirements,
/// including table update observation and direct access to SQLite connections.
///
/// - Provides async streams of table updates for replication.
/// - Bridges GRDB's managed connections to PowerSync's lease abstraction.
/// - Allows both read and write access to raw SQLite connections.
///
/// Bear Days / threetwo fork: table updates are multicast via ``BroadcastStream``.
/// Upload loop and `watch()` (AttachmentQueue, etc.) both subscribe; upstream
/// 1.14.4's uni-cast `AsyncStream` lets the second subscriber steal events so
/// CRUD uploads stop waking after the attachment watcher starts.
actor GRDBConnectionPool: SQLiteConnectionPoolProtocol {
    let pool: DatabasePool

    private let tableUpdatesStream = BroadcastStream<Set<String>>()

    var tableUpdates: AsyncStream<Set<String>> {
        tableUpdatesStream.subscribe()
    }

    init(
        pool: DatabasePool
    ) {
        self.pool = pool
        let stream = tableUpdatesStream
        pool.add(
            transactionObserver: PowerSyncTransactionObserver { updates in
                stream.dispatch(event: updates)
            },
            extent: .databaseLifetime
        )
    }

    func read<T: Sendable>(
        onConnection: @Sendable @escaping (SQLiteConnectionLease) throws -> T
    ) async throws -> T {
        return try await pool.read { database in
            try onConnection(
                GRDBConnectionLease(database: database)
            )
        }
    }

    func write<T: Sendable>(
        onConnection: @Sendable @escaping (SQLiteConnectionLease) throws -> T
    ) async throws -> T {
        // Don't start an explicit transaction, we do this internally.
        let (result, updates) = try await pool.writeWithoutTransaction { database in
            let observer = AllWritesObserver()
            database.add(transactionObserver: observer)
            defer { database.remove(transactionObserver: observer) }

            let result = try onConnection(GRDBConnectionLease(database: database))
            return (result, observer.committedTables)
        }

        if !updates.isEmpty {
            tableUpdatesStream.dispatch(event: updates)

            // Notify GRDB, this needs to be a write (transaction)
            try await pool.write { database in
                // Notify GRDB about these changes
                for table in updates {
                    try database.notifyChanges(in: Table(table))
                }
            }
        }
        return result
    }

    func withAllConnections<T: Sendable>(
        onConnection: @Sendable @escaping (SQLiteConnectionLease, [SQLiteConnectionLease]) throws -> T
    ) async throws -> T {
        // FIXME, we currently don't support updating the schema
        let result = try await pool.writeWithoutTransaction { database in
            let lease = try GRDBConnectionLease(database: database)
            let result = try onConnection(lease, [])
            database.clearSchemaCache()
            return result
        }
        pool.invalidateReadOnlyConnections()
        return result
    }

    func close() async throws {
        try pool.close()
    }
}
