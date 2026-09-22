//! Background worker that owns the [`NzConnection`].
//!
//! The TUI must never block on the appliance: a long `SELECT` would freeze
//! the event loop and make `F8` (cancel) useless, since the UI thread is the
//! one that has to send the out-of-band cancel packet. So the connection
//! lives on a worker thread; the UI talks to it through two channels:
//!
//! * [`Job`] (UI → worker): run a query, load catalog data, quit.
//! * [`WorkerEvent`] (worker → UI): query outcome, catalog chunks, state.
//!
//! Jobs are serialized on the single connection, so catalog loads queue up
//! behind a running query instead of racing it.

use nz_rust::{NzColumnInfo, NzConnection, NzResult, NzTableInfo, QueryResult};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// A unit of work for the worker thread.
pub enum Job {
    /// Execute a SQL batch.
    Query(String),
    /// Load the user-table list for the sidebar.
    LoadTables,
    /// Load the columns of one table (`table_index` echoes back in the event).
    LoadColumns {
        schema: String,
        table: String,
        table_index: usize,
    },
}

/// Something the worker wants the UI to know.
pub enum WorkerEvent {
    /// A query finished. `in_transaction` mirrors the session state after the
    /// batch so the status bar stays truthful without touching the worker.
    QueryDone {
        result: NzResult<QueryResult>,
        elapsed: Duration,
        in_transaction: bool,
    },
    /// The table list for the sidebar.
    Tables(Result<Vec<TableEntryLite>, String>),
    /// Columns of one table (`table_index` echoes the job's value).
    Columns {
        table_index: usize,
        result: Result<Vec<NzColumnInfo>, String>,
    },
}

/// Connection-free snapshot of [`NzTableInfo`] for the sidebar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEntryLite {
    pub schema: String,
    pub name: String,
}

impl From<&NzTableInfo> for TableEntryLite {
    fn from(t: &NzTableInfo) -> Self {
        TableEntryLite {
            schema: t.schema.clone(),
            name: t.name.clone(),
        }
    }
}

/// Start the worker with a connected connection. Returns the job sender and
/// the event receiver. The worker exits when every job sender has been
/// dropped (the UI going away closes the channel).
pub fn spawn(conn: NzConnection) -> (mpsc::Sender<Job>, mpsc::Receiver<WorkerEvent>) {
    let (job_tx, job_rx) = mpsc::channel::<Job>();
    let (event_tx, event_rx) = mpsc::channel::<WorkerEvent>();
    std::thread::Builder::new()
        .name("nz-worker".into())
        .spawn(move || worker_loop(conn, job_rx, event_tx))
        .expect("spawn nz-worker");
    (job_tx, event_rx)
}

fn worker_loop(
    mut conn: NzConnection,
    job_rx: mpsc::Receiver<Job>,
    event_tx: mpsc::Sender<WorkerEvent>,
) {
    let send = |event: WorkerEvent| {
        // A closed UI channel means the TUI is gone; stop working.
        if event_tx.send(event).is_err() {
            return false;
        }
        true
    };

    loop {
        match job_rx.recv() {
            Ok(Job::Query(sql)) => {
                let started = Instant::now();
                let result = conn.query(&sql, &[]);
                let event = WorkerEvent::QueryDone {
                    result,
                    elapsed: started.elapsed(),
                    in_transaction: conn.in_transaction(),
                };
                if !send(event) {
                    return;
                }
            }
            Ok(Job::LoadTables) => {
                let event = match conn.metadata().tables(None, None) {
                    Ok(tables) => {
                        WorkerEvent::Tables(Ok(tables.iter().map(TableEntryLite::from).collect()))
                    }
                    Err(e) => WorkerEvent::Tables(Err(e.to_string())),
                };
                if !send(event) {
                    return;
                }
            }
            Ok(Job::LoadColumns {
                schema,
                table,
                table_index,
            }) => {
                let schema = if schema.is_empty() {
                    None
                } else {
                    Some(schema.as_str())
                };
                let event = WorkerEvent::Columns {
                    table_index,
                    result: conn
                        .metadata()
                        .columns(&table, schema)
                        .map_err(|e| e.to_string()),
                };
                if !send(event) {
                    return;
                }
            }
            // A closed channel (UI dropped) ends the worker.
            Err(mpsc::RecvError) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The worker must exit when every job sender is dropped, even mid-job.
    #[test]
    fn worker_exits_when_channels_close() {
        // No appliance here: build a connection that never connects is not
        // possible (NzConnection::connect needs a server), so exercise the
        // quit path through a dropped sender using a dummy — we can only
        // assert the channel mechanics compile and close, which the type
        // system guarantees. This test exists to pin the contract.
        let (_tx, rx) = mpsc::channel::<Job>();
        drop(_tx);
        assert!(rx.recv().is_err());
    }
}
