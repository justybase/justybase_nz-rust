//! `nz-editor` — a terminal SQL workbench for IBM Netezza, built on the
//! pure-Rust `netezza` driver.
//!
//! Layout: a scrolling SQL editor on top, a scrolling result grid below, an
//! optional schema-browser sidebar (F9) and a status/log pane at the bottom.
//!
//! Queries run on a worker thread, so the UI stays responsive while the
//! appliance chews on a big scan; F8 sends the out-of-band cancel packet.
//!
//! | Key | Action |
//! |-----|--------|
//! | `F5` / `Ctrl+Enter` | execute the buffer (non-blocking) |
//! | `F8` | cancel the running query (out-of-band cancel packet) |
//! | `F6` | save results to `--out` (`.xlsx` / `.xlsb` select Excel) |
//! | `F4` | describe the current result set's columns |
//! | `F2` | expand/collapse the log pane |
//! | `F9` | toggle the schema browser sidebar |
//! | `Tab` / `Esc` | cycle focus: editor → grid → sidebar |
//! | `[` / `]` | previous / next result set (grid) |
//! | arrows / `PgUp` / `PgDn` / `Home` / `End` | navigate (grid, sidebar) |
//! | sidebar: type / `Backspace` | filter tables by name |
//! | sidebar: `Enter` | expand table / insert name into the editor |
//! | `Ctrl+P` / `Ctrl+N` | previous / next statement from history |
//! | `Ctrl+Q` / `Ctrl+C` | quit |
//!
//! Connection comes from `--conn <uri>` or `NZ_HOST`, `NZ_PORT`,
//! `NZ_DATABASE`, `NZ_USER`, `NZ_PASSWORD`.
//!
//! When `--sql` is given and stdout is not a terminal the query runs in batch
//! mode, prints tab-separated text and exits — handy for scripting and CI.

mod app;
mod browser;
mod export_excel;
mod grid;
mod syntax;
mod ui;
mod worker;

use app::{App, Focus};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use nz_rust::{NzConnection, NzConnectionConfig};
use std::io::IsTerminal;
use std::process::exit;
use std::time::Duration;

const DEFAULT_OUT: &str = "nzedit_export.txt";

struct Args {
    conn: Option<String>,
    sql: String,
    out: Option<String>,
}

fn parse_args() -> Args {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from<I: Iterator<Item = String>>(mut it: I) -> Args {
    let mut args = Args {
        conn: None,
        sql: String::new(),
        out: None,
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--conn" => args.conn = it.next(),
            "--sql" => args.sql = it.next().unwrap_or_default(),
            "--out" => args.out = it.next(),
            "-h" | "--help" => {
                println!(
                    "nz-editor — Netezza SQL TUI\n\n\
                     USAGE:\n  nz-editor [--conn URI] [--sql SQL] [--out FILE]\n\n\
                     OPTIONS:\n\
                     \x20 --conn URI    connection string (overrides NZ_* env vars)\n\
                     \x20 --sql SQL     statement to run on start (batch mode off a TTY)\n\
                     \x20 --out FILE    export path for F6 / batch mode; .xlsx/.xlsb select Excel\n\n\
                     ENV: NZ_HOST NZ_PORT NZ_DATABASE NZ_USER NZ_PASSWORD\n"
                );
                exit(0);
            }
            other => {
                eprintln!("nz-editor: unknown argument `{other}` (try --help)");
                exit(2);
            }
        }
    }
    args
}

fn build_connection(args: &Args) -> NzConnection {
    if let Some(uri) = &args.conn {
        return match NzConnection::connect_with_str(uri) {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("nz-editor: connect failed: {e}");
                exit(1);
            }
        };
    }

    if std::env::var("NZ_HOST").is_err() {
        eprintln!(
            "nz-editor: no connection configured — pass --conn, or set \
             NZ_HOST/NZ_PORT/NZ_DATABASE/NZ_USER/NZ_PASSWORD"
        );
        exit(2);
    }
    let config = NzConnectionConfig {
        host: std::env::var("NZ_HOST").unwrap_or_default(),
        port: std::env::var("NZ_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5480),
        database: std::env::var("NZ_DATABASE").unwrap_or_else(|_| "JUST_DATA".into()),
        user: std::env::var("NZ_USER").unwrap_or_else(|_| "admin".into()),
        password: std::env::var("NZ_PASSWORD").unwrap_or_default(),
        ..Default::default()
    };
    match NzConnection::connect(&config) {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("nz-editor: connect failed: {e}");
            exit(1);
        }
    }
}

fn describe(conn: &NzConnection) -> String {
    let config = conn.config();
    format!(
        "{}@{}:{}/{}",
        config.user, config.host, config.port, config.database
    )
}

/// Non-interactive execution: run one statement, write the result and exit.
fn run_batch(mut conn: NzConnection, args: &Args) -> ! {
    match conn.query(&args.sql, &[]) {
        Ok(result) => {
            match &args.out {
                None => print!("{}", nz_rust::result_to_text(&result, true)),
                Some(path) if export_excel::is_workbook_path(path) => {
                    if let Err(e) = export_excel::write_query_result_to_workbook(
                        &result,
                        std::path::Path::new(path),
                    ) {
                        eprintln!("nz-editor: cannot write {path}: {e}");
                        exit(1);
                    }
                }
                Some(path) => {
                    let text = nz_rust::result_to_text(&result, true);
                    if let Err(e) = std::fs::write(path, &text) {
                        eprintln!("nz-editor: cannot write {path}: {e}");
                        exit(1);
                    }
                }
            }
            for notice in &result.notices {
                eprintln!("notice: {notice}");
            }
            exit(0);
        }
        Err(e) => {
            eprintln!("nz-editor: {e}");
            exit(1);
        }
    }
}

fn main() {
    let args = parse_args();
    let conn = build_connection(&args);

    if !args.sql.is_empty() && !std::io::stdout().is_terminal() {
        run_batch(conn, &args);
    }

    let server = describe(&conn);
    // Hand the connection to the worker; keep the cancel key data locally so
    // F8 can interrupt a query while the worker is blocked on the socket.
    let cancel = (
        conn.config().clone(),
        conn.backend_process_id(),
        conn.backend_secret_key(),
    );
    let (jobs, events) = worker::spawn(conn);
    let out_path = args.out.clone().unwrap_or_else(|| DEFAULT_OUT.to_string());
    let mut app = App::new(server, out_path, jobs, events, cancel);
    app.load_schema();
    if !args.sql.is_empty() {
        app.editor.set_text(&args.sql);
        app.execute();
    }

    let mut terminal = ratatui::init();
    let outcome = run(&mut terminal, &mut app);
    ratatui::restore();
    if let Err(e) = outcome {
        eprintln!("nz-editor: {e}");
        exit(1);
    }
}

fn run(terminal: &mut ratatui::DefaultTerminal, app: &mut App) -> std::io::Result<()> {
    while !app.should_quit {
        app.drain_events();
        terminal.draw(|frame| ui::draw(frame, app))?;
        // Short poll: keeps the UI responsive while a query runs and lets
        // elapsed-time/cancel state update smoothly.
        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => handle_key(app, key),
                _ => {}
            }
        }
    }
    Ok(())
}

fn handle_key(app: &mut App, key: KeyEvent) {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

    if is_run_shortcut(key.code, key.modifiers) {
        app.execute();
        return;
    }

    if ctrl {
        match key.code {
            KeyCode::Char('q') | KeyCode::Char('c') => {
                app.should_quit = true;
                return;
            }
            KeyCode::Char('p') => {
                app.history_prev();
                return;
            }
            KeyCode::Char('n') => {
                app.history_next();
                return;
            }
            _ => {}
        }
    }

    match key.code {
        KeyCode::F(2) => app.toggle_log_pane(),
        KeyCode::F(4) => app.describe_columns(),
        KeyCode::F(5) => app.execute(),
        KeyCode::F(6) => app.save_results(),
        KeyCode::F(8) => app.cancel(),
        KeyCode::F(9) => app.toggle_browser(),
        KeyCode::Tab => app.toggle_focus(),
        KeyCode::Esc => app.focus = Focus::Input,
        _ => match app.focus {
            Focus::Input => editor_key(&mut app.editor, key.code, ctrl),
            Focus::Grid => grid_key(app, key.code),
            Focus::Browser => browser_key(app, key.code),
        },
    }
}

fn is_run_shortcut(code: KeyCode, modifiers: KeyModifiers) -> bool {
    code == KeyCode::Enter && modifiers.contains(KeyModifiers::CONTROL)
}

/// Browser keys: navigation, filter typing, Enter to expand/insert.
fn browser_key(app: &mut App, code: KeyCode) {
    match code {
        KeyCode::Up => app.browser.move_selection(-1),
        KeyCode::Down => app.browser.move_selection(1),
        KeyCode::PageUp => app.browser.move_selection(-20),
        KeyCode::PageDown => app.browser.move_selection(20),
        KeyCode::Home => app.browser.select_first(),
        KeyCode::End => app.browser.select_last(),
        KeyCode::Enter => app.browser_activate(),
        KeyCode::Backspace => app.browser_filter_backspace(),
        KeyCode::Char(ch) => app.browser_filter_key(ch),
        _ => {}
    }
}

/// Editor keys. `ctrl` is passed in so that an unhandled `Ctrl+<letter>` is
/// swallowed rather than typed into the buffer.
fn editor_key(editor: &mut crate::app::Editor, code: KeyCode, ctrl: bool) {
    match code {
        KeyCode::Char(c) if !ctrl => editor.insert_char(c),
        KeyCode::Enter if !ctrl => editor.newline(),
        KeyCode::Backspace => editor.backspace(),
        KeyCode::Delete => editor.delete(),
        KeyCode::Left => editor.move_left(),
        KeyCode::Right => editor.move_right(),
        KeyCode::Up => editor.move_up(),
        KeyCode::Down => editor.move_down(),
        KeyCode::Home => editor.home(),
        KeyCode::End => editor.end(),
        _ => {}
    }
}

fn grid_key(app: &mut App, code: KeyCode) {
    match code {
        KeyCode::Up => app.move_row(-1),
        KeyCode::Down => app.move_row(1),
        KeyCode::PageUp => app.move_row(-20),
        KeyCode::PageDown => app.move_row(20),
        KeyCode::Home => app.grid.row = 0,
        KeyCode::End => app.grid.row = app.row_count().saturating_sub(1),
        KeyCode::Left => app.move_col(-1),
        KeyCode::Right => app.move_col(1),
        KeyCode::Char('[') => app.step_set(false),
        KeyCode::Char(']') => app.step_set(true),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Args {
        parse_args_from(list.iter().map(|s| s.to_string()))
    }

    #[test]
    fn parses_all_options() {
        let a = args(&[
            "--conn",
            "host=1.2.3.4;user=admin",
            "--sql",
            "SELECT 1",
            "--out",
            "r.txt",
        ]);
        assert_eq!(a.conn.as_deref(), Some("host=1.2.3.4;user=admin"));
        assert_eq!(a.sql, "SELECT 1");
        assert_eq!(a.out.as_deref(), Some("r.txt"));
    }

    #[test]
    fn defaults_when_no_options_given() {
        let a = args(&[]);
        assert!(a.conn.is_none());
        assert!(a.sql.is_empty());
        assert!(a.out.is_none());
    }

    #[test]
    fn missing_option_value_is_tolerated() {
        // A dangling flag must not panic; the value is simply absent.
        let a = args(&["--sql"]);
        assert!(a.sql.is_empty());
    }

    #[test]
    fn editor_keys_type_and_edit() {
        use crate::app::Editor;
        let mut e = Editor::new();
        for c in "ab".chars() {
            editor_key(&mut e, KeyCode::Char(c), false);
        }
        assert_eq!(e.text(), "ab");
        editor_key(&mut e, KeyCode::Enter, false);
        editor_key(&mut e, KeyCode::Char('c'), false);
        assert_eq!(e.text(), "ab\nc");
        editor_key(&mut e, KeyCode::Backspace, false);
        editor_key(&mut e, KeyCode::Left, false);
        editor_key(&mut e, KeyCode::Home, false);
        editor_key(&mut e, KeyCode::End, false);
        assert_eq!(e.text(), "ab\n");
    }

    #[test]
    fn unhandled_control_chords_do_not_type_letters() {
        use crate::app::Editor;
        let mut e = Editor::new();
        for c in ['a', 'z', 'x', 'v', 'l'] {
            editor_key(&mut e, KeyCode::Char(c), true);
        }
        assert_eq!(e.text(), "", "Ctrl+<letter> must not reach the buffer");
        // Ctrl+Enter must not insert a newline (it is the run shortcut).
        editor_key(&mut e, KeyCode::Enter, true);
        assert_eq!(e.text(), "");
        // Navigation still works with Ctrl held.
        editor_key(&mut e, KeyCode::Char('o'), false);
        editor_key(&mut e, KeyCode::Char('k'), false);
        editor_key(&mut e, KeyCode::Left, true);
        assert_eq!(e.cur.1, 1);
    }

    #[test]
    fn ctrl_enter_is_the_run_shortcut() {
        assert!(is_run_shortcut(KeyCode::Enter, KeyModifiers::CONTROL));
        assert!(!is_run_shortcut(KeyCode::Enter, KeyModifiers::NONE));
        assert!(!is_run_shortcut(KeyCode::Char('r'), KeyModifiers::CONTROL));
    }
}
