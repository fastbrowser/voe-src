use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use voe::mux::{decode_frame, encode_frame, StreamTx, CLOSE, DATA, OPEN, OPEN_FAIL, OPEN_OK};
use voe::{
    ctrl_frame, decode, decode_ctrl, decrypt_data, encrypt_data, load_config, ServerConfig,
    ShareBanConfig, CTRL, VERSION,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, WebSocketStream};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

type WsSink = SplitSink<WebSocketStream<TcpStream>, Message>;

const STALE_AFTER: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

enum EventMsg {
    Connected { id: usize, name: String },
    Disconnected { id: usize },
    Traffic { id: usize, bytes: usize },
}

struct UserSession {
    name: String,
    username: String,
    current_kbps: f64,
    total_bytes: usize,
    kill_switch: Arc<Mutex<Option<String>>>,
    last_seen: Arc<Mutex<Instant>>,
    msg_tx: mpsc::UnboundedSender<String>,
}

struct RateLimiter {
    bytes_per_sec: u32,
    bytes_sent_this_window: usize,
    last_window_start: Instant,
}

impl RateLimiter {
    fn new(kbps: u32) -> Self {
        Self {
            bytes_per_sec: kbps.saturating_mul(1024),
            bytes_sent_this_window: 0,
            last_window_start: Instant::now(),
        }
    }

    async fn limit(&mut self, amount: usize) {
        if self.bytes_per_sec == 0 {
            return;
        }
        self.bytes_sent_this_window += amount;
        let elapsed = self.last_window_start.elapsed();
        if elapsed.as_secs() >= 1 {
            self.bytes_sent_this_window = 0;
            self.last_window_start = Instant::now();
        } else if self.bytes_sent_this_window > self.bytes_per_sec as usize {
            sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn send_ctrl(ws: &Arc<AsyncMutex<WsSink>>, secret: &str, text: &str) {
    ws.lock().await.send(Message::Binary(ctrl_frame(secret, text))).await.ok();
}
async fn reject(ws: &Arc<AsyncMutex<WsSink>>, secret: &str, reason: &str) {
    let mut w = ws.lock().await;
    w.send(Message::Binary(ctrl_frame(secret, &format!("fatal:{}", reason))))
        .await
        .ok();
    w.close().await.ok();
}

enum Admit {
    Admitted,
    Conflict,
    Denied(String),
}
fn try_admit(
    bans: &Mutex<HashMap<String, Instant>>,
    sessions: &Mutex<HashMap<usize, UserSession>>,
    share_ban: &ShareBanConfig,
    id: usize,
    username: &str,
    grace_over: bool,
    make_session: impl FnOnce() -> UserSession,
) -> Admit {
    let now = Instant::now();
    let mut b = bans.lock().unwrap();
    let mut s = sessions.lock().unwrap();

    if share_ban.enabled {
        if let Some(until) = b.get(username).copied() {
            if now < until {
                let mins = (until.saturating_duration_since(now).as_secs() + 59) / 60;
                return Admit::Denied(format!(
                    "This key is banned for another {} minute(s) for connection sharing.",
                    mins
                ));
            }
            b.remove(username);
        }

        let live = s
            .values()
            .any(|x| x.username == username && x.last_seen.lock().unwrap().elapsed() < STALE_AFTER);
        if live {
            if !grace_over {
                return Admit::Conflict;
            }
            let minutes = share_ban.duration_minutes;
            b.insert(username.to_string(), now + Duration::from_secs(minutes * 60));
            let reason = format!(
                "This key was used from more than one connection and is banned for {} minutes.",
                minutes
            );
            for sess in s.values().filter(|x| x.username == username) {
                *sess.kill_switch.lock().unwrap() = Some(reason.clone());
            }
            return Admit::Denied(reason);
        }
        for sess in s.values().filter(|x| x.username == username) {
            *sess.kill_switch.lock().unwrap() = Some("Replaced by a newer connection.".to_string());
        }
    }

    s.insert(id, make_session());
    Admit::Admitted
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: ServerConfig = load_config("config-server.yml");
    let (tx, mut rx) = mpsc::channel::<EventMsg>(1000);

    let sessions: Arc<Mutex<HashMap<usize, UserSession>>> = Arc::new(Mutex::new(HashMap::new()));
    let bans: Arc<Mutex<HashMap<String, Instant>>> = Arc::new(Mutex::new(HashMap::new()));

    let sessions_clone = Arc::clone(&sessions);
    let bans_clone = Arc::clone(&bans);
    let tx_clone = tx.clone();
    let cfg = config.clone();

    tokio::spawn(async move {
        let listener = TcpListener::bind(&cfg.listen_addr).await.unwrap();
        let mut id_counter = 0;

        while let Ok((stream, _)) = listener.accept().await {
            stream.set_nodelay(true).ok();
            id_counter += 1;
            let current_id = id_counter;
            let cfg_inner = cfg.clone();
            let tx_inner = tx_clone.clone();
            let sessions_inner = Arc::clone(&sessions_clone);
            let bans_inner = Arc::clone(&bans_clone);

            tokio::spawn(async move {
                let ws_stream = match accept_async(stream).await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let (ws_sink, mut ws_read) = ws_stream.split();
                let ws_write: Arc<AsyncMutex<WsSink>> = Arc::new(AsyncMutex::new(ws_sink));
                let secret = cfg_inner.secret_key.clone();
                let mut authenticated_user = None;
                if let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(10), ws_read.next()).await {
                    let auth = decode(msg.to_text().unwrap_or(""));
                    let parts: Vec<&str> = auth.split(':').collect();
                    if parts.len() == 2 {
                        if let Some(user) = cfg_inner
                            .users
                            .iter()
                            .find(|u| u.username == parts[0] && u.password == parts[1])
                        {
                            authenticated_user = Some(user.clone());
                        }
                    }
                }
                let user = match authenticated_user {
                    Some(u) => u,
                    None => {
                        reject(&ws_write, &secret, "Invalid username or password.").await;
                        return;
                    }
                };
                let hello = match timeout(Duration::from_secs(10), ws_read.next()).await {
                    Ok(Some(Ok(m))) => m,
                    _ => return,
                };
                let client_version = decode_ctrl(&secret, &hello.into_data())
                    .and_then(|t| t.strip_prefix("hello:").map(str::to_string));
                if client_version.as_deref() != Some(VERSION) {
                    let reason = format!(
                        "Please update your client. (client: {}, server: {})",
                        client_version.as_deref().unwrap_or("unknown"),
                        VERSION
                    );
                    reject(&ws_write, &secret, &reason).await;
                    return;
                }

                let kill_switch: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
                let kill_switch_clone = Arc::clone(&kill_switch);
                let last_seen = Arc::new(Mutex::new(Instant::now()));
                let last_seen_clone = Arc::clone(&last_seen);
                let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<String>();

                let grace_deadline = Instant::now() + STALE_AFTER + Duration::from_secs(1);
                let denied: Option<String> = loop {
                    let grace_over = Instant::now() >= grace_deadline;
                    let outcome = try_admit(
                        &bans_inner,
                        &sessions_inner,
                        &cfg_inner.share_ban,
                        current_id,
                        &user.username,
                        grace_over,
                        || UserSession {
                            name: user.display_name.clone(),
                            username: user.username.clone(),
                            current_kbps: 0.0,
                            total_bytes: 0,
                            kill_switch: Arc::clone(&kill_switch),
                            last_seen: Arc::clone(&last_seen),
                            msg_tx: msg_tx.clone(),
                        },
                    );
                    match outcome {
                        Admit::Admitted => break None,
                        Admit::Denied(m) => break Some(m),
                        Admit::Conflict => sleep(Duration::from_secs(1)).await,
                    }
                };
                if let Some(reason) = denied {
                    reject(&ws_write, &secret, &reason).await;
                    return;
                }

                send_ctrl(&ws_write, &secret, &format!("welcome:{}", VERSION)).await;
                let _ = tx_inner
                    .send(EventMsg::Connected { id: current_id, name: user.display_name.clone() })
                    .await;

                let kbps = user.max_kbps.unwrap_or(u32::MAX);
                let rx_limiter = Arc::new(AsyncMutex::new(RateLimiter::new(kbps)));
                let tx_limiter = Arc::new(AsyncMutex::new(RateLimiter::new(kbps)));

                let routes: Arc<Mutex<HashMap<u32, StreamTx>>> = Arc::new(Mutex::new(HashMap::new()));

                {
                    let ws = Arc::clone(&ws_write);
                    let secret = secret.clone();
                    tokio::spawn(async move {
                        while let Some(text) = msg_rx.recv().await {
                            send_ctrl(&ws, &secret, &format!("notice:{}", text)).await;
                        }
                    });
                }

                loop {
                    let kill_reason = kill_switch_clone.lock().unwrap().clone();
                    if let Some(reason) = kill_reason {
                        send_ctrl(&ws_write, &secret, &format!("fatal:{}", reason)).await;
                        break;
                    }
                    if last_seen_clone.lock().unwrap().elapsed() > IDLE_TIMEOUT {
                        break;
                    }
                    let msg = match timeout(Duration::from_millis(500), ws_read.next()).await {
                        Err(_) => continue,
                        Ok(Some(Ok(m))) => m,
                        Ok(_) => break,
                    };
                    *last_seen_clone.lock().unwrap() = Instant::now();

                    let enc = msg.into_data();
                    rx_limiter.lock().await.limit(enc.len()).await;
                    let _ = tx_inner.send(EventMsg::Traffic { id: current_id, bytes: enc.len() }).await;

                    let raw = match decrypt_data(&cfg_inner.secret_key, &enc) {
                        Some(d) => d,
                        None => continue,
                    };
                    let (sid, kind, payload) = match decode_frame(&raw) {
                        Some(f) => f,
                        None => continue,
                    };

                    match kind {
                        CTRL => {
                            if payload == b"ping" {
                                send_ctrl(&ws_write, &secret, "pong").await;
                            }
                        }
                        OPEN => {
                            if payload.len() < 4 {
                                continue;
                            }
                            let atyp = payload[3];
                            let parsed = match atyp {
                                1 => {
                                    if payload.len() < 10 {
                                        continue;
                                    }
                                    Some((
                                        format!("{}.{}.{}.{}", payload[4], payload[5], payload[6], payload[7]),
                                        8usize,
                                    ))
                                }
                                3 => {
                                    if payload.len() < 5 {
                                        continue;
                                    }
                                    let len = payload[4] as usize;
                                    if payload.len() < 5 + len + 2 {
                                        continue;
                                    }
                                    Some((String::from_utf8_lossy(&payload[5..5 + len]).to_string(), 5 + len))
                                }
                                _ => None,
                            };
                            let Some((host, port_offset)) = parsed else { continue };
                            let port = u16::from_be_bytes([payload[port_offset], payload[port_offset + 1]]);
                            let target = format!("{}:{}", host, port);

                            let ws_write = Arc::clone(&ws_write);
                            let routes = Arc::clone(&routes);
                            let secret = secret.clone();
                            let tx_limiter = Arc::clone(&tx_limiter);
                            let tx_inner2 = tx_inner.clone();

                            tokio::spawn(async move {
                                match TcpStream::connect(&target).await {
                                    Ok(mut tcp) => {
                                        let (mut tcp_read, mut tcp_write) = tcp.split();
                                        let (stx, mut srx) = mpsc::channel::<Vec<u8>>(256);
                                        routes.lock().unwrap().insert(sid, stx);

                                        let ok = encrypt_data(&secret, &encode_frame(sid, OPEN_OK, &[]));
                                        ws_write.lock().await.send(Message::Binary(ok)).await.ok();

                                        let writer = async {
                                            while let Some(chunk) = srx.recv().await {
                                                if tcp_write.write_all(&chunk).await.is_err() {
                                                    break;
                                                }
                                            }
                                        };
                                        let reader = async {
                                            let mut buf = [0u8; 8192];
                                            loop {
                                                match tcp_read.read(&mut buf).await {
                                                    Ok(0) | Err(_) => break,
                                                    Ok(n) => {
                                                        tx_limiter.lock().await.limit(n).await;
                                                        let _ = tx_inner2
                                                            .send(EventMsg::Traffic { id: current_id, bytes: n })
                                                            .await;
                                                        let f = encrypt_data(
                                                            &secret,
                                                            &encode_frame(sid, DATA, &buf[..n]),
                                                        );
                                                        if ws_write.lock().await.send(Message::Binary(f)).await.is_err()
                                                        {
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        };
                                        tokio::select! { _ = writer => {}, _ = reader => {} }
                                        routes.lock().unwrap().remove(&sid);
                                        let close = encrypt_data(&secret, &encode_frame(sid, CLOSE, &[]));
                                        ws_write.lock().await.send(Message::Binary(close)).await.ok();
                                    }
                                    Err(_) => {
                                        let fail = encrypt_data(&secret, &encode_frame(sid, OPEN_FAIL, &[]));
                                        ws_write.lock().await.send(Message::Binary(fail)).await.ok();
                                    }
                                }
                            });
                        }
                        DATA => {
                            let maybe_tx = routes.lock().unwrap().get(&sid).cloned();
                            if let Some(stx) = maybe_tx {
                                let _ = stx.send(payload.to_vec()).await;
                            }
                        }
                        CLOSE => {
                            routes.lock().unwrap().remove(&sid);
                        }
                        _ => {}
                    }
                }

                routes.lock().unwrap().clear();
                ws_write.lock().await.close().await.ok();

                sessions_inner.lock().unwrap().remove(&current_id);
                let _ = tx_inner.send(EventMsg::Disconnected { id: current_id }).await;
            });
        }
    });

    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut selected_index: usize = 0;
    let mut tick_count = 0; 
    let mut compose: Option<(Option<usize>, String)> = None;

    loop {
        terminal.draw(|f| {
            let size = f.size();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(3)])
                .split(size);

            f.render_widget(
                Paragraph::new(format!("voe server v{}", VERSION)).block(Block::default().borders(Borders::ALL)),
                chunks[0],
            );

            let sessions_lock = sessions.lock().unwrap();
            let mut sorted_sessions: Vec<(usize, &UserSession)> =
                sessions_lock.iter().map(|(&id, session)| (id, session)).collect();
            sorted_sessions.sort_by_key(|k| k.0);

            let current_count = sorted_sessions.len();
            let active_index = if current_count == 0 { 0 } else { selected_index.min(current_count - 1) };

            let mut list_items = Vec::new();
            for (i, (id, session)) in sorted_sessions.iter().enumerate() {
                let speed = if session.current_kbps > 1024.0 {
                    format!("{:.2} MBps", session.current_kbps / 1024.0)
                } else {
                    format!("{:.2} KBps", session.current_kbps)
                };

                let text = format!(
                    "ID: {} | {} | {} | Total: {} MB",
                    id,
                    session.name,
                    speed,
                    session.total_bytes / 1048576
                );

                if i == active_index {
                    list_items.push(ListItem::new(text).style(
                        ratatui::style::Style::default()
                            .fg(ratatui::style::Color::Black)
                            .bg(ratatui::style::Color::Yellow)
                            .add_modifier(ratatui::style::Modifier::BOLD),
                    ));
                } else {
                    list_items.push(ListItem::new(text));
                }
            }

            f.render_widget(
                List::new(list_items)
                    .block(Block::default().title("Active Users (Arrows to move, Enter to Kick)").borders(Borders::ALL)),
                chunks[1],
            );

            let footer = match &compose {
                Some((target, buf)) => format!(
                    "To {}: {}_   (Enter to send, Esc to cancel)",
                    if target.is_some() { "selected user" } else { "all users" },
                    buf
                ),
                None => "q: quit | Enter: kick | m: message selected user | b: broadcast".to_string(),
            };
            f.render_widget(
                Paragraph::new(footer).block(Block::default().borders(Borders::ALL)),
                chunks[2],
            );
        })?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if let Some((target, mut buf)) = compose.take() {
                    match key.code {
                        KeyCode::Esc => {}
                        KeyCode::Enter => {
                            if !buf.trim().is_empty() {
                                let s = sessions.lock().unwrap();
                                match target {
                                    Some(id) => {
                                        if let Some(sess) = s.get(&id) {
                                            let _ = sess.msg_tx.send(buf.clone());
                                        }
                                    }
                                    None => {
                                        for sess in s.values() {
                                            let _ = sess.msg_tx.send(buf.clone());
                                        }
                                    }
                                }
                            }
                        }
                        KeyCode::Backspace => {
                            buf.pop();
                            compose = Some((target, buf));
                        }
                        KeyCode::Char(c) => {
                            buf.push(c);
                            compose = Some((target, buf));
                        }
                        _ => {
                            compose = Some((target, buf));
                        }
                    }
                } else {
                    match key.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('b') => {
                            compose = Some((None, String::new()));
                        }
                        KeyCode::Char('m') => {
                            let s = sessions.lock().unwrap();
                            let mut ids: Vec<usize> = s.keys().cloned().collect();
                            ids.sort();
                            if let Some(&id_val) = ids.get(selected_index) {
                                compose = Some((Some(id_val), String::new()));
                            }
                        }
                        KeyCode::Up => {
                            selected_index = selected_index.saturating_sub(1);
                        }
                        KeyCode::Down => {
                            let s = sessions.lock().unwrap();
                            if selected_index < s.len().saturating_sub(1) {
                                selected_index += 1;
                            }
                        }
                        KeyCode::Enter => {
                            let s = sessions.lock().unwrap();
                            let mut ids: Vec<usize> = s.keys().cloned().collect();
                            ids.sort();

                            if let Some(&id_val) = ids.get(selected_index) {
                                if let Some(session) = s.get(&id_val) {
                                    *session.kill_switch.lock().unwrap() =
                                        Some("You were disconnected by the server owner.".to_string());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        while let Ok(msg) = rx.try_recv() {
            let mut s = sessions.lock().unwrap();
            match msg {
                EventMsg::Connected { .. } => {}
                EventMsg::Disconnected { id } => {
                    s.remove(&id);
                    if selected_index >= s.len() && selected_index > 0 {
                        selected_index -= 1;
                    }
                }
                EventMsg::Traffic { id, bytes } => {
                    if let Some(session) = s.get_mut(&id) {
                        session.total_bytes += bytes;
                        session.current_kbps = (bytes as f64 / 1024.0) * 10.0;
                    }
                }
            }
        }

        tick_count += 1;
        if tick_count % 10 == 0 {
            let mut s = sessions.lock().unwrap();
            for session in s.values_mut() {
                session.current_kbps *= 0.9;
            }
        }
    }

    disable_raw_mode()?;
    execute!(std::io::stdout(), LeaveAlternateScreen)?;
    Ok(())
}