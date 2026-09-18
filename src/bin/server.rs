use futures_util::{StreamExt, SinkExt};
use voe::{load_config, ServerConfig, decode, decrypt_data, encrypt_data};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, Duration};
use tokio::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use ratatui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders, List, ListItem, Paragraph},
    layout::{Layout, Constraint, Direction},
    Terminal,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

enum EventMsg {
    Connected { id: usize, name: String },
    Disconnected { id: usize },
    Traffic { id: usize, bytes: usize },
}

struct UserSession {
    name: String,
    current_kbps: f64,
    total_bytes: usize,
    kill_switch: Arc<Mutex<bool>>,
}

struct RateLimiter {
    bytes_per_sec: u32,
    bytes_sent_this_window: usize,
    last_window_start: std::time::Instant,
}

impl RateLimiter {
    fn new(kbps: u32) -> Self {
        Self {
            bytes_per_sec: kbps * 1024,
            bytes_sent_this_window: 0,
            last_window_start: std::time::Instant::now(),
        }
    }
    async fn limit(&mut self, amount: usize) {
        if self.bytes_per_sec == 0 { return; }
        self.bytes_sent_this_window += amount;
        let elapsed = self.last_window_start.elapsed();
        if elapsed.as_secs() >= 1 {
            self.bytes_sent_this_window = 0;
            self.last_window_start = std::time::Instant::now();
        } else if self.bytes_sent_this_window > self.bytes_per_sec as usize {
            sleep(Duration::from_millis(50)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: ServerConfig = load_config("config-server.yml");
    let (tx, mut rx) = mpsc::channel::<EventMsg>(1000);
    
    let sessions: Arc<Mutex<HashMap<usize, UserSession>>> = Arc::new(Mutex::new(HashMap::new()));
    
    let sessions_clone = Arc::clone(&sessions);
    let tx_clone = tx.clone();
    let cfg = config.clone();

    tokio::spawn(async move {
        let listener = TcpListener::bind(&cfg.listen_addr).await.unwrap();
        let mut id_counter = 0;

        while let Ok((stream, _)) = listener.accept().await {
            id_counter += 1;
            let current_id = id_counter;
            let cfg_inner = cfg.clone();
            let tx_inner = tx_clone.clone();
            let sessions_inner = Arc::clone(&sessions_clone);

            tokio::spawn(async move {
                let ws_stream = match accept_async(stream).await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let (mut ws_write, mut ws_read) = ws_stream.split();

                let mut authenticated_user = None;
                if let Some(Ok(msg)) = ws_read.next().await {
                    let auth = decode(msg.to_text().unwrap_or(""));
                    let parts: Vec<&str> = auth.split(':').collect();
                    if parts.len() == 2 {
                        if let Some(user) = cfg_inner.users.iter().find(|u| u.username == parts[0] && u.password == parts[1]) {
                            authenticated_user = Some(user.clone());
                        }
                    }
                }

                let user = match authenticated_user {
                    Some(u) => u,
                    None => return,
                };

                let kill_switch = Arc::new(Mutex::new(false));
                let kill_switch_clone = Arc::clone(&kill_switch);
                
                {
                    let mut s = sessions_inner.lock().unwrap();
                    s.insert(current_id, UserSession {
                        name: user.display_name.clone(),
                        current_kbps: 0.0,
                        total_bytes: 0,
                        kill_switch: Arc::clone(&kill_switch),
                    });
                }
                let _ = tx_inner.send(EventMsg::Connected { id: current_id, name: user.display_name.clone() }).await;

                let kbps = user.max_kbps.unwrap_or(u32::MAX);
                let mut tx_limiter = RateLimiter::new(kbps);
                let mut rx_limiter = RateLimiter::new(kbps);

                if let Some(Ok(msg)) = ws_read.next().await {
                    let encrypted_data = msg.into_data();
                    let socks_req = match decrypt_data(&cfg_inner.secret_key, &encrypted_data) {
                        Some(data) => data,
                        None => return,
                    };

                    if socks_req.len() < 4 { return; }
                    let atyp = socks_req[3];
                    let (host, port_offset) = match atyp {
                        1 => (format!("{}.{}.{}.{}", socks_req[4], socks_req[5], socks_req[6], socks_req[7]), 8),
                        3 => {
                            let len = socks_req[4] as usize;
                            (String::from_utf8_lossy(&socks_req[5..5+len]).to_string(), 5 + len)
                        }
                        _ => return,
                    };
                    let port = u16::from_be_bytes([socks_req[port_offset], socks_req[port_offset+1]]);
                    let target = format!("{}:{}", host, port);

                    match TcpStream::connect(&target).await {
                        Ok(mut tcp_stream) => {
                            let (mut tcp_read, mut tcp_write) = tcp_stream.split();
                            let resp = vec![0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
                            let enc_resp = encrypt_data(&cfg_inner.secret_key, &resp);
                            ws_write.send(tokio_tungstenite::tungstenite::Message::Binary(enc_resp)).await.ok();

                            let client_to_server = async {
                                while let Some(Ok(m)) = ws_read.next().await {
                                    if *kill_switch_clone.lock().unwrap() { break; }
                                    let enc_data = m.into_data();
                                    rx_limiter.limit(enc_data.len()).await;
                                    let _ = tx_inner.send(EventMsg::Traffic { id: current_id, bytes: enc_data.len() }).await;
                                    if let Some(dec_data) = decrypt_data(&cfg_inner.secret_key, &enc_data) {
                                        if tcp_write.write_all(&dec_data).await.is_err() { break; }
                                    }
                                }
                            };

                            let server_to_client = async {
                                let mut buf = [0u8; 8192];
                                while let Ok(n) = tcp_read.read(&mut buf).await {
                                    if n == 0 { break; }
                                    if *kill_switch_clone.lock().unwrap() { break; }
                                    tx_limiter.limit(n).await;
                                    let _ = tx_inner.send(EventMsg::Traffic { id: current_id, bytes: n }).await;
                                    let enc_data = encrypt_data(&cfg_inner.secret_key, &buf[..n]);
                                    if ws_write.send(tokio_tungstenite::tungstenite::Message::Binary(enc_data)).await.is_err() { break; }
                                }
                            };
                            tokio::select! { _ = client_to_server => {}, _ = server_to_client => {}, }
                        }
                        Err(_) => {
                            let fail = vec![0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
                            let enc_fail = encrypt_data(&cfg_inner.secret_key, &fail);
                            ws_write.send(tokio_tungstenite::tungstenite::Message::Binary(enc_fail)).await.ok();
                        }
                    }
                }
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

    loop {
        terminal.draw(|f| {
            let size = f.size();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(3)])
                .split(size);

            f.render_widget(Paragraph::new("VOE Secure Server Management").block(Block::default().borders(Borders::ALL)), chunks[0]);

            let sessions_lock = sessions.lock().unwrap();
            let mut sorted_sessions: Vec<(usize, &UserSession)> = sessions_lock
                .iter()
                .map(|(&id, session)| (id, session))
                .collect();
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
                
                let text = format!("ID: {} | {} | {} | Total: {} MB", 
                    id, session.name, speed, session.total_bytes / 1048576);
                
                if i == active_index {
                    list_items.push(ListItem::new(text).style(
                        ratatui::style::Style::default()
                            .fg(ratatui::style::Color::Black)
                            .bg(ratatui::style::Color::Yellow)
                            .add_modifier(ratatui::style::Modifier::BOLD)
                    ));
                } else {
                    list_items.push(ListItem::new(text));
                }
            }

            f.render_widget(List::new(list_items).block(Block::default().title("Active Users (Arrows to move, Enter to Kick)").borders(Borders::ALL)), chunks[1]);
            f.render_widget(Paragraph::new("Press 'q' to quit").block(Block::default().borders(Borders::ALL)), chunks[2]);
        })?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => break,
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
                                let mut kill = session.kill_switch.lock().unwrap();
                                *kill = true;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        while let Ok(msg) = rx.try_recv() {
            let mut s = sessions.lock().unwrap();
            match msg {
                EventMsg::Connected { .. } => {},
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