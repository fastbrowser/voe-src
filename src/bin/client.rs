slint::slint! {
    import { Button, LineEdit, VerticalBox, HorizontalBox } from "std-widgets.slint";

    export component AppWindow inherits Window {
        in property <string> version: "";
        in-out property <string> server_url: "";
        in-out property <string> local_addr: "";
        in-out property <string> username: "";
        in-out property <string> password: "";
        in-out property <string> secret_key: "";
        in-out property <string> password_display: "";
        in-out property <string> secret_display: "";
        in-out property <bool> show_password: false;
        in-out property <bool> show_secret: false;
        in-out property <string> status: "Stopped";
        in-out property <string> server_message: "";
        in-out property <bool> is_running: false;
        in-out property <bool> is_connected: false;

        callback request_start();
        callback request_stop();
        callback request_save();
        callback password_changed(string);
        callback secret_changed(string);
        callback toggle_password();
        callback toggle_secret();

        title: "voe client " + root.version;
        width: 550px;
        height: 660px;
        background: #1e1e1e;

        Flickable {
            VerticalBox {
                padding: 40px;
                spacing: 25px;
                alignment: center;

                Text {
                    text: "Proxy Configuration";
                    color: #ffffff;
                    font-size: 22px;
                    horizontal-alignment: center;
                }

                VerticalBox {
                    spacing: 5px;
                    Text { text: "Server URL"; color: #aaa; }
                    LineEdit { text: root.server_url; edited(text) => { root.server_url = text; } }
                }

                VerticalBox {
                    spacing: 5px;
                    Text { text: "Local Listen Address"; color: #aaa; }
                    LineEdit { text: root.local_addr; edited(text) => { root.local_addr = text; } }
                }

                VerticalBox {
                    spacing: 5px;
                    Text { text: "Username"; color: #aaa; }
                    LineEdit { text: root.username; edited(text) => { root.username = text; } }
                }

                HorizontalBox {
                    spacing: 10px;
                    VerticalBox {
                        spacing: 2px;
                        Text { text: "Password"; color: #aaa; font-size: 12px; }
                        LineEdit { text: root.password_display; edited(text) => { root.password_changed(text); } }
                    }
                    Button {
                        text: root.show_password ? "Hide" : "Show";
                        width: 60px;
                        clicked => { root.toggle_password(); }
                    }
                }

                HorizontalBox {
                    spacing: 10px;
                    VerticalBox {
                        spacing: 2px;
                        Text { text: "Secret Key (32 chars)"; color: #aaa; font-size: 12px; }
                        LineEdit { text: root.secret_display; edited(text) => { root.secret_changed(text); } }
                    }
                    Button {
                        text: root.show_secret ? "Hide" : "Show";
                        width: 60px;
                        clicked => { root.toggle_secret(); }
                    }
                }

                Text {
                    text: "Status: " + root.status;
                    color: root.is_connected ? #00ff00 : #ff4444;
                    horizontal-alignment: center;
                    font-size: 16px;
                }

                if root.server_message != "" : Text {
                    text: root.server_message;
                    color: #ffb347;
                    wrap: word-wrap;
                    horizontal-alignment: center;
                    font-size: 14px;
                }

                HorizontalBox {
                    alignment: center;
                    spacing: 20px;
                    Button {
                        text: root.is_running ? "Stop Proxy" : "Start Proxy";
                        width: 150px;
                        clicked => { if (root.is_running) { root.request_stop(); } else { root.request_start(); } }
                    }
                    Button {
                        text: "Save Config";
                        width: 150px;
                        clicked => { root.request_save(); }
                    }
                }
            }
        }
    }
}

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream};

use voe::mux::{decode_frame, encode_frame, CLOSE, DATA, OPEN, OPEN_OK};
use voe::{ctrl_frame, decode_ctrl, decrypt_data, encode, encrypt_data, load_config, ClientConfig, CTRL, VERSION};

type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;

type StreamMsg = (u8, Vec<u8>);

const PING_EVERY: Duration = Duration::from_secs(5);
const LINK_TIMEOUT: Duration = Duration::from_secs(20);
const SOCKS_FAIL: [u8; 10] = [0x05, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

#[derive(Clone)]
struct Reporter {
    ui: slint::Weak<AppWindow>,
}

impl Reporter {
    fn status(&self, text: impl Into<String>, connected: bool) {
        let text: String = text.into();
        let _ = self.ui.upgrade_in_event_loop(move |ui| {
            ui.set_status(text.into());
            ui.set_is_connected(connected);
        });
    }

    fn message(&self, text: impl Into<String>) {
        let text: String = text.into();
        let _ = self.ui.upgrade_in_event_loop(move |ui| ui.set_server_message(text.into()));
    }

    fn finished(&self) {
        let _ = self.ui.upgrade_in_event_loop(|ui| {
            ui.set_status("Stopped".into());
            ui.set_is_running(false);
            ui.set_is_connected(false);
        });
    }
}

enum ConnectError {
    Fatal(String),
    Failed,
}

struct MuxClient {
    ws_write: Arc<AsyncMutex<WsSink>>,
    routes: Arc<StdMutex<HashMap<u32, mpsc::Sender<StreamMsg>>>>,
    next_id: AtomicU32,
    secret: String,
    dead: watch::Receiver<bool>,
    last_rx: Arc<StdMutex<Instant>>,
    fatal: Arc<StdMutex<Option<String>>>,
}

impl MuxClient {
    async fn connect(config: &ClientConfig, rep: Reporter) -> Result<Self, ConnectError> {
        let (ws, _) = match timeout(Duration::from_secs(10), connect_async(&config.server_url)).await {
            Ok(Ok(v)) => v,
            _ => return Err(ConnectError::Failed),
        };
        let (mut write, mut read) = ws.split();
        let secret = config.secret_key.clone();

        let auth = format!("{}:{}", config.username, config.password);
        write
            .send(Message::Text(encode(&auth)))
            .await
            .map_err(|_| ConnectError::Failed)?;
        write
            .send(Message::Binary(ctrl_frame(&secret, &format!("hello:{}", VERSION))))
            .await
            .map_err(|_| ConnectError::Failed)?;

        let first = timeout(Duration::from_secs(30), async {
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Binary(enc) = msg {
                    if let Some(text) = decode_ctrl(&secret, &enc) {
                        return Some(text);
                    }
                }
            }
            None
        })
        .await;
        match first {
            Ok(Some(text)) => {
                if text.starts_with("welcome:") {
                } else if let Some(reason) = text.strip_prefix("fatal:") {
                    return Err(ConnectError::Fatal(reason.to_string()));
                } else {
                    return Err(ConnectError::Failed);
                }
            }
            _ => return Err(ConnectError::Failed),
        }

        let routes: Arc<StdMutex<HashMap<u32, mpsc::Sender<StreamMsg>>>> = Arc::new(StdMutex::new(HashMap::new()));
        let last_rx = Arc::new(StdMutex::new(Instant::now()));
        let fatal: Arc<StdMutex<Option<String>>> = Arc::new(StdMutex::new(None));
        let (dead_tx, dead_rx) = watch::channel(false);

        let routes_demux = Arc::clone(&routes);
        let last_rx_demux = Arc::clone(&last_rx);
        let fatal_demux = Arc::clone(&fatal);
        let secret_demux = secret.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Binary(enc) = msg {
                    let Some(raw) = decrypt_data(&secret_demux, &enc) else { continue };
                    let Some((sid, kind, payload)) = decode_frame(&raw) else { continue };
                    *last_rx_demux.lock().unwrap() = Instant::now();

                    if kind == CTRL {
                        let text = String::from_utf8_lossy(payload).into_owned();
                        if let Some(reason) = text.strip_prefix("fatal:") {
                            *fatal_demux.lock().unwrap() = Some(reason.to_string());
                            rep.message(reason);
                        } else if let Some(note) = text.strip_prefix("notice:") {
                            rep.message(note);
                        }
                        continue;
                    }

                    let tx = routes_demux.lock().unwrap().get(&sid).cloned();
                    if let Some(tx) = tx {
                        let _ = tx.send((kind, payload.to_vec())).await;
                    }
                }
            }
            routes_demux.lock().unwrap().clear();
            let _ = dead_tx.send(true);
        });

        Ok(Self {
            ws_write: Arc::new(AsyncMutex::new(write)),
            routes,
            next_id: AtomicU32::new(1),
            secret,
            dead: dead_rx,
            last_rx,
            fatal,
        })
    }

    async fn send_frame(&self, sid: u32, kind: u8, payload: &[u8]) -> bool {
        let f = encrypt_data(&self.secret, &encode_frame(sid, kind, payload));
        self.ws_write.lock().await.send(Message::Binary(f)).await.is_ok()
    }

    async fn send_ctrl(&self, text: &str) -> bool {
        let f = ctrl_frame(&self.secret, text);
        self.ws_write.lock().await.send(Message::Binary(f)).await.is_ok()
    }

    async fn closed(&self) {
        let mut rx = self.dead.clone();
        loop {
            if *rx.borrow() {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    async fn shutdown(&self) {
        let _ = timeout(Duration::from_secs(2), async {
            self.ws_write.lock().await.close().await.ok();
        })
        .await;
    }

    fn take_fatal(&self) -> Option<String> {
        self.fatal.lock().unwrap().take()
    }

    fn register(&self) -> (u32, mpsc::Receiver<StreamMsg>) {
        let sid = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(256);
        self.routes.lock().unwrap().insert(sid, tx);
        (sid, rx)
    }

    fn deregister(&self, sid: u32) {
        self.routes.lock().unwrap().remove(&sid);
    }
}

type Link = Option<Arc<MuxClient>>;

async fn wait_stop(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

async fn supervise(
    config: &ClientConfig,
    link_tx: &watch::Sender<Link>,
    stop_rx: &mut watch::Receiver<bool>,
    rep: &Reporter,
) {
    let mut first_attempt = true;
    let mut fails: u32 = 0;

    loop {
        if *stop_rx.borrow() {
            return;
        }
        rep.status(if first_attempt { "Connecting..." } else { "Reconnecting..." }, false);
        first_attempt = false;

        let result = tokio::select! {
            _ = wait_stop(stop_rx) => return,
            r = MuxClient::connect(config, rep.clone()) => r,
        };

        match result {
            Ok(mux) => {
                let mux = Arc::new(mux);
                fails = 0;
                rep.message("");
                rep.status("Connected", true);
                let _ = link_tx.send(Some(Arc::clone(&mux)));

                let mut tick = tokio::time::interval(PING_EVERY);
                let stopped = loop {
                    tokio::select! {
                        _ = wait_stop(stop_rx) => break true,
                        _ = mux.closed() => break false,
                        _ = tick.tick() => {
                            if mux.last_rx.lock().unwrap().elapsed() > LINK_TIMEOUT {
                                break false;
                            }
                            match timeout(Duration::from_secs(5), mux.send_ctrl("ping")).await {
                                Ok(true) => {}
                                _ => break false,
                            }
                        }
                    }
                };

                let _ = link_tx.send(None);
                mux.shutdown().await;
                if stopped {
                    return;
                }
                if mux.take_fatal().is_some() {
                    return;
                }
                rep.status("Connection lost", false);
            }
            Err(ConnectError::Fatal(reason)) => {
                rep.message(reason);
                return;
            }
            Err(ConnectError::Failed) => {
                fails += 1;
                if fails >= 3 {
                    rep.message("Can't connect to server, please check the server ip, username, password and secret key.");
                }
            }
        }

        let delay = (1u64 << fails.min(5)).min(30);
        rep.status(format!("Reconnecting in {}s...", delay), false);
        tokio::select! {
            _ = sleep(Duration::from_secs(delay)) => {}
            _ = wait_stop(stop_rx) => return,
        }
    }
}

async fn handle_socks(mut stream: TcpStream, mut link_rx: watch::Receiver<Link>) {
    stream.set_nodelay(true).ok();

    let mut buf = [0u8; 1024];
    match stream.read(&mut buf).await {
        Ok(n) if n >= 2 && buf[0] == 0x05 => {}
        _ => return,
    }
    if stream.write_all(&[0x05, 0x00]).await.is_err() {
        return;
    }
    let n = match stream.read(&mut buf).await {
        Ok(n) if n >= 4 && buf[1] == 0x01 => n,
        _ => return,
    };

    let wait_link = async {
        loop {
            let current = link_rx.borrow().clone();
            if let Some(m) = current {
                return Some(m);
            }
            if link_rx.changed().await.is_err() {
                return None;
            }
        }
    };
    let mux = match timeout(Duration::from_secs(10), wait_link).await {
        Ok(Some(m)) => m,
        _ => {
            stream.write_all(&SOCKS_FAIL).await.ok();
            return;
        }
    };

    let (sid, mut rx) = mux.register();

    if !mux.send_frame(sid, OPEN, &buf[..n]).await {
        mux.deregister(sid);
        stream.write_all(&SOCKS_FAIL).await.ok();
        return;
    }

    match rx.recv().await {
        Some((OPEN_OK, _)) => {
            let resp = [0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
            if stream.write_all(&resp).await.is_err() {
                mux.deregister(sid);
                return;
            }
        }
        _ => {
            stream.write_all(&SOCKS_FAIL).await.ok();
            mux.deregister(sid);
            return;
        }
    }

    let (mut client_read, mut client_write) = tokio::io::split(stream);
    let mux_up = Arc::clone(&mux);

    let upload = async move {
        let mut buf = [0u8; 8192];
        loop {
            match client_read.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if !mux_up.send_frame(sid, DATA, &buf[..n]).await {
                        break;
                    }
                }
            }
        }
        mux_up.send_frame(sid, CLOSE, &[]).await;
    };
    let download = async move {
        while let Some((kind, payload)) = rx.recv().await {
            match kind {
                DATA => {
                    if client_write.write_all(&payload).await.is_err() {
                        break;
                    }
                }
                CLOSE => break,
                _ => {}
            }
        }
    };
    tokio::select! { _ = upload => {}, _ = download => {} }
    mux.deregister(sid);
}

async fn accept_loop(listener: TcpListener, link_rx: watch::Receiver<Link>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_socks(stream, link_rx.clone()));
            }
            Err(_) => sleep(Duration::from_millis(50)).await,
        }
    }
}

async fn run_proxy(config: ClientConfig, mut stop_rx: watch::Receiver<bool>, rep: Reporter) {
    match TcpListener::bind(&config.local_listen_addr).await {
        Ok(listener) => {
            let (link_tx, link_rx) = watch::channel::<Link>(None);
            let accept_task = tokio::spawn(accept_loop(listener, link_rx));
            supervise(&config, &link_tx, &mut stop_rx, &rep).await;
            accept_task.abort();
        }
        Err(e) => {
            rep.message(format!("Can't listen on {}: {}", config.local_listen_addr, e));
        }
    }
    rep.finished();
}

fn main() -> Result<(), slint::PlatformError> {
    let ui = AppWindow::new()?;
    let ui_handle = ui.as_weak();
    let initial_cfg = load_config::<ClientConfig>("config-client.yml");
    ui.set_version(VERSION.into());
    ui.set_server_url(initial_cfg.server_url.clone().into());
    ui.set_local_addr(initial_cfg.local_listen_addr.clone().into());
    ui.set_username(initial_cfg.username.clone().into());
    ui.set_password(initial_cfg.password.clone().into());
    ui.set_secret_key(initial_cfg.secret_key.clone().into());
    ui.set_password_display("●".repeat(initial_cfg.password.len()).into());
    ui.set_secret_display("●".repeat(initial_cfg.secret_key.len()).into());

    let ui_t = ui.as_weak();
    ui.on_toggle_password(move || {
        let ui = ui_t.unwrap();
        let show = !ui.get_show_password();
        ui.set_show_password(show);
        if show {
            ui.set_password_display(ui.get_password().into());
        } else {
            ui.set_password_display("●".repeat(ui.get_password().len()).into());
        }
    });

    let ui_s = ui.as_weak();
    ui.on_toggle_secret(move || {
        let ui = ui_s.unwrap();
        let show = !ui.get_show_secret();
        ui.set_show_secret(show);
        if show {
            ui.set_secret_display(ui.get_secret_key().into());
        } else {
            ui.set_secret_display("●".repeat(ui.get_secret_key().len()).into());
        }
    });

    let ui_p = ui.as_weak();
    ui.on_password_changed(move |text| {
        let ui = ui_p.unwrap();
        let val = text.to_string();
        ui.set_password(val.clone().into());
        if !ui.get_show_password() {
            ui.set_password_display("●".repeat(val.len()).into());
        }
    });

    let ui_sk = ui.as_weak();
    ui.on_secret_changed(move |text| {
        let ui = ui_sk.unwrap();
        let val = text.to_string();
        ui.set_secret_key(val.clone().into());
        if !ui.get_show_secret() {
            ui.set_secret_display("●".repeat(val.len()).into());
        }
    });

    let (stop_tx, _stop_rx) = watch::channel(false);

    ui.on_request_start({
        let ui_h = ui_handle.clone();
        let stop_tx = stop_tx.clone();
        move || {
            let ui = ui_h.unwrap();
            let config = ClientConfig {
                server_url: ui.get_server_url().into(),
                local_listen_addr: ui.get_local_addr().into(),
                username: ui.get_username().into(),
                password: ui.get_password().into(),
                secret_key: ui.get_secret_key().into(),
            };
            if config.secret_key.len() != 32 {
                ui.set_server_message("Secret key must be exactly 32 characters.".into());
                return;
            }
            ui.set_server_message("".into());
            ui.set_status("Connecting...".into());
            ui.set_is_running(true);
            stop_tx.send_replace(false);
            let stop_rx = stop_tx.subscribe();
            let rep = Reporter { ui: ui_h.clone() };
            tokio::spawn(async move {
                run_proxy(config, stop_rx, rep).await;
            });
        }
    });

    ui.on_request_stop({
        let ui_h = ui_handle.clone();
        let stop_tx = stop_tx.clone();
        move || {
            let ui = ui_h.unwrap();
            let _ = stop_tx.send(true);
            ui.set_status("Stopping...".into());
        }
    });

    ui.on_request_save({
        let ui_h = ui_handle.clone();
        move || {
            let ui = ui_h.unwrap();
            let config = ClientConfig {
                server_url: ui.get_server_url().into(),
                local_listen_addr: ui.get_local_addr().into(),
                username: ui.get_username().into(),
                password: ui.get_password().into(),
                secret_key: ui.get_secret_key().into(),
            };
            fs::write("config-client.yml", serde_yaml::to_string(&config).unwrap()).ok();
        }
    });

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { ui.run() })
}