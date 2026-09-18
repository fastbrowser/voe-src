slint::slint! {
    import { Button, LineEdit, VerticalBox, HorizontalBox } from "std-widgets.slint";

    export component AppWindow inherits Window {
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
        in-out property <bool> is_running: false;

        callback request_start();
        callback request_stop();
        callback request_save();
        callback password_changed(string);
        callback secret_changed(string);
        callback toggle_password();
        callback toggle_secret();

        title: "VOE Secure Client Control Panel";
        width: 550px;
        height: 600px;
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
                    color: root.is_running ? #00ff00 : #ff4444;
                    horizontal-alignment: center;
                    font-size: 16px;
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

use std::fs;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use std::collections::VecDeque;
use voe::{load_config, ClientConfig, encode, encrypt_data, decrypt_data};

type WsStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct ConnectionPool {
    conns: VecDeque<WsStream>,
    config: ClientConfig,
}

impl ConnectionPool {
    async fn get(&mut self) -> WsStream {
        if let Some(conn) = self.conns.pop_front() { return conn; }
        let (ws, _) = connect_async(&self.config.server_url).await.expect("Server unreachable");
        let mut ws = ws;
        let auth_payload = format!("{}:{}", self.config.username, self.config.password);
        ws.send(Message::Text(encode(&auth_payload))).await.expect("Auth failed");
        ws
    }
}

async fn run_proxy(config: ClientConfig, mut stop_rx: watch::Receiver<bool>) {
    let pool = Arc::new(Mutex::new(ConnectionPool { conns: VecDeque::new(), config: config.clone() }));
    let listener = match TcpListener::bind(&config.local_listen_addr).await { Ok(l) => l, Err(_) => return };
    loop {
        tokio::select! {
            _ = stop_rx.changed() => { if *stop_rx.borrow() { break; } }
            Ok((mut stream, _)) = listener.accept() => {
                let pool_clone = Arc::clone(&pool);
                let cfg = config.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    if let Ok(n) = stream.read(&mut buf).await {
                        if n < 2 || buf[0] != 0x05 { return; }
                        if stream.write_all(&[0x05, 0x00]).await.is_err() { return; }
                    } else { return; }
                    if let Ok(n) = stream.read(&mut buf).await {
                        if n < 4 || buf[1] != 0x01 { return; }
                        let mut pool_lock = pool_clone.lock().await;
                        let ws_stream = pool_lock.get().await;
                        drop(pool_lock);
                        let (mut ws_write, mut ws_read) = ws_stream.split();
                        let enc_req = encrypt_data(&cfg.secret_key, &buf[..n]);
                        ws_write.send(Message::Binary(enc_req)).await.ok();
                        if let Some(Ok(msg)) = ws_read.next().await {
                            if let Message::Binary(enc_resp) = msg {
                                if let Some(dec_resp) = decrypt_data(&cfg.secret_key, &enc_resp) {
                                    stream.write_all(&dec_resp).await.ok();
                                }
                            }
                        }
                        let (mut client_read, mut client_write) = tokio::io::split(stream);
                        let client_to_server = async {
                            let mut buf = [0u8; 8192];
                            while let Ok(n) = client_read.read(&mut buf).await {
                                if n == 0 { break; }
                                let enc_data = encrypt_data(&cfg.secret_key, &buf[..n]);
                                if ws_write.send(Message::Binary(enc_data)).await.is_err() { break; }
                            }
                        };
                        let server_to_client = async {
                            while let Some(Ok(msg)) = ws_read.next().await {
                                if let Message::Binary(enc_data) = msg {
                                    if let Some(dec_data) = decrypt_data(&cfg.secret_key, &enc_data) {
                                        if client_write.write_all(&dec_data).await.is_err() { break; }
                                    }
                                }
                            }
                        };
                        tokio::select! { _ = client_to_server => {}, _ = server_to_client => {}, }
                    }
                });
            }
        }
    }
}

fn main() -> Result<(), slint::PlatformError> {
    let ui = AppWindow::new()?;
    let ui_handle = ui.as_weak();
    let initial_cfg = load_config::<ClientConfig>("config-client.yml");
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
        if show { ui.set_password_display(ui.get_password().into()); } 
        else { ui.set_password_display("●".repeat(ui.get_password().len()).into()); }
    });

    let ui_s = ui.as_weak();
    ui.on_toggle_secret(move || {
        let ui = ui_s.unwrap();
        let show = !ui.get_show_secret();
        ui.set_show_secret(show);
        if show { ui.set_secret_display(ui.get_secret_key().into()); } 
        else { ui.set_secret_display("●".repeat(ui.get_secret_key().len()).into()); }
    });

    let ui_p = ui.as_weak();
    ui.on_password_changed(move |text| {
        let ui = ui_p.unwrap();
        let val = text.to_string();
        ui.set_password(val.clone().into());
        if !ui.get_show_password() { ui.set_password_display("●".repeat(val.len()).into()); }
    });

    let ui_sk = ui.as_weak();
    ui.on_secret_changed(move |text| {
        let ui = ui_sk.unwrap();
        let val = text.to_string();
        ui.set_secret_key(val.clone().into());
        if !ui.get_show_password() { ui.set_secret_display("●".repeat(val.len()).into()); }
    });

    let (stop_tx, _stop_rx) = tokio::sync::watch::channel(false);
    let stop_tx_clone = stop_tx.clone();

    ui.on_request_start({
        let ui_h = ui_handle.clone();
        let stop_tx = stop_tx_clone.clone();
        move || {
            let ui = ui_h.unwrap();
            let config = ClientConfig {
                server_url: ui.get_server_url().into(),
                local_listen_addr: ui.get_local_addr().into(),
                username: ui.get_username().into(),
                password: ui.get_password().into(),
                secret_key: ui.get_secret_key().into(),
            };
            let stop_rx = stop_tx.subscribe();
            tokio::spawn(async move { run_proxy(config, stop_rx).await; });
            ui.set_status("Running".into());
            ui.set_is_running(true);
        }
    });

    ui.on_request_stop({
        let ui_h = ui_handle.clone();
        let stop_tx = stop_tx_clone.clone();
        move || {
            let ui = ui_h.unwrap();
            let _ = stop_tx.send(true);
            ui.set_status("Stopped".into());
            ui.set_is_running(false);
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