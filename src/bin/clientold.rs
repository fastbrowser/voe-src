use futures_util::{StreamExt, SinkExt};
use voe::{load_config, ClientConfig, encode, encrypt_data, decrypt_data};
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::VecDeque;

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

#[tokio::main]
async fn main() {
    let config: ClientConfig = load_config("config-client.yml");
    let pool = Arc::new(Mutex::new(ConnectionPool {
        conns: VecDeque::new(),
        config: config.clone(),
    }));

    let listener = TcpListener::bind(&config.local_listen_addr).await.expect("Bind failed");
    println!("VOE Secure Client listening on {}", config.local_listen_addr);

    while let Ok((mut stream, _)) = listener.accept().await {
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