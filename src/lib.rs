use serde::{Deserialize, Serialize};
use std::fs;
use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::{aead::{Aead, KeyInit}};
use rand::{RngCore, rngs::OsRng};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct User {
    pub username: String,
    pub password: String,
    pub display_name: String,
    pub max_kbps: Option<u32>, 
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub secret_key: String,
    pub users: Vec<User>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub server_url: String,
    pub local_listen_addr: String,
    pub username: String,
    pub password: String,
    pub secret_key: String,
}

pub fn encode(text: &str) -> String {
    if text.is_empty() { return "".to_string(); }
    let input_bytes = text.as_bytes();
    let mut result = String::with_capacity(input_bytes.len() * 2);
    for (i, &b_orig) in input_bytes.iter().enumerate() {
        let mut b = b_orig as u8;
        if i % 2 == 0 { b ^= ((i * 33 + 7) & 0xFF) as u8; } 
        else { b = (b ^ 0xFF) ^ ((i * 13 + 3) & 0xFF) as u8; }
        let high_nibble = (b & 0xF0) >> 4;
        let low_nibble = (b & 0x0F) << 4;
        b = high_nibble | low_nibble;
        b = ((b << 3) | (b >> 5)) & 0xFF;
        result.push_str(&format!("{:02x}", b));
    }
    result
}

pub fn decode(hex_str: &str) -> String {
    if hex_str.is_empty() { return "".to_string(); }
    let bytes = hex::decode(hex_str).unwrap_or_default();
    let mut decoded_bytes = Vec::with_capacity(bytes.len());
    for (idx, &byte_val) in bytes.iter().enumerate() {
        let mut b = byte_val;
        b = ((b >> 3) | (b << 5)) & 0xFF;
        let high_nibble = (b & 0xF0) >> 4;
        let low_nibble = (b & 0x0F) << 4;
        b = high_nibble | low_nibble;
        if idx % 2 == 0 { b ^= ((idx * 33 + 7) & 0xFF) as u8; } 
        else { b = (b ^ ((idx * 13 + 3) & 0xFF) as u8) ^ 0xFF; }
        decoded_bytes.push(b);
    }
    String::from_utf8_lossy(&decoded_bytes).into_owned()
}

pub fn encrypt_data(key_str: &str, plaintext: &[u8]) -> Vec<u8> {
    let key = Key::<Aes256Gcm>::from_slice(key_str.as_bytes());
    let cipher = Aes256Gcm::new(key);
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher.encrypt(nonce, plaintext).expect("Encryption failure");
    let mut result = nonce_bytes.to_vec();
    result.extend(ciphertext);
    result
}

pub fn decrypt_data(key_str: &str, ciphertext_with_nonce: &[u8]) -> Option<Vec<u8>> {
    if ciphertext_with_nonce.len() < 12 { return None; }
    let key = Key::<Aes256Gcm>::from_slice(key_str.as_bytes());
    let cipher = Aes256Gcm::new(key);
    let (nonce_bytes, ciphertext) = ciphertext_with_nonce.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher.decrypt(nonce, ciphertext).ok()
}

pub fn load_config<T: for<'de> Deserialize<'de>>(path: &str) -> T {
    let content = fs::read_to_string(path).expect("Config file missing");
    serde_yaml::from_str(&content).expect("Invalid YAML")
}