use tokio::sync::mpsc;

pub const OPEN: u8 = 0;
pub const DATA: u8 = 1;
pub const CLOSE: u8 = 2;
pub const OPEN_OK: u8 = 3;
pub const OPEN_FAIL: u8 = 4;

pub fn encode_frame(stream_id: u32, kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.extend_from_slice(&stream_id.to_le_bytes());
    buf.push(kind);
    buf.extend_from_slice(payload);
    buf
}

pub fn decode_frame(raw: &[u8]) -> Option<(u32, u8, &[u8])> {
    if raw.len() < 5 {
        return None;
    }
    let id = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    Some((id, raw[4], &raw[5..]))
}

pub type StreamTx = mpsc::Sender<Vec<u8>>;