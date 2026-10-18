use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::net::IpAddr;

static BLOCKLIST_BYTES: &[u8] = include_bytes!("../resources/adblocklist.txt");

static BLOCKED: Lazy<HashSet<String>> = Lazy::new(|| parse_blocklist(BLOCKLIST_BYTES));

fn parse_blocklist(bytes: &[u8]) -> HashSet<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(parse_line)
        .collect()
}

fn parse_line(line: &str) -> Option<String> {
    let line = line.split('#').next()?.trim();
    if line.is_empty() {
        return None;
    }

    let mut tokens = line.split_whitespace();
    let first = tokens.next()?;
    let raw = if first.parse::<IpAddr>().is_ok() {
        tokens.next()?
    } else {
        first
    };

    let domain = raw
        .trim_start_matches("||")
        .trim_start_matches("*.")
        .trim_end_matches('^')
        .trim_end_matches('.')
        .to_ascii_lowercase();

    if domain.is_empty() || domain == "localhost" || !domain.contains('.') {
        return None;
    }
    Some(domain)
}

pub fn is_blocked(host: &str) -> bool {
    let host = host
        .split(':')
        .next()
        .unwrap_or(host)
        .trim_end_matches('.')
        .to_ascii_lowercase();

    let mut candidate = host.as_str();
    loop {
        if BLOCKED.contains(candidate) {
            return true;
        }
        match candidate.split_once('.') {
            Some((_, rest)) if rest.contains('.') => candidate = rest,
            _ => return false,
        }
    }
}