# Description
Voe is a toy proxying app I have made except the protocol is SOCKS-over-HTTP. This means servers are hostable through things like temporary HTTP tunnels

All traffic between client and server is encrypted with AES-256-GCM using a shared `secret_key` (it must be exactly 32 characters).

# Usage
## Client
* Download the client from releases or build from source
* Make a config-client.yml in the same directory as the client.
* Example:
```
server_url: ws://127.0.0.1:1234
local_listen_addr: 127.0.0.1:1080
username: username
password: password
secret_key: a-32-character-secret-key-for-encryption
```
* The secret key, username and password are all given by the host of the server
* Start the client, scroll down, and click start proxy
* Point your applications at the SOCKS5 proxy on `local_listen_addr`
* The client and server versions must match. If they don't, the server rejects the connection with an "update your client" message

| Field | Description |
|---|---|
| `server_url` | WebSocket URL of the server (`ws://host:port`) |
| `local_listen_addr` | Local address the SOCKS5 proxy listens on |
| `username` / `password` | Credentials given by the server host |
| `secret_key` | 32-character encryption key, must match the server's |

## Server
* Download the server from releases or build from source
* Make a config-server.yml in the same directory as the server
* Example: 
```
listen_addr: "0.0.0.0:1978"
secret_key: "106282c922ff609da2547f00ea6bb79b"

share_ban:
  enabled: true
  duration_minutes: 20

users:
  - username: "username"
    password: "password"
    display_name: "User 1"
    max_kbps: 65536
  - username: "username2"
    password: "password2"
    display_name: "User 2"
  - username: "public"
    password: "public-password"
    display_name: "Public"
    max_kbps: 2000
    allow_sharing: true
```

### Server options
| Field | Required | Description |
|---|---|---|
| `listen_addr` | yes | Address the server listens on |
| `secret_key` | yes | 32-character encryption key shared with clients |
| `share_ban` | no | Connection-sharing protection (see below). Disabled by default |
| `users` | yes | List of accounts |

### User options
| Field | Required | Default | Description |
|---|---|---|---|
| `username` | yes | | Login name |
| `password` | yes | | Login password |
| `display_name` | yes | | Name shown in the server panel |
| `max_kbps` | no | unlimited | Bandwidth limit, see below |
| `allow_sharing` | no | `false` | Allow many clients to use this key at once, see below |

### Bandwidth limits (`max_kbps`)
* `max_kbps` limits the bandwidth of people connecting with certain credentials
  * note: if max_kbps is not present there is no limit
* The unit is real kilobits per second, so `max_kbps: 10000` is 10 Mbps
* The limit applies to each direction separately: `2000` means 2 Mbps down and 2 Mbps up
* Only tunneled payload data counts, so a speedtest through the proxy should land close to the configured value (a few percent under because of TCP ramp-up)
* The limit is per key, not per connection or per stream. All traffic on that key shares one budget

### Share ban (`share_ban`)
Stops a key from being used from more than one place at once.
```
share_ban:
  enabled: true
  duration_minutes: 20
```
* `enabled` (default `false`): turn the protection on
* `duration_minutes` (default `20`): how long a key is banned after being caught
* When a key connects while another connection with that key is live, the server waits a short grace period (to allow for a client reconnecting). If the old connection is still live after that, the key is banned for `duration_minutes` and all of its connections are dropped
* If the old connection has gone quiet, the new connection replaces it

### Shared keys (`allow_sharing`)
For handing out a heavily limited "public" key.
* `allow_sharing: true` exempts the key from the share ban and from the "replaced by a newer connection" kick, so any number of clients can be connected with it at once
* The key's `max_kbps` is shared between all of its clients. With `max_kbps: 2000`, everyone on that key together gets 2 Mbps down and 2 Mbps up
* The limit is not split fairly between clients. Whoever sends data first gets it, so one heavy user can slow down the others on the same key
* The option is optional and defaults to `false`. Leaving it out keeps the normal one-connection-per-key behavior
* To block a shared key, remove it from the config. Kicking from the panel only drops that connection, and the client can reconnect

### Server panel
The server shows a terminal UI listing every connection with its ID, display name, current speed (payload only, in kbps/Mbps) and total transferred data. A key with `allow_sharing` shows one row per connection.

| Key | Action |
|---|---|
| Up / Down | Move the selection |
| Enter | Kick the selected connection |
| `m` | Send a message to the selected connection |
| `b` | Broadcast a message to all connections |
| `q` | Quit |

Type the message, press Enter to send or Esc to cancel. Messages arrive on the client as notices.