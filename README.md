# Description
Voe is a toy proxying app I have made except the protocol is SOCKS-over-HTTP. This means servers are hostable through things like temporary HTTP tunnels

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

## Server
* Download the server from releases or build from source
* Make a config-server.yml in the same directory as the server
* Example: 
```
listen_addr: "0.0.0.0:1978"
secret_key: "106282c922ff609da2547f00ea6bb79b"

users:
  - username: "username"
    password: "password"
    display_name: "User 1"
    max_kbps: 65536
  - username: "username2"
    password: "password2"
    display_name: "User 2"
```
* Here, max_kbps limits the bandwidth of people connecting with certain credentials
  * note: if max_kbps is not present there is no limit

