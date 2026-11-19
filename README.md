# Global Broadcast

A Paper backend plugin and Velocity proxy plugin for network-wide player broadcasts.
Players can use `/globalbroadcast <message>` or `/gb <message>` on any backend.
The backend sends the message to its connected Velocity proxy, which stores it in
MySQL/MariaDB. Every proxy polls the shared database and delivers new broadcasts
to its online players.

## Requirements

- Java 21
- Paper 1.21.4 or newer
- Velocity 3.4 or newer
- MySQL or MariaDB reachable by every Velocity proxy

## Build with Gradle

Run `gradle build`. The installable plugin jars are:

- `backend/build/libs/GlobalBroadcast-Backend-1.0.0.jar`
- `proxy/build/libs/GlobalBroadcast-Proxy-1.0.0.jar`

## Build with Maven

Run `mvn package` from the repository root. Maven builds the same backend and proxy
plugins; their jars are written under `backend/target/` and `proxy/target/`.

## Install and configure

Install the backend jar on each Paper server and the proxy jar on each Velocity
proxy. Create a MySQL/MariaDB database and user, then start each proxy once to
generate `plugins/globalbroadcast/config.properties`. Configure the same database
host, port, name, username, and password on every proxy. The plugin creates its
`global_broadcasts` table automatically. `poll_interval_ms` controls how often a
proxy checks for new broadcasts (default: 1000).

For example, create the database and a dedicated user before starting the proxies:

```sql
CREATE DATABASE globalbroadcast;
CREATE USER 'globalbroadcast'@'%' IDENTIFIED BY 'change-this-password';
GRANT SELECT, INSERT, DELETE, CREATE ON globalbroadcast.* TO 'globalbroadcast'@'%';
```

The command is limited to players and requires `globalbroadcast.use`, granted to
operators by default. Broadcast messages are limited to 256 characters. Broadcast
rows older than seven days are pruned automatically.