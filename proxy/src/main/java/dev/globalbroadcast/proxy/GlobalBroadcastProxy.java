package dev.globalbroadcast.proxy;

import com.google.inject.Inject;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.PluginMessageEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import com.velocitypowered.api.scheduler.ScheduledTask;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.globalbroadcast.common.BroadcastPayload;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;

@Plugin(id = "globalbroadcast", name = "GlobalBroadcast", version = "1.0.0")
public final class GlobalBroadcastProxy {
    private static final MinecraftChannelIdentifier MESSAGE_CHANNEL =
            MinecraftChannelIdentifier.from("globalbroadcast:message");
    private static final long CLEANUP_INTERVAL_MILLIS = TimeUnit.DAYS.toMillis(1);

    private final ProxyServer proxy;
    private final Logger logger;
    private final Path dataDirectory;
    private final AtomicBoolean polling = new AtomicBoolean();

    private HikariDataSource dataSource;
    private ExecutorService databaseExecutor;
    private ScheduledTask pollTask;
    private long lastSeenId;
    private long pollIntervalMillis;
    private long lastCleanupMillis;
    private long lastDatabaseErrorLogMillis;
    private boolean databaseErrorReported;

    @Inject
    public GlobalBroadcastProxy(
            ProxyServer proxy,
            Logger logger,
            @com.velocitypowered.api.plugin.annotation.DataDirectory Path dataDirectory) {
        this.proxy = proxy;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onProxyInitialization(ProxyInitializeEvent event) {
        Properties config = loadConfig();
        String host = requiredProperty(config, "database.host");
        int port = parseInteger("database.port", config.getProperty("database.port", "3306"), 1, 65535);
        String database = requiredProperty(config, "database.name");
        if (!database.matches("[A-Za-z0-9_]+")) {
            throw new IllegalStateException("database.name may contain only letters, digits, and underscores");
        }
        String username = requiredProperty(config, "database.username");
        String password = config.getProperty("database.password", "");
        pollIntervalMillis = parseInteger(
                "poll_interval_ms", config.getProperty("poll_interval_ms", "1000"), 250, 60000);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName("GlobalBroadcast");
        hikariConfig.setJdbcUrl("jdbc:mariadb://" + host + ":" + port + "/" + database);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(4);
        hikariConfig.setConnectionTimeout(5000);
        dataSource = new HikariDataSource(hikariConfig);

        try {
            initializeDatabase();
        } catch (SQLException exception) {
            closeDatabase();
            throw new IllegalStateException("Could not initialize the GlobalBroadcast database.", exception);
        }

        proxy.getChannelRegistrar().register(MESSAGE_CHANNEL);
        databaseExecutor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "GlobalBroadcast-Database");
            thread.setDaemon(true);
            return thread;
        });
        pollTask = proxy.getScheduler().buildTask(this, this::queuePoll)
                .repeat(pollIntervalMillis, TimeUnit.MILLISECONDS)
                .schedule();
    }

    @Subscribe
    public void onPluginMessage(PluginMessageEvent event) {
        if (!MESSAGE_CHANNEL.equals(event.getIdentifier())) {
            return;
        }
        event.setResult(PluginMessageEvent.ForwardResult.handled());
        if (!(event.getSource() instanceof ServerConnection)) {
            logger.warning("Ignoring global broadcast plugin message not sent by a backend server.");
            return;
        }

        try {
            BroadcastPayload payload = BroadcastPayload.decode(event.getData());
            executeDatabaseTask(() -> insertBroadcast(payload));
        } catch (IOException exception) {
            logger.log(Level.WARNING, "Ignoring malformed global broadcast plugin message.", exception);
        }
    }

    @Subscribe
    public void onProxyShutdown(ProxyShutdownEvent event) {
        closeDatabase();
    }

    private void initializeDatabase() throws SQLException {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS global_broadcasts (
                        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        player_name VARCHAR(16) NOT NULL,
                        message VARCHAR(256) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """);
            try (ResultSet result = statement.executeQuery(
                    "SELECT COALESCE(MAX(id), 0) FROM global_broadcasts")) {
                result.next();
                lastSeenId = result.getLong(1);
            }
            statement.executeUpdate(
                    "DELETE FROM global_broadcasts WHERE created_at < CURRENT_TIMESTAMP - INTERVAL 7 DAY");
        }
        lastCleanupMillis = System.currentTimeMillis();
    }

    private void insertBroadcast(BroadcastPayload payload) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO global_broadcasts (player_name, message) VALUES (?, ?)")) {
            statement.setString(1, payload.playerName());
            statement.setString(2, payload.message());
            statement.executeUpdate();
        } catch (SQLException exception) {
            logDatabaseFailure("Could not save global broadcast to the database.", exception);
        }
    }

    private void pollBroadcasts() {
        try {
            List<StoredBroadcast> broadcasts = new ArrayList<>();
            try (Connection connection = dataSource.getConnection();
                    PreparedStatement statement = connection.prepareStatement(
                            "SELECT id, player_name, message FROM global_broadcasts "
                                    + "WHERE id > ? ORDER BY id ASC LIMIT 100")) {
                statement.setLong(1, lastSeenId);
                try (ResultSet results = statement.executeQuery()) {
                    while (results.next()) {
                        broadcasts.add(new StoredBroadcast(
                                results.getLong("id"),
                                new BroadcastPayload(
                                        results.getString("player_name"),
                                        results.getString("message"))));
                    }
                }
            }

            for (StoredBroadcast broadcast : broadcasts) {
                lastSeenId = broadcast.id();
                sendBroadcast(broadcast.payload());
            }
            cleanupOldBroadcastsIfNeeded();
            if (databaseErrorReported) {
                logger.info("GlobalBroadcast database connection restored.");
                databaseErrorReported = false;
            }
        } catch (SQLException exception) {
            logDatabaseFailure("Could not read global broadcasts from the database.", exception);
        }
    }

    private void queuePoll() {
        if (!polling.compareAndSet(false, true)) {
            return;
        }
        if (!executeDatabaseTask(() -> {
            try {
                pollBroadcasts();
            } finally {
                polling.set(false);
            }
        })) {
            polling.set(false);
        }
    }

    private boolean executeDatabaseTask(Runnable task) {
        ExecutorService executor = databaseExecutor;
        if (executor == null) {
            logger.warning("GlobalBroadcast database worker is not running.");
            return false;
        }
        try {
            executor.execute(task);
            return true;
        } catch (RejectedExecutionException exception) {
            logger.log(Level.WARNING, "Could not queue GlobalBroadcast database work.", exception);
            return false;
        }
    }

    private void sendBroadcast(BroadcastPayload payload) {
        Component formatted = Component.text("[Global] ", NamedTextColor.GOLD)
                .append(Component.text(payload.playerName(), NamedTextColor.YELLOW))
                .append(Component.text(": " + payload.message(), NamedTextColor.WHITE));
        proxy.getScheduler().buildTask(this, () ->
                proxy.getAllPlayers().forEach(player -> player.sendMessage(formatted))).schedule();
    }

    private void cleanupOldBroadcastsIfNeeded() throws SQLException {
        long now = System.currentTimeMillis();
        if (now - lastCleanupMillis < CLEANUP_INTERVAL_MILLIS) {
            return;
        }
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "DELETE FROM global_broadcasts WHERE created_at < CURRENT_TIMESTAMP - INTERVAL 7 DAY");
        }
        lastCleanupMillis = now;
    }

    private void logDatabaseFailure(String message, SQLException exception) {
        long now = System.currentTimeMillis();
        if (!databaseErrorReported || now - lastDatabaseErrorLogMillis >= TimeUnit.MINUTES.toMillis(1)) {
            logger.log(Level.WARNING, message, exception);
            lastDatabaseErrorLogMillis = now;
            databaseErrorReported = true;
        }
    }

    private Properties loadConfig() {
        try {
            Files.createDirectories(dataDirectory);
            Path configPath = dataDirectory.resolve("config.properties");
            if (Files.notExists(configPath)) {
                Properties defaults = new Properties();
                defaults.setProperty("database.host", "127.0.0.1");
                defaults.setProperty("database.port", "3306");
                defaults.setProperty("database.name", "globalbroadcast");
                defaults.setProperty("database.username", "globalbroadcast");
                defaults.setProperty("database.password", "");
                defaults.setProperty("poll_interval_ms", "1000");
                try (var output = Files.newOutputStream(configPath)) {
                    defaults.store(output, "GlobalBroadcast MySQL/MariaDB settings");
                }
            }

            Properties config = new Properties();
            try (var input = Files.newInputStream(configPath)) {
                config.load(input);
            }
            return config;
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Could not load GlobalBroadcast config from " + dataDirectory, exception);
        }
    }

    private static String requiredProperty(Properties config, String key) {
        String value = config.getProperty(key, "").trim();
        if (value.isEmpty()) {
            throw new IllegalStateException(key + " must not be empty");
        }
        return value;
    }

    private static int parseInteger(String key, String value, int minimum, int maximum) {
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed >= minimum && parsed <= maximum) {
                return parsed;
            }
        } catch (NumberFormatException ignored) {
            // Report one clear configuration error for invalid and out-of-range values.
        }
        throw new IllegalStateException(
                key + " must be an integer between " + minimum + " and " + maximum);
    }

    private void closeDatabase() {
        if (pollTask != null) {
            pollTask.cancel();
            pollTask = null;
        }
        if (dataSource != null) {
            if (databaseExecutor != null) {
                databaseExecutor.shutdown();
                try {
                    if (!databaseExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        databaseExecutor.shutdownNow();
                    }
                } catch (InterruptedException exception) {
                    databaseExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                    logger.log(Level.WARNING, "Interrupted while stopping database worker.", exception);
                }
                databaseExecutor = null;
            }
            dataSource.close();
            dataSource = null;
        }
    }

    private record StoredBroadcast(long id, BroadcastPayload payload) {}
}
