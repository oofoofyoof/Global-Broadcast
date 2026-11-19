package dev.globalbroadcast.backend;

import dev.globalbroadcast.common.BroadcastPayload;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

public final class GlobalBroadcastBackend extends JavaPlugin implements CommandExecutor {
    private static final String CHANNEL = "globalbroadcast:message";
    private static final int MAX_MESSAGE_LENGTH = 256;

    @Override
    public void onEnable() {
        getServer().getMessenger().registerOutgoingPluginChannel(this, CHANNEL);
        getCommand("globalbroadcast").setExecutor(this);
    }

    @Override
    public boolean onCommand(
            CommandSender sender, Command command, String label, String[] args) {
        if (!(sender instanceof Player player)) {
            sender.sendMessage("Only players can use this command.");
            return true;
        }

        if (args.length == 0) {
            player.sendMessage(ChatColor.RED + "Usage: /" + label + " <message>");
            return true;
        }

        String message = String.join(" ", args);
        if (message.length() > MAX_MESSAGE_LENGTH) {
            player.sendMessage(ChatColor.RED + "Your message must be 256 characters or fewer.");
            return true;
        }

        player.sendPluginMessage(this, CHANNEL, new BroadcastPayload(player.getName(), message).encode());
        player.sendMessage(ChatColor.GREEN + "Global broadcast sent.");
        return true;
    }
}
