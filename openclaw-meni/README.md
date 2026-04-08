# 🐾 Meni — OpenClaw WhatsApp AI Assistant

Personal AI assistant running on OpenClaw, restricted to the "אני ומני" WhatsApp group.

## Architecture
- **Gateway**: OpenClaw gateway on `localhost:18789` (auto-starts at login)
- **LLM**: GitHub Copilot GPT-4o (free via Copilot subscription)
- **Channel**: WhatsApp Web (Baileys)
- **Safety**: DMs disabled, group allowlist by sender phone + group ID

## Config (`~/.openclaw/openclaw.json`)
```json5
{
  channels: {
    whatsapp: {
      dmPolicy: "disabled",
      groupPolicy: "allowlist",
      groupAllowFrom: ["+972547894873"],   // sender allowlist (E.164)
      groups: {
        "120363407470486178@g.us": { requireMention: false }  // group allowlist
      }
    }
  }
}
```

## Key files
- `~/.openclaw/workspace/SOUL.md` — personality + hard rules
- `~/.openclaw/workspace/IDENTITY.md` — name, emoji, vibe
- `~/.openclaw/workspace/USER.md` — owner profile
- `~/.openclaw/openclaw.json` — gateway + channel config

## Dashboard (port 8096)
Shows gateway health, WhatsApp connection status, security config.
Can start gateway and trigger WhatsApp QR re-link if disconnected.

## Troubleshooting
- **No replies**: Check `groupAllowFrom` has your **phone number** (E.164), not group ID
- **Stale session**: `openclaw channels logout --channel whatsapp` then re-login
- **Corrupted creds**: Gateway auto-restores from `.bak`; if persistent, logout + login
