// db.js
const Database = require('better-sqlite3');
const db = new Database('./ponto.db');

function init() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      discord_id TEXT NOT NULL,
      guild_id TEXT NOT NULL,
      UNIQUE(discord_id, guild_id)
    );

    CREATE TABLE IF NOT EXISTS sessions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      guild_id TEXT NOT NULL,
      start_ts INTEGER NOT NULL,
      end_ts INTEGER,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS pauses (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      guild_id TEXT NOT NULL,
      start_ts INTEGER NOT NULL,
      end_ts INTEGER,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS settings (
      guild_id TEXT PRIMARY KEY,
      panel_message_id TEXT
    );

    CREATE TABLE IF NOT EXISTS relatorios_semanais (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      username TEXT NOT NULL,
      semana_inicio INTEGER NOT NULL,
      total_ms INTEGER NOT NULL,
      atingiu_meta INTEGER NOT NULL
    );
  `);
}

function getUser(discordId, guildId) {
  let user = db
    .prepare(`SELECT * FROM users WHERE discord_id = ? AND guild_id = ?`)
    .get(discordId, guildId);

  if (!user) {
    db.prepare(`INSERT INTO users (discord_id, guild_id) VALUES (?, ?)`)
      .run(discordId, guildId);

    user = db
      .prepare(`SELECT * FROM users WHERE discord_id = ? AND guild_id = ?`)
      .get(discordId, guildId);
  }

  return user;
}

module.exports = { db, init, getUser };
