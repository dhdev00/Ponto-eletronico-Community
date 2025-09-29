require('dotenv').config();

// -------------------------------------------------------------
// Discord.js - Imports
// -------------------------------------------------------------
const {
  Client,
  GatewayIntentBits,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder,
  ChannelType,
  PermissionFlagsBits,
  StringSelectMenuBuilder,
  MessageFlags,
} = require('discord.js');

// -------------------------------------------------------------
// DB - Inicialização e helpers do seu módulo local
// -------------------------------------------------------------
const { db, init, getUser } = require('./db');
init();

// -------------------------------------------------------------
// Índices (performance) - executa só uma vez (IF NOT EXISTS)
// -------------------------------------------------------------
db.prepare(`CREATE INDEX IF NOT EXISTS idx_sessions_guild_open ON sessions (guild_id, end_ts)`).run();
db.prepare(`CREATE INDEX IF NOT EXISTS idx_sessions_user_guild ON sessions (user_id, guild_id)`).run();
db.prepare(`CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions (guild_id, start_ts)`).run();
db.prepare(`CREATE INDEX IF NOT EXISTS idx_adjust_guild_ts ON adjustments (guild_id, created_ts)`).run();
db.prepare(`CREATE INDEX IF NOT EXISTS idx_adjust_user_guild ON adjustments (user_id, guild_id)`).run();

// -------------------------------------------------------------
// CONSTANTES & ENV
// -------------------------------------------------------------
const META_SEMANAL_HORAS = 7; // meta padrão de horas/semana
const BOT_OWNER_ID = process.env.BOT_OWNER_ID;              // obrigatório
const MANAGER_ROLE_ID = process.env.MANAGER_ROLE_ID || null;
const TEAM_ROLE_ID = process.env.TEAM_ROLE_ID || null;      // p/ mencionar no reset (opcional)
const GUILD_ID = process.env.GUILD_ID || null;              // p/ registrar slash commands
const RESET_REPORT_CHANNEL_ID = process.env.RESET_REPORT_CHANNEL_ID || null;

// Admin extra (IDs separados por vírgula no .env)
const EXTRA_ADMIN_IDS = (process.env.EXTRA_ADMIN_IDS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

// Metas dinâmicas por cargo (ex.: Auxiliar)
const META_MANAGER_HORAS = Number(process.env.META_MANAGER_HORAS ?? 3.5);
const AUXILIAR_ROLE_ID = process.env.AUXILIAR_ROLE_ID || null;
const META_AUXILIAR_HORAS = Number(process.env.META_AUXILIAR_HORAS ?? 4);

if (!BOT_OWNER_ID) {
  console.warn('⚠️ BOT_OWNER_ID não definido no .env. Ações restritas não funcionarão.');
}

// -------------------------------------------------------------
// EMOJIS / ÍCONES
// -------------------------------------------------------------
const emgregistro = '<:32535applicationapprivedids:1410453305485824000>';
const emgregistroand = '<:5775applicationpendingids:1410453201672736768>';
const emgwarning = '<:8649warning:1410453253791027381>';
const emgalerta = '<:4260info:1411032846981665090>';
const semacesso = ':8056engagedinsuspectedspamactiv:';
const controleemg = '<:8907top:1410453258001973258>';
const cumprido = '<:1372checkmark:1411032833664880741>';
const andamento = '<:8649cooldown:1410453246413115402>';
const emgjustificado = '<:4260info:1411032846981665090> ';
const erro = '<:2360cross:1411032838903431348>';
const verificacao = '<a:verification_icon:1376945592223142070>';
const membrosemtrabalhoemg = '<:5801stableping:1411032863930843137>';

// -------------------------------------------------------------
// CLIENT - Intents mínimas + robustez
// -------------------------------------------------------------
const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers],
});

process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e));
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e));

// -------------------------------------------------------------
// FORMATADORES / HELPERS GERAIS
// -------------------------------------------------------------
const fmtDateTime = new Intl.DateTimeFormat('pt-BR', { hour12: false, dateStyle: 'short', timeStyle: 'short' });
const fmtTime = new Intl.DateTimeFormat('pt-BR', { hour12: false, timeStyle: 'medium' });

function msToHoursFloat(ms) {
  return Math.max(0, ms / 3_600_000);
}
function truncate(s, max = 4000) {
  if (!s) return s;
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

// -------------------------------------------------------------
// PERMISSÕES (owner, extra admins, perms, cargo manager)
// -------------------------------------------------------------
function canModerate(interaction) {
  if (!interaction?.member) return false;
  const uid = interaction.user.id;

  if (uid === BOT_OWNER_ID) return true;
  if (EXTRA_ADMIN_IDS.includes(uid)) return true;

  const perms = interaction.member.permissions;
  if (perms?.has(PermissionFlagsBits.Administrator) || perms?.has(PermissionFlagsBits.ManageGuild)) return true;

  if (MANAGER_ROLE_ID) return interaction.member.roles.cache.has(MANAGER_ROLE_ID);
  return false;
}

// -------------------------------------------------------------
// META / RESET SEMANAL (meta table + backup/desfazer)
// -------------------------------------------------------------
function ensureMetaTable() {
  db.prepare(`
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `).run();
}
function metaGet(key, def = null) {
  const row = db.prepare(`SELECT value FROM meta WHERE key = ?`).get(key);
  return row ? row.value : def;
}
function metaSet(key, value) {
  db.prepare(`
    INSERT INTO meta (key, value) VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `).run(key, value);
}
function metaDel(key) {
  db.prepare(`DELETE FROM meta WHERE key = ?`).run(key);
}
function getLastResetTsForGuild(guildId) {
  ensureMetaTable();
  const v = metaGet(`last_reset_ts:${guildId}`, '0');
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}
function setLastResetTsForGuild(guildId, ts) {
  ensureMetaTable();
  metaSet(`last_reset_ts:${guildId}`, String(ts));
}

// Backup p/ desfazer reset
function setResetBackup(guildId, prevTs, byId, atTs) {
  ensureMetaTable();
  metaSet(`last_reset_prev_ts:${guildId}`, String(prevTs));
  metaSet(`last_reset_prev_by:${guildId}`, String(byId));
  metaSet(`last_reset_prev_saved_at:${guildId}`, String(atTs));
}
function getResetBackup(guildId) {
  ensureMetaTable();
  const prev = Number(metaGet(`last_reset_prev_ts:${guildId}`, '0')) || 0;
  const by = metaGet(`last_reset_prev_by:${guildId}`, null);
  const when = Number(metaGet(`last_reset_prev_saved_at:${guildId}`, '0')) || 0;
  return { prev, by, when };
}
function clearResetBackup(guildId) {
  metaDel(`last_reset_prev_ts:${guildId}`);
  metaDel(`last_reset_prev_by:${guildId}`);
  metaDel(`last_reset_prev_saved_at:${guildId}`);
}

// -------------------------------------------------------------
// AJUSTES (somar/subtrair horas)
// -------------------------------------------------------------
function ensureAdjustmentsTable() {
  db.prepare(`
    CREATE TABLE IF NOT EXISTS adjustments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      guild_id TEXT NOT NULL,
      delta_ms INTEGER NOT NULL,  -- negativo remove horas; positivo adiciona
      reason TEXT,
      created_by TEXT NOT NULL,   -- discord id de quem criou
      created_ts INTEGER NOT NULL
    )
  `).run();
}

// -------------------------------------------------------------
// JUSTIFICATIVAS - Semana toda (flag) + Dias justificados
// -------------------------------------------------------------
// Justificativa total da semana
function ensureJustifiedTable() {
  db.prepare(`
    CREATE TABLE IF NOT EXISTS justified (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      guild_id TEXT NOT NULL,
      period_start_ts INTEGER NOT NULL,
      reason TEXT,
      created_by TEXT NOT NULL,
      created_ts INTEGER NOT NULL,
      UNIQUE(user_id, guild_id, period_start_ts)
    )
  `).run();
}
function markJustified(userIdInt, guildId, periodStartTs, reason, createdBy) {
  const agora = Date.now();
  db.prepare(`
    INSERT INTO justified (user_id, guild_id, period_start_ts, reason, created_by, created_ts)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_id, guild_id, period_start_ts)
    DO UPDATE SET reason = excluded.reason, created_by = excluded.created_by, created_ts = excluded.created_ts
  `).run(userIdInt, guildId, periodStartTs, reason, createdBy, agora);
}
function hasJustified(guildId, userIdInt, periodStartTs) {
  const row = db.prepare(`
    SELECT 1 FROM justified
    WHERE guild_id = ? AND user_id = ? AND period_start_ts = ?
  `).get(guildId, userIdInt, periodStartTs);
  return !!row;
}

// Dias justificados (1 dia = -1h na meta efetiva)
function ensureJustifiedDaysCountTable() {
  db.prepare(`
    CREATE TABLE IF NOT EXISTS justified_days_count (
      user_id INTEGER NOT NULL,
      guild_id TEXT NOT NULL,
      period_start_ts INTEGER NOT NULL,
      dias INTEGER NOT NULL,
      reason TEXT,
      created_by TEXT NOT NULL,
      created_ts INTEGER NOT NULL,
      PRIMARY KEY (user_id, guild_id, period_start_ts)
    )
  `).run();
}
function setJustifiedDaysCount(userIdInt, guildId, periodStartTs, dias, reason, createdBy) {
  const now = Date.now();
  db.prepare(`
    INSERT INTO justified_days_count (user_id, guild_id, period_start_ts, dias, reason, created_by, created_ts)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_id, guild_id, period_start_ts)
    DO UPDATE SET dias=excluded.dias, reason=excluded.reason, created_by=excluded.created_by, created_ts=excluded.created_ts
  `).run(userIdInt, guildId, periodStartTs, dias, reason || null, createdBy, now);
}
function getJustifiedDaysCount(userIdInt, guildId, periodStartTs) {
  const row = db.prepare(`
    SELECT dias FROM justified_days_count
    WHERE user_id=? AND guild_id=? AND period_start_ts=?
  `).get(userIdInt, guildId, periodStartTs);
  return Math.max(0, Number(row?.dias || 0));
}

// Remoções (parcial/total) de justificativas
function unmarkJustified(userIdInt, guildId, periodStartTs) {
  db.prepare(`
    DELETE FROM justified
    WHERE user_id = ? AND guild_id = ? AND period_start_ts = ?
  `).run(userIdInt, guildId, periodStartTs);
}
function removeJustifiedDays(userIdInt, guildId, periodStartTs, diasARemover) {
  const atuais = getJustifiedDaysCount(userIdInt, guildId, periodStartTs);
  const remover = Math.max(0, Number(diasARemover || 0));
  const novo = Math.max(0, atuais - remover);
  setJustifiedDaysCount(userIdInt, guildId, periodStartTs, novo, null, 'system');
  return { anteriores: atuais, removidos: Math.min(atuais, remover), novo };
}

// -------------------------------------------------------------
// EXCLUSÕES DE RANKING (do seu original)
// -------------------------------------------------------------
function ensureRankingExcludedTable() {
  db.prepare(`
    CREATE TABLE IF NOT EXISTS ranking_excluded (
      guild_id TEXT NOT NULL,
      user_id  INTEGER NOT NULL,  -- id interno da tabela users
      added_by TEXT NOT NULL,
      created_ts INTEGER NOT NULL,
      PRIMARY KEY (guild_id, user_id)
    )
  `).run();
}
function getExcludedSet(guildId) {
  const rows = db.prepare(`SELECT user_id FROM ranking_excluded WHERE guild_id = ?`).all(guildId);
  return new Set(rows.map(r => r.user_id));
}
function excludeUserFromRanking(guildId, userIdInt, byId) {
  db.prepare(`
    INSERT INTO ranking_excluded (guild_id, user_id, added_by, created_ts)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(guild_id, user_id) DO UPDATE SET added_by = excluded.added_by, created_ts = excluded.created_ts
  `).run(guildId, userIdInt, byId, Date.now());
}
function includeUserInRanking(guildId, userIdInt) {
  db.prepare(`DELETE FROM ranking_excluded WHERE guild_id = ? AND user_id = ?`).run(guildId, userIdInt);
}

// -------------------------------------------------------------
// CACHE DE NOMES (para reduzir fetchs)
// -------------------------------------------------------------
const nameCache = new Map(); // { id: { name, exp } }
function cacheGet(id) {
  const hit = nameCache.get(id);
  if (hit && hit.exp > Date.now()) return hit.name;
  return null;
}
function cacheSet(id, name, ttlMs = 5 * 60 * 1000) {
  nameCache.set(id, { name, exp: Date.now() + ttlMs });
}
async function resolveNamesBatch(guild, ids) {
  const toFetch = [];
  const names = new Map();
  for (const raw of ids) {
    const id = String(raw);
    const c = cacheGet(id);
    if (c) names.set(id, c);
    else toFetch.push(id);
  }
  if (toFetch.length) {
    const col = await guild.members.fetch({ user: toFetch }).catch(() => null);
    if (col) {
      for (const id of toFetch) {
        const m = col.get(id);
        const nm = m?.nickname ?? m?.user?.globalName ?? m?.user?.username ?? id;
        cacheSet(id, nm);
        names.set(id, nm);
      }
    } else {
      for (const id of toFetch) names.set(id, id);
    }
  }
  return names;
}
async function resolveDisplayName(guild, discordId) {
  const id = String(discordId);
  const c = cacheGet(id);
  if (c) return c;

  try {
    const member = await guild.members.fetch(id);
    const nm = member?.nickname ?? member?.user?.globalName ?? member?.user?.username ?? id;
    cacheSet(id, nm);
    return nm;
  } catch {
    try {
      const user = await guild.client.users.fetch(id);
      const nm = user?.globalName ?? user?.username ?? id;
      cacheSet(id, nm);
      return nm;
    } catch {
      return id;
    }
  }
}

// -------------------------------------------------------------
// METAS - base/efetiva (considera cargo + dias justificados)
// -------------------------------------------------------------
function getTargetHoursForUser(guild, discordId) {
  try {
    if (guild && MANAGER_ROLE_ID) {
      const role = guild.roles.cache.get(MANAGER_ROLE_ID);
      if (role?.members?.has(String(discordId))) return META_MANAGER_HORAS; // Manager = 3h30
    }
    if (guild && AUXILIAR_ROLE_ID) {
      const role = guild.roles.cache.get(AUXILIAR_ROLE_ID);
      if (role?.members?.has(String(discordId))) return META_AUXILIAR_HORAS; // Auxiliar
    }
  } catch {}
  return META_SEMANAL_HORAS; // Padrão
}

function getEffectiveTargetHoursForUser(guild, discordId, userIdInt, guildId, periodStartTs) {
  const base = getTargetHoursForUser(guild, discordId);
  const dias = getJustifiedDaysCount(userIdInt, guildId, periodStartTs);
  // 1 dia justificado = -1h (limitado à meta base)
  return Math.max(0, base - Math.min(base, dias));
}

// -------------------------------------------------------------
// RESUMO SEMANAL (para ranking/reset) - une sessões + ajustes
// -------------------------------------------------------------
function getWeeklyRowsForGuild(guildId) {
  const lastResetTs = getLastResetTsForGuild(guildId) || 0;

  // Totais de SESSÕES
  const sessRows = db
    .prepare(
      `SELECT u.discord_id as discordId, u.id as userId, SUM(s.end_ts - s.start_ts) as totalSess
       FROM sessions s
       JOIN users u ON s.user_id = u.id
       WHERE s.start_ts >= ? AND s.end_ts IS NOT NULL AND s.guild_id = ?
       GROUP BY u.id`
    )
    .all(lastResetTs, guildId);

  // Totais de AJUSTES
  const adjRows = db
    .prepare(
      `SELECT u.discord_id as discordId, a.user_id as userId, SUM(a.delta_ms) as totalAdj
       FROM adjustments a
       JOIN users u ON a.user_id = u.id
       WHERE a.guild_id = ? AND a.created_ts >= ?
       GROUP BY a.user_id`
    )
    .all(guildId, lastResetTs);

  // Indexa por userId e soma total
  const byUser = new Map();
  for (const r of sessRows) {
    byUser.set(r.userId, { userId: r.userId, discordId: r.discordId, totalSess: r.totalSess || 0, totalAdj: 0 });
  }
  for (const r of adjRows) {
    if (!byUser.has(r.userId)) {
      byUser.set(r.userId, { userId: r.userId, discordId: r.discordId, totalSess: 0, totalAdj: r.totalAdj || 0 });
    } else {
      const item = byUser.get(r.userId);
      item.totalAdj = (item.totalAdj || 0) + (r.totalAdj || 0);
    }
  }

  // Array final
  let rows = Array.from(byUser.values()).map((it) => ({
    userId: it.userId,
    discordId: it.discordId,
    total: (it.totalSess || 0) + (it.totalAdj || 0),
  }));

  // Excluir quem estiver marcado para não aparecer no ranking
  const excluded = getExcludedSet(guildId);
  if (excluded.size) rows = rows.filter(r => !excluded.has(r.userId));

  // Ordenar por maior total (tie-break por Discord ID)
  rows.sort((a, b) => (b.total || 0) - (a.total || 0) || String(a.discordId).localeCompare(String(b.discordId)));

  return { rows, sinceTs: lastResetTs };
}

// -------------------------------------------------------------
// Formatação de resumo (para log de reset)
// Regra de exibição (prioridade):
// 1) Se bateu a META BASE -> mostra "cumprido"
// 2) Senão, se "full justificado" OU (dias>0 e atingiu META EFETIVA) -> "justificado"
// 3) Caso contrário -> "erro" (não cumpriu)
// -------------------------------------------------------------
async function formatWeeklyRowsForLog(guild, rows, limit = 25) {
  if (!rows.length) return 'Ninguém registrou horas nesta semana.';

  const ids = rows.slice(0, limit).map(r => String(r.discordId));
  const names = await resolveNamesBatch(guild, ids);

  const lines = [];
  const top = rows.slice(0, limit);
  const guildId = guild?.id || 'default';
  const sinceTs = getLastResetTsForGuild(guildId) || 0;

  let pos = 1;
  for (const r of top) {
    const totalMs = r.total || 0;
    const h = Math.max(0, Math.floor(totalMs / 1000 / 60 / 60));
    const m = Math.max(0, Math.floor((totalMs / 1000 / 60) % 60));
    const hFloat = msToHoursFloat(totalMs); // calcular fração

    const diasJustificados = getJustifiedDaysCount(r.userId, guildId, sinceTs);
    const fullJustificado = hasJustified(guildId, r.userId, sinceTs);

    const baseTarget = getTargetHoursForUser(guild, r.discordId);
    const effectiveTarget = getEffectiveTargetHoursForUser(guild, r.discordId, r.userId, guildId, sinceTs);

    let statusLabel;
    if (hFloat >= baseTarget) {
      statusLabel = `**${cumprido}**`;
    } else if (fullJustificado || (diasJustificados > 0 && hFloat >= effectiveTarget)) {
      statusLabel = `**${emgjustificado}**`;
    } else {
      statusLabel = `**${erro}**`;
    }

    const nome = names.get(String(r.discordId)) ?? String(r.discordId);
    lines.push(`**${pos}. ${nome}** — \`${h}h ${m}min\` • ${statusLabel}`);
    pos++;
  }

  if (rows.length > limit) lines.push(`… e mais **${rows.length - limit}** membro(s).`);
  return lines.join('\n');
}


// -------------------------------------------------------------
// LOGS - helpers gerais (entrada/saída, ajustes, justificativas)
// -------------------------------------------------------------
async function getOrCreateLogChannel(guild) {
  if (!guild) return null;
  try {
    // Por ID (se definido)
    if (process.env.LOG_CHANNEL_ID) {
      try {
        const ch = await guild.channels.fetch(process.env.LOG_CHANNEL_ID);
        if (ch && ch.type === ChannelType.GuildText) return ch;
        console.warn(`[LOG] LOG_CHANNEL_ID inválido. Tentando por nome...`);
      } catch (e) {
        console.warn(`[LOG] LOG_CHANNEL_ID não encontrado: ${e.message}`);
      }
    }
    // Por nome
    const found = guild.channels.cache.find((c) => c.type === ChannelType.GuildText && c.name.toLowerCase() === 'ponto-logs');
    if (found) return found;

    // Criar se possível
    const me = guild.members.me;
    const canCreate = me?.permissions?.has(PermissionFlagsBits.ManageChannels);
    if (!canCreate) {
      console.warn(`[LOG] Sem permissão para criar canal. Defina LOG_CHANNEL_ID.`);
      return null;
    }
    return await guild.channels.create({
      name: 'ponto-logs',
      type: ChannelType.GuildText,
      reason: 'Canal de logs do Ponto Eletrônico',
    });
  } catch (err) {
    console.error(`[LOG] Falha ao obter/criar canal:`, err);
    return null;
  }
}

// Canal exclusivo p/ relatórios de reset
async function getOrCreateResetReportChannel(guild) {
  if (!guild) return null;
  try {
    if (RESET_REPORT_CHANNEL_ID) {
      try {
        const ch = await guild.channels.fetch(RESET_REPORT_CHANNEL_ID);
        if (ch && ch.type === ChannelType.GuildText) return ch;
        console.warn(`[RESET-REPORT] RESET_REPORT_CHANNEL_ID inválido. Tentando por nome...`);
      } catch (e) {
        console.warn(`[RESET-REPORT] RESET_REPORT_CHANNEL_ID não encontrado: ${e.message}`);
      }
    }
    const found = guild.channels.cache.find((c) => c.type === ChannelType.GuildText && c.name.toLowerCase() === 'ponto-relatorios');
    if (found) return found;

    const me = guild.members.me;
    const canCreate = me?.permissions?.has(PermissionFlagsBits.ManageChannels);
    if (!canCreate) {
      console.warn(`[RESET-REPORT] Sem permissão para criar canal. Defina RESET_REPORT_CHANNEL_ID.`);
      return null;
    }
    return await guild.channels.create({
      name: 'ponto-relatorios',
      type: ChannelType.GuildText,
      reason: 'Canal dedicado a relatórios de reset do Ponto Eletrônico',
    });
  } catch (err) {
    console.error(`[RESET-REPORT] Falha ao obter/criar canal:`, err);
    return null;
  }
}

// Embeds para entrada/saída
function buildLogEmbed(tipo, member, startTs, endTs = null, total = {}, finalizadoPor = null) {
  const cor = tipo === 'entrada' ? 0x57f287 : 0xed4245;
  const titulo = tipo === 'entrada' ? '🟢 Entrada registrada' : '🔴 Saída registrada';
  const inicioFmt = fmtDateTime.format(new Date(startTs));
  const fimFmt = endTs ? fmtDateTime.format(new Date(endTs)) : '—';
  const avatar = member?.user?.displayAvatarURL?.({ size: 256 }) ?? null;

  const embed = new EmbedBuilder()
    .setColor(cor)
    .setTitle(titulo)
    .setThumbnail(avatar)
    .addFields(
      { name: 'Membro', value: `${member} \`(${member?.user?.id ?? 'N/A'})\``, inline: false },
      { name: 'Início', value: `\`${inicioFmt}\``, inline: true },
      { name: 'Fim', value: `\`${fimFmt}\``, inline: true },
    )
    .setFooter({ text: `Ponto eletrônico - DH Bot's` })
    .setTimestamp();

  if (tipo === 'saida' && typeof total.horas === 'number') {
    embed.addFields({ name: 'Total trabalhado', value: `\`${total.horas}h ${total.minutos}min\``, inline: false });
  }
  if (finalizadoPor) {
    embed.addFields({ name: 'Finalizado por', value: `${finalizadoPor}`, inline: false });
  }
  return embed;
}
async function sendLogEmbed(guild, embed) {
  const ch = await getOrCreateLogChannel(guild);
  if (!ch) return false;
  try {
    await ch.send({ embeds: [embed] });
    return true;
  } catch (e) {
    console.error(`[LOG] Erro ao enviar embed:`, e);
    return false;
  }
}

// Log: ajustes (add/remover horas)
async function sendAdjustmentLog(guild, { tipo, executorId, alvoId, horas, minutos, motivo }) {
  try {
    const ch = await getOrCreateLogChannel(guild);
    if (!ch) return;

    const names = await resolveNamesBatch(guild, [executorId, alvoId]);
    const executorNome = names.get(String(executorId)) ?? String(executorId);
    const alvoNome = names.get(String(alvoId)) ?? String(alvoId);

    const cor = tipo === 'adicao' ? 0x2ecc71 : 0xe67e22;
    const titulo = tipo === 'adicao' ? '**🔹 Horas adicionadas**' : '**🔸 Horas removidas**';

    const embed = new EmbedBuilder()
      .setColor(cor)
      .setTitle(titulo)
      .addFields(
        { name: '**Membro**', value: `**${alvoNome}** \`(${alvoId})\``, inline: true },
        { name: '**Quantidade**', value: `\`${horas}h ${minutos}min\``, inline: true },
        { name: '**Motivo**', value: motivo || '—', inline: false },
        { name: '**Alteração feita por**', value: `**${executorNome}** \`(${executorId})\``, inline: false },
      )
      .setTimestamp()
      .setFooter({ text: `Ponto eletrônico - DH Bot's` });

    await ch.send({ embeds: [embed] });
  } catch (e) {
    console.error('[AJUSTE][LOG] Falha ao enviar log de ajuste:', e);
  }
}

// Log: justificativas
async function sendJustificationLog(guild, { executorId, alvoId, motivo }) {
  try {
    const ch = await getOrCreateLogChannel(guild);
    if (!ch) return;

    const names = await resolveNamesBatch(guild, [executorId, alvoId]);
    const executorNome = names.get(String(executorId)) ?? String(executorId);
    const alvoNome = names.get(String(alvoId)) ?? String(alvoId);

    const embed = new EmbedBuilder()
      .setColor(0xf1c40f)
      .setTitle('⏰ Membro Justificado')
      .addFields(
        { name: '**Membro**', value: `**${alvoNome}** \`(${alvoId})\``, inline: false },
        { name: '**Motivo**', value: motivo || '—', inline: false },
        { name: '**Justificado por**', value: `**${executorNome}** \`(${executorId})\``, inline: false },
      )
      .setTimestamp()
      .setFooter({ text: `Ponto eletrônico - DH Bot's` });

    await ch.send({ embeds: [embed] });
  } catch (e) {
    console.error('[JUSTIFICAR][LOG] Falha ao enviar log de justificativa:', e);
  }
}

// Log: dias justificados (add)
async function sendJustifiedDaysLog(guild, { executorId, alvoId, dias, motivo }) {
  try {
    const ch = await getOrCreateLogChannel(guild);
    if (!ch) return;

    const names = await resolveNamesBatch(guild, [executorId, alvoId]);
    const executorNome = names.get(String(executorId)) ?? String(executorId);
    const alvoNome = names.get(String(alvoId)) ?? String(alvoId);

    const embed = new EmbedBuilder()
      .setColor(0xf1c40f)
      .setTitle('⏰ Membro Justificado')
      .addFields(
        { name: '**Membro**', value: `**${alvoNome}** \`(${alvoId})\``, inline: false },
        { name: '**Dias justificados**', value: `\`${dias}\``, inline: true },
        { name: '**Motivo**', value: motivo || '—', inline: false },
        { name: '**Justificado por**', value: `**${executorNome}** \`(${executorId})\``, inline: false },
      )
      .setTimestamp()
      .setFooter({ text: `Ponto eletrônico - DH Bot's` });

    await ch.send({ embeds: [embed] });
  } catch (e) {
    console.error('[JUSTIFICAR DIAS][LOG] Falha ao enviar log de dias justificados:', e);
  }
}

// Logs: remoção de justificativas (dias/tudo)
async function sendRemovedJustifiedDaysLog(guild, { executorId, alvoId, removidos, totalFinal, motivo }) {
  try {
    const ch = await getOrCreateLogChannel(guild);
    if (!ch) return;

    const names = await resolveNamesBatch(guild, [executorId, alvoId]);
    const executorNome = names.get(String(executorId)) ?? String(executorId);
    const alvoNome = names.get(String(alvoId)) ?? String(alvoId);

    const embed = new EmbedBuilder()
      .setColor(0xe67e22)
      .setTitle('🗑️ Dias Justificados Removidos')
      .addFields(
        { name: '**Membro**', value: `**${alvoNome}** \`(${alvoId})\``, inline: false },
        { name: '**Removidos**', value: `\`${removidos}\``, inline: true },
        { name: '**Total após remoção**', value: `\`${totalFinal}\``, inline: true },
        { name: '**Motivo**', value: motivo || '—', inline: false },
        { name: '**Ação por**', value: `**${executorNome}** \`(${executorId})\``, inline: false },
      )
      .setTimestamp()
      .setFooter({ text: `Ponto eletrônico - DH Bot's` });

    await ch.send({ embeds: [embed] });
  } catch (e) {
    console.error('[JUSTIFICAR DIAS][LOG] Falha ao enviar log de remoção de dias:', e);
  }
}
async function sendRemovedAllJustificationsLog(guild, { executorId, alvoId, motivo }) {
  try {
    const ch = await getOrCreateLogChannel(guild);
    if (!ch) return;

    const names = await resolveNamesBatch(guild, [executorId, alvoId]);
    const executorNome = names.get(String(executorId)) ?? String(executorId);
    const alvoNome = names.get(String(alvoId)) ?? String(alvoId);

    const embed = new EmbedBuilder()
      .setColor(0xe74c3c)
      .setTitle('🗑️ Justificativas Removidas (Semana)')
      .addFields(
        { name: '**Membro**', value: `**${alvoNome}** \`(${alvoId})\``, inline: false },
        { name: '**Ação**', value: 'Removido **justificado full** e **zerados os dias justificados** desta semana.', inline: false },
        { name: '**Motivo**', value: motivo || '—', inline: false },
        { name: '**Ação por**', value: `**${executorNome}** \`(${executorId})\``, inline: false },
      )
      .setTimestamp()
      .setFooter({ text: `Ponto eletrônico - DH Bot's` });

    await ch.send({ embeds: [embed] });
  } catch (e) {
    console.error('[JUSTIFICAR][LOG] Falha ao enviar log de remoção total:', e);
  }
}

// -------------------------------------------------------------
// LIMPAR CANAL DO PAINEL (apaga tudo e recria a mensagem do painel)
// -------------------------------------------------------------
async function clearPanelChannel(channel) {
  if (!channel) return;

  // 1) Bulk (só < 14d)
  let fetched;
  do {
    fetched = await channel.messages.fetch({ limit: 100 }).catch(() => null);
    if (!fetched || fetched.size === 0) break;

    const bulkable = fetched.filter(m => (Date.now() - m.createdTimestamp) < 14 * 24 * 60 * 60 * 1000);
    if (bulkable.size) await channel.bulkDelete(bulkable, true).catch(() => {});
  } while (fetched && fetched.size >= 2);

  // 2) Fallback (apaga 1 a 1, inclusive >14d)
  let olderLeft = true;
  while (olderLeft) {
    const pack = await channel.messages.fetch({ limit: 50 }).catch(() => null);
    if (!pack || pack.size === 0) break;
    for (const [, msg] of pack) {
      try { await msg.delete().catch(() => {}); } catch {}
    }
    olderLeft = pack.size > 0;
    if (pack.size < 5) break;
  }
}

// -------------------------------------------------------------
// PAINEL - Mensagem com botões
// -------------------------------------------------------------
async function criarPainel() {
  try {
    const channel = await client.channels.fetch(process.env.CHANNEL_ID);
    if (!channel) return console.error('❌ Canal do painel não encontrado!');

    // Apaga tudo antes de criar
    await clearPanelChannel(channel);

    // Embed principal do painel
    const embed = new EmbedBuilder()
      .setTitle('**Ponto eletrônico - Community**')
      .setDescription('Clique nos botões abaixo para registrar ou consultar horas.')
      .setColor(0x2b2d31)
      .setImage('https://cdn.discordapp.com/attachments/1337779192057692254/1415154677661831218/PONTOCOMMUNITY.png?ex=68c22cff&is=68c0db7f&hm=1cb133024d15e2019297e7b1852e9321cf441b6b0a6dfa51ecdb21bf0d3ff199&');

    // Linha 1 (ações principais)
    const row1 = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('entrada').setLabel('Iniciar Ponto').setStyle(ButtonStyle.Success),
      new ButtonBuilder().setCustomId('saida').setLabel('Finalizar Ponto').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('ativos').setLabel('Ativos Agora').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('forcar_saida').setLabel('Função Managers').setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId('ranking').setLabel('Horas semanais').setStyle(ButtonStyle.Secondary),
    );

    // Linha 2 (admin)
    const row2 = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('zerar').setLabel('Zerar contagem (Admin)').setStyle(ButtonStyle.Danger),
    );

    await channel.send({ embeds: [embed], components: [row1, row2] });
    console.log(`🧭 Painel publicado em #${channel.name} (${channel.id}).`);
  } catch (e) {
    console.error('❌ Erro ao criar painel:', e);
  }
}

// -------------------------------------------------------------
// READY - Inicialização e registro de slash commands
// -------------------------------------------------------------
client.once('clientReady', async (client) => {
  console.log(`✅ Bot logado como ${client.user.tag}`);

  // Garantir tabelas
  ensureMetaTable();
  ensureAdjustmentsTable();
  ensureJustifiedTable();
  ensureJustifiedDaysCountTable();
  ensureRankingExcludedTable();

  // Registrar slash commands no GUILD_ID (se definido)
  try {
    if (GUILD_ID) {
      const guild = await client.guilds.fetch(GUILD_ID).catch(() => null);
      if (guild) {
        // Ajustes de horas
        await guild.commands.create({
          name: 'remover_horas',
          description: 'Remove horas de um usuário (apenas DHorizonX).',
          options: [
            { name: 'usuario', description: 'Usuário alvo', type: 6, required: true },
            { name: 'horas', description: 'Quantidade de horas a remover', type: 4, required: true },
            { name: 'minutos', description: 'Minutos a remover (0-59)', type: 4, required: false },
            { name: 'motivo', description: 'Motivo do ajuste', type: 3, required: false },
          ],
        });
        await guild.commands.create({
          name: 'adicionar_horas',
          description: 'Adiciona horas a um usuário (apenas DHorizonX).',
          options: [
            { name: 'usuario', description: 'Usuário alvo', type: 6, required: true },
            { name: 'horas', description: 'Quantidade de horas a adicionar', type: 4, required: true },
            { name: 'minutos', description: 'Minutos a adicionar (0-59)', type: 4, required: false },
            { name: 'motivo', description: 'Motivo do ajuste', type: 3, required: false },
          ],
        });

        // Reset / desfazer
        await guild.commands.create({ name: 'desfazer_reset', description: 'Desfaz o último reset semanal (apenas DHorizonX).' });

        // Justificativa total
        await guild.commands.create({
          name: 'justificar',
          description: 'Marca um membro como justificado por não cumprir a meta (somente dono).',
          options: [
            { name: 'usuario', description: 'Usuário alvo', type: 6, required: true },
            { name: 'motivo', description: 'Motivo da justificativa', type: 3, required: true },
          ],
        });

        // Dias justificados (acumula até a meta base)
        await guild.commands.create({
          name: 'justificar_dias',
          description: 'Desconta da meta a quantidade de dias justificados (1 dia = -1h).',
          options: [
            { name: 'usuario', description: 'Usuário alvo', type: 6, required: true },
            { name: 'dias', description: 'Quantidade de dias (0-7)', type: 4, required: true },
            { name: 'motivo', description: 'Motivo (opcional)', type: 3, required: false },
          ],
        });

        // Remover justificativas (dias ou tudo)
        await guild.commands.create({
          name: 'remover_justificativas',
          description: 'Remove dias justificados ou zera toda a justificativa da semana atual.',
          options: [
            { name: 'usuario', description: 'Usuário alvo', type: 6, required: true },
            {
              name: 'tipo',
              description: 'O que remover',
              type: 3,
              required: true,
              choices: [
                { name: 'dias (remover quantidade)', value: 'dias' },
                { name: 'tudo (limpar full + dias)', value: 'tudo' },
              ],
            },
            { name: 'dias', description: 'Qtd de dias a remover (se tipo = dias)', type: 4, required: false },
            { name: 'motivo', description: 'Motivo (opcional)', type: 3, required: false },
          ],
        });

        // Ranking (exclusões)
        await guild.commands.create({
          name: 'excluir_ranking',
          description: 'Exclui um usuário do ranking semanal (Managers ou acima).',
          options: [{ name: 'usuario', description: 'Usuário alvo', type: 6, required: true }],
        });
        await guild.commands.create({
          name: 'incluir_ranking',
          description: 'Inclui novamente um usuário no ranking semanal (Managers ou acima).',
          options: [{ name: 'usuario', description: 'Usuário alvo', type: 6, required: true }],
        });
        await guild.commands.create({
          name: 'listar_excluidos',
          description: 'Lista os usuários excluídos do ranking (Managers ou acima).',
        });

        console.log('⌨️  Slash commands registrados em:', guild.name);
      } else {
        console.warn('⚠️ Não consegui localizar o guild do GUILD_ID para registrar comandos.');
      }
    } else {
      console.warn('⚠️ GUILD_ID não definido; comandos não serão registrados.');
    }
  } catch (err) {
    console.error('❌ Falha ao registrar comandos:', err);
  }

  // Criar/atualizar painel
  await criarPainel();
});

// -------------------------------------------------------------
// INTERAÇÕES (Slash + Buttons + SelectMenus)
// -------------------------------------------------------------
client.on('interactionCreate', async (interaction) => {
  // ========== SLASH COMMANDS ==========
  if (interaction.isChatInputCommand()) {

    // --- /justificar (semana toda) ---
    if (interaction.commandName === 'justificar') {
      if (!BOT_OWNER_ID) return interaction.reply({ content: '⚠️ BOT_OWNER_ID não configurado no .env.', flags: MessageFlags.Ephemeral });
      if (interaction.user.id !== BOT_OWNER_ID) {
        return interaction.reply({ content: `**${semacesso} Você não tem permissão para justificar membros.**`, flags: MessageFlags.Ephemeral });
      }

      const alvo = interaction.options.getUser('usuario');
      const motivo = interaction.options.getString('motivo');
      const guildIdLocal = interaction.guildId || 'default';
      const alvoRow = getUser(alvo.id, guildIdLocal);
      const periodStart = getLastResetTsForGuild(guildIdLocal) || 0;

      try {
        markJustified(alvoRow.id, guildIdLocal, periodStart, motivo, interaction.user.id);
        await sendJustificationLog(interaction.guild, { executorId: interaction.user.id, alvoId: alvo.id, motivo });
        return interaction.reply({ content: `✅ O membro **${alvo.username}** foi **justificado**.\n**Motivo:** ${motivo}`, flags: MessageFlags.Ephemeral });
      } catch (e) {
        console.error('[JUSTIFICAR] Erro ao marcar justificativa:', e);
        return interaction.reply({ content: '❌ Ocorreu um erro ao registrar a justificativa.', flags: MessageFlags.Ephemeral });
      }
    }

    // --- /justificar_dias (1 dia = -1h) — ACUMULA até a meta base ---
    if (interaction.commandName === 'justificar_dias') {
      if (!BOT_OWNER_ID) return interaction.reply({ content: '⚠️ BOT_OWNER_ID não configurado no .env.', flags: MessageFlags.Ephemeral });
      if (interaction.user.id !== BOT_OWNER_ID) {
        return interaction.reply({ content: `**${semacesso} Você não tem permissão para justificar dias.**`, flags: MessageFlags.Ephemeral });
      }

      const alvo = interaction.options.getUser('usuario');
      const diasSolicitados = interaction.options.getInteger('dias');
      const motivo = interaction.options.getString('motivo') || null;

      if (!Number.isInteger(diasSolicitados) || diasSolicitados < 0 || diasSolicitados > 7) {
        return interaction.reply({ content: `${emgwarning} Informe **dias** entre **0 e 7**.`, flags: MessageFlags.Ephemeral });
      }

      const guildIdLocal = interaction.guildId || 'default';
      const periodStart = getLastResetTsForGuild(guildIdLocal) || 0;
      const alvoRow = getUser(alvo.id, guildIdLocal);

      try {
        const diasAtuais = getJustifiedDaysCount(alvoRow.id, guildIdLocal, periodStart);

        // Limite: meta base do usuário (1 dia = 1h)
        const baseTargetHoras = getTargetHoursForUser(interaction.guild, alvo.id);
        const maxDiasPermitidos = Math.max(0, Math.floor(baseTargetHoras));

        const novoTotal = Math.min(maxDiasPermitidos, diasAtuais + diasSolicitados);
        const acrescimoReal = Math.max(0, novoTotal - diasAtuais);

        if (acrescimoReal === 0) {
          const msg = diasSolicitados === 0
            ? `${emgalerta} Nenhum dia foi acrescentado (foi informado **0**).`
            : `${emgalerta} **${alvo.username}** já atingiu o limite de **${maxDiasPermitidos}** dia(s) justificados para a meta desta semana.`;
          return interaction.reply({ content: msg, flags: MessageFlags.Ephemeral });
        }

        setJustifiedDaysCount(alvoRow.id, guildIdLocal, periodStart, novoTotal, motivo, interaction.user.id);

        await sendJustifiedDaysLog(interaction.guild, {
          executorId: interaction.user.id,
          alvoId: alvo.id,
          dias: acrescimoReal,
          motivo,
        });

        return interaction.reply({
          content: `${cumprido} **${alvo.username}** recebeu **+${acrescimoReal}** dia(s) justificado(s). ` +
                   `Total acumulado nesta semana: **${novoTotal}/${maxDiasPermitidos}** (1 dia = -1h na meta).`,
          flags: MessageFlags.Ephemeral,
        });
      } catch (e) {
        console.error('[JUSTIFICAR_DIAS] Erro:', e);
        return interaction.reply({ content: '❌ Erro ao registrar dias justificados.', flags: MessageFlags.Ephemeral });
      }
    }

    // --- /remover_justificativas (dias | tudo) ---
    if (interaction.commandName === 'remover_justificativas') {
      if (!BOT_OWNER_ID) return interaction.reply({ content: '⚠️ BOT_OWNER_ID não configurado no .env.', flags: MessageFlags.Ephemeral });
      if (interaction.user.id !== BOT_OWNER_ID) {
        return interaction.reply({ content: `**${semacesso} Você não tem permissão para remover justificativas.**`, flags: MessageFlags.Ephemeral });
      }

      const alvo = interaction.options.getUser('usuario');
      const tipo = interaction.options.getString('tipo');
      const diasParam = interaction.options.getInteger('dias');
      const motivo = interaction.options.getString('motivo') || null;

      const guildIdLocal = interaction.guildId || 'default';
      const periodStart = getLastResetTsForGuild(guildIdLocal) || 0;
      const alvoRow = getUser(alvo.id, guildIdLocal);

      try {
        if (tipo === 'dias') {
          const diasARemover = Number.isInteger(diasParam) ? diasParam : 0;
          if (diasARemover <= 0) {
            return interaction.reply({ content: `${emgwarning} Informe **dias** > **0** para remover.`, flags: MessageFlags.Ephemeral });
          }

          const { anteriores, removidos, novo } = removeJustifiedDays(alvoRow.id, guildIdLocal, periodStart, diasARemover);

          await sendRemovedJustifiedDaysLog(interaction.guild, {
            executorId: interaction.user.id,
            alvoId: alvo.id,
            removidos,
            totalFinal: novo,
            motivo,
          });

          return interaction.reply({
            content: `${cumprido} Removido(s) **${removidos}** dia(s) justificado(s) de **${alvo.username}**. ` +
                     `Total desta semana: **${novo}** (antes: ${anteriores}).`,
            flags: MessageFlags.Ephemeral,
          });
        }

        if (tipo === 'tudo') {
          setJustifiedDaysCount(alvoRow.id, guildIdLocal, periodStart, 0, motivo, interaction.user.id); // zera dias
          unmarkJustified(alvoRow.id, guildIdLocal, periodStart);                                     // remove full

          await sendRemovedAllJustificationsLog(interaction.guild, {
            executorId: interaction.user.id,
            alvoId: alvo.id,
            motivo,
          });

          return interaction.reply({
            content: `${cumprido} Todas as **justificativas** de **${alvo.username}** nesta semana foram **removidas/zeradas**.`,
            flags: MessageFlags.Ephemeral,
          });
        }

        return interaction.reply({ content: `${emgwarning} Tipo inválido. Use **dias** ou **tudo**.`, flags: MessageFlags.Ephemeral });
      } catch (e) {
        console.error('[REMOVER_JUSTIFICATIVAS] Erro:', e);
        return interaction.reply({ content: '❌ Erro ao remover justificativas.', flags: MessageFlags.Ephemeral });
      }
    }

    // --- /adicionar_horas | /remover_horas ---
    if (interaction.commandName === 'adicionar_horas' || interaction.commandName === 'remover_horas') {
      if (!BOT_OWNER_ID) return interaction.reply({ content: '⚠️ BOT_OWNER_ID não configurado no .env.', flags: MessageFlags.Ephemeral });
      if (interaction.user.id !== BOT_OWNER_ID) {
        return interaction.reply({ content: '**🚫 Você não tem permissão para adicionar ou remover horas.**', flags: MessageFlags.Ephemeral });
      }

      const alvo = interaction.options.getUser('usuario');
      const horas = interaction.options.getInteger('horas');
      const minutos = interaction.options.getInteger('minutos') || 0;
      const motivo = interaction.options.getString('motivo') || (interaction.commandName === 'adicionar_horas' ? 'Ajuste manual (adição)' : 'Ajuste manual (remoção)');

      if (horas < 0 || minutos < 0 || minutos >= 60) {
        return interaction.reply({ content: '**⚠️ | Valores inválidos. Use horas >= 0 e minutos entre 0 e 59.**', flags: MessageFlags.Ephemeral });
      }

      const guildId = interaction.guildId || 'default';
      const agora = Date.now();
      const sinal = interaction.commandName === 'adicionar_horas' ? 1 : -1;
      const deltaMs = sinal * (horas * 60 * 60 * 1000 + minutos * 60 * 1000);

      try {
        const userRow = getUser(alvo.id, guildId);
        db.prepare(
          `INSERT INTO adjustments (user_id, guild_id, delta_ms, reason, created_by, created_ts)
           VALUES (?, ?, ?, ?, ?, ?)`
        ).run(userRow.id, guildId, deltaMs, motivo, interaction.user.id, agora);

        await sendAdjustmentLog(interaction.guild, {
          tipo: sinal > 0 ? 'adicao' : 'remocao',
          executorId: interaction.user.id,
          alvoId: alvo.id,
          horas,
          minutos,
          motivo,
        });

        return interaction.reply({
          content: sinal > 0
            ? `**Você adicionou** **${horas}h ${minutos}min** **para** **${alvo.username}**.`
            : `**Você removeu** **${horas}h ${minutos}min** **de** **${alvo.username}**.`,
          flags: MessageFlags.Ephemeral,
        });
      } catch (e) {
        console.error('[AJUSTE] Erro ao inserir ajuste:', e);
        return interaction.reply({ content: '[ERRO] Não foi possível registrar o ajuste.', flags: MessageFlags.Ephemeral });
      }
    }

    // --- /desfazer_reset ---
    if (interaction.commandName === 'desfazer_reset') {
      if (!BOT_OWNER_ID) return interaction.reply({ content: '⚠️ BOT_OWNER_ID não configurado no .env.', flags: MessageFlags.Ephemeral });
      if (interaction.user.id !== BOT_OWNER_ID) {
        return interaction.reply({ content: `**${semacesso} Você não tem permissão para desfazer o reset.**`, flags: MessageFlags.Ephemeral });
      }

      const guildIdLocal = interaction.guildId || 'default';
      const { prev } = getResetBackup(guildIdLocal);
      if (!prev || prev <= 0) {
        return interaction.reply({ content: '**Não há reset anterior para desfazer.**', flags: MessageFlags.Ephemeral });
      }

      setLastResetTsForGuild(guildIdLocal, prev);
      clearResetBackup(guildIdLocal);

      try {
        let ch = await getOrCreateResetReportChannel(interaction.guild);
        if (!ch) ch = await getOrCreateLogChannel(interaction.guild);
        if (ch) await ch.send(`**${interaction.user}** desfez o reset. Contagem novamente desde **${fmtDateTime.format(new Date(prev))}**.`);
      } catch {}

      return interaction.reply({ content: `${cumprido}**Reset desfeito. O ranking voltou a contar desde** **${fmtDateTime.format(new Date(prev))}**.`, flags: MessageFlags.Ephemeral });
    }

    // --- Ranking: excluir/incluir/listar excluídos ---
    if (interaction.commandName === 'excluir_ranking') {
      if (!canModerate(interaction)) return interaction.reply({ content: `${semacesso} Você não tem permissão para isso.`, flags: MessageFlags.Ephemeral });
      const alvo = interaction.options.getUser('usuario');
      const guildIdLocal = interaction.guildId || 'default';
      const alvoRow = getUser(alvo.id, guildIdLocal);
      excludeUserFromRanking(guildIdLocal, alvoRow.id, interaction.user.id);
      return interaction.reply({ content: `${cumprido} **${alvo.username}** foi **removido do ranking** desta guild.`, flags: MessageFlags.Ephemeral });
    }
    if (interaction.commandName === 'incluir_ranking') {
      if (!canModerate(interaction)) return interaction.reply({ content: `${semacesso} Você não tem permissão para isso.`, flags: MessageFlags.Ephemeral });
      const alvo = interaction.options.getUser('usuario');
      const guildIdLocal = interaction.guildId || 'default';
      const alvoRow = getUser(alvo.id, guildIdLocal);
      includeUserInRanking(guildIdLocal, alvoRow.id);
      return interaction.reply({ content: `${cumprido} **${alvo.username}** foi **incluído novamente no ranking** desta guild.`, flags: MessageFlags.Ephemeral });
    }
    if (interaction.commandName === 'listar_excluidos') {
      if (!canModerate(interaction)) return interaction.reply({ content: `${semacesso} Você não tem permissão para isso.`, flags: MessageFlags.Ephemeral });
      const guildIdLocal = interaction.guildId || 'default';
      const rows = db.prepare(`
        SELECT re.user_id, u.discord_id AS discordId
        FROM ranking_excluded re
        JOIN users u ON u.id = re.user_id
        WHERE re.guild_id = ?
      `).all(guildIdLocal);

      if (!rows.length) return interaction.reply({ content: `${emgalerta} **Não há usuários excluídos do ranking.**`, flags: MessageFlags.Ephemeral });

      const discordIds = rows.map(r => String(r.discordId));
      const names = await resolveNamesBatch(interaction.guild, discordIds);
      const lista = rows.map(r => `• ${names.get(String(r.discordId)) ?? String(r.discordId)} \`(${r.discordId})\``).join('\n');

      return interaction.reply({ content: `**Excluídos do ranking:**\n${lista}`, flags: MessageFlags.Ephemeral });
    }
  }

  // ========== SELECT MENU (forçar saída) ==========
  if (interaction.isStringSelectMenu() && interaction.customId === 'forcar_saida_select') {
    if (!canModerate(interaction)) {
      const embed = new EmbedBuilder()
        .setColor('#FF0000')
        .setTitle('**Acesso Negado**')
        .setDescription(`Este recurso é **exclusivo para Managers**.\n\n${emgalerta} Verifique seu cargo e tente novamente.`);
      return interaction.reply({ embeds: [embed], flags: MessageFlags.Ephemeral });
    }

    const agora = Date.now();
    const guildId = interaction.guildId || 'default';
    const selectedSessionId = Number(interaction.values?.[0]);

    const sessao = db.prepare(`
      SELECT s.id, s.user_id, s.start_ts, u.discord_id as discordId
      FROM sessions s
      JOIN users u ON s.user_id = u.id
      WHERE s.id = ? AND s.end_ts IS NULL AND s.guild_id = ?
    `).get(selectedSessionId, guildId);

    if (!sessao) return interaction.update({ content: `**${emgalerta} | Essa sessão já foi finalizada ou não existe.**`, components: [] });

    db.prepare(`UPDATE sessions SET end_ts = ? WHERE id = ?`).run(agora, sessao.id);

    const totalMs = agora - sessao.start_ts;
    const horas = Math.floor(totalMs / 1000 / 60 / 60);
    const minutos = Math.floor((totalMs / 1000 / 60) % 60);

    try {
      const memberFinalizado = await interaction.guild.members.fetch(sessao.discordId).catch(() => null);
      const embedLog = buildLogEmbed('saida', memberFinalizado, sessao.start_ts, agora, { horas, minutos }, interaction.user);
      await sendLogEmbed(interaction.guild, embedLog);
    } catch (e) {
      console.error('[LOG][forcar_saida] Falha ao montar/enviar log:', e);
    }

    const nome = await resolveDisplayName(interaction.guild, sessao.discordId);
    return interaction.update({ content: `**${cumprido} Você finalizou o ponto de** **${nome}**.\n**Total de Horas registradas:** \`${horas}h ${minutos}min\`.`, components: [] });
  }

  // ========== BOTÕES ==========
  if (!interaction.isButton()) return;

  const userId = interaction.user.id;
  const agora = Date.now();
  const guildId = interaction.guildId || 'default';

  // --- Iniciar ponto ---
  if (interaction.customId === 'entrada') {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const user = getUser(userId, guildId);
    const existeAberto = db.prepare(`SELECT * FROM sessions WHERE user_id = ? AND guild_id = ? AND end_ts IS NULL`).get(user.id, guildId);

    if (existeAberto) return interaction.editReply({ content: `** ${emgalerta} | Você já está com ponto batido!**` });

    db.prepare(`INSERT INTO sessions (user_id, guild_id, start_ts, end_ts) VALUES (?, ?, ?, NULL)`).run(user.id, guildId, agora);
    await interaction.editReply({ content: `** ${emgregistro} | Ponto iniciado às ${fmtTime.format(new Date(agora))}**` });

    queueMicrotask(async () => {
      try {
        const member = await interaction.guild.members.fetch(userId).catch(() => null);
        const embedLog = buildLogEmbed('entrada', member, agora, null);
        await sendLogEmbed(interaction.guild, embedLog);
      } catch (e) { console.error('[LOG][entrada] Falha ao montar/enviar log:', e); }
    });
  }

  // --- Finalizar ponto ---
  if (interaction.customId === 'saida') {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const user = getUser(userId, guildId);
    const aberto = db.prepare(`SELECT * FROM sessions WHERE user_id = ? AND guild_id = ? AND end_ts IS NULL`).get(user.id, guildId);

    if (!aberto) return interaction.editReply({ content: `** ${emgalerta} | Você não possui ponto aberto para encerrar.**` });

    db.prepare(`UPDATE sessions SET end_ts = ? WHERE id = ?`).run(agora, aberto.id);

    const totalMs = agora - aberto.start_ts;
    const horas = Math.floor(totalMs / 1000 / 60 / 60);
    const minutos = Math.floor((totalMs / 1000 / 60) % 60);

    await interaction.editReply({ content: `**${emgregistroand} | Saída registrada!**\n**Total trabalhado: ${horas}h ${minutos}min.**` });

    queueMicrotask(async () => {
      try {
        const member = await interaction.guild.members.fetch(userId).catch(() => null);
        const embedLog = buildLogEmbed('saida', member, aberto.start_ts, agora, { horas, minutos });
        await sendLogEmbed(interaction.guild, embedLog);
      } catch (e) { console.error('[LOG][saida] Falha ao montar/enviar log:', e); }
    });
  }

  // --- Ativos agora ---
  if (interaction.customId === 'ativos') {
    const rows = db.prepare(`
      SELECT u.discord_id as discordId, s.start_ts
      FROM sessions s
      JOIN users u ON s.user_id = u.id
      WHERE s.end_ts IS NULL AND s.guild_id = ?
    `).all(guildId);

    if (!rows.length) return interaction.reply({ content: `${emgalerta} | **Nenhum membro está em trabalho no momento.**`, flags: MessageFlags.Ephemeral });

    const idsAtivos = rows.map(r => String(r.discordId));
    const namesAtivos = await resolveNamesBatch(interaction.guild, idsAtivos);

    let lista = '';
    for (const r of rows) {
      const nome = namesAtivos.get(String(r.discordId)) ?? String(r.discordId);
      lista += `${verificacao} **${nome}** — desde ${fmtTime.format(new Date(r.start_ts))}\n`;
    }

    const embed = new EmbedBuilder().setTitle(`**${membrosemtrabalhoemg} MEMBROS EM TRABALHO**`).setDescription(lista).setColor(0x57f287);
    return interaction.reply({ embeds: [embed], flags: MessageFlags.Ephemeral });
  }

  // --- Ranking semanal (sessões + ajustes) ---
  if (interaction.customId === 'ranking') {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const { rows, sinceTs: lastResetTs } = getWeeklyRowsForGuild(guildId);

    if (!rows.length) {
      const info = lastResetTs
        ? `** ${emgalerta} Ninguém registrou horas desde o último reset (${fmtDateTime.format(new Date(lastResetTs))}).**`
        : `** ${emgalerta} Ainda não há horas registradas.**`;
      return interaction.editReply({ content: info });
    }

    const ids = rows.map(r => String(r.discordId));
    const names = await resolveNamesBatch(interaction.guild, ids);

    let lista = '';
    for (const r of rows) {
      const totalMs = r.total || 0;
      const h = Math.max(0, Math.floor(totalMs / 1000 / 60 / 60));
      const m = Math.max(0, Math.floor((totalMs / 1000 / 60) % 60));
      const hFloat = msToHoursFloat(totalMs); // usa minutos também


      // Metas
      const targetBase = getTargetHoursForUser(interaction.guild, r.discordId);
      const targetEffective = getEffectiveTargetHoursForUser(interaction.guild, r.discordId, r.userId, guildId, lastResetTs);

      // Barra de progresso (evita dividir por 0)
      const progresso = Math.min(Math.floor((hFloat / Math.max(0.25, targetEffective)) * 10), 10);
      const barra = '█'.repeat(progresso) + '░'.repeat(10 - progresso);

      // Status com prioridade: meta base > justificado > andamento
      const diasJustificados = getJustifiedDaysCount(r.userId, guildId, lastResetTs);
      const fullJustificado = hasJustified(guildId, r.userId, lastResetTs);

      let status;
      if (hFloat >= targetBase) status = `**${cumprido} Meta cumprida**`;
      else if (fullJustificado || (diasJustificados > 0 && hFloat >= targetEffective)) status = `**${emgjustificado} Justificado**`;
      else status = `**${andamento} Em andamento**`;


      const nomeUsuario = names.get(String(r.discordId)) ?? String(r.discordId);
      lista += `**${nomeUsuario}**\n${barra} \`${h}h ${m}min\` • ${status}\n\n`;
    }

    const footer = lastResetTs ? `Desde: ${fmtDateTime.format(new Date(lastResetTs))}` : 'A contagem será resetada aos domingos!';
    const embed = new EmbedBuilder()
      .setTitle(`**${controleemg} Controle de Metas Semanais**`)
      .setColor(0x3498db)
      .setDescription(truncate(lista))
      .setFooter({ text: footer })
      .setTimestamp();

    return interaction.editReply({ embeds: [embed] });
  }

  // --- Forçar saída (Managers) ---
  if (interaction.customId === 'forcar_saida') {
    if (!canModerate(interaction)) {
      const embed = new EmbedBuilder()
        .setColor('#FF0000')
        .setTitle('**Acesso Negado**')
        .setDescription(`Este recurso é **exclusivo para Managers**.\n\n${emgalerta} Verifique seu cargo e tente novamente.`);
      return interaction.reply({ embeds: [embed], flags: MessageFlags.Ephemeral });
    }

    await interaction.deferReply({ flags: MessageFlags.Ephemeral });

    const abertos = db.prepare(`
      SELECT s.id as sessionId, u.discord_id as discordId, s.start_ts
      FROM sessions s
      JOIN users u ON s.user_id = u.id
      WHERE s.end_ts IS NULL AND s.guild_id = ?
      ORDER BY s.start_ts ASC
      LIMIT 25
    `).all(interaction.guildId || 'default');

    if (!abertos.length) return interaction.editReply({ content: `** ${emgalerta} | Nenhum membro possui ponto batido para ser finalizado.**` });

    const ids = abertos.map(r => String(r.discordId));
    const names = await resolveNamesBatch(interaction.guild, ids);

    const options = abertos.map((r) => ({
      label: `${names.get(String(r.discordId)) ?? String(r.discordId)} — desde ${fmtTime.format(new Date(r.start_ts))}`,
      value: String(r.sessionId),
    }));

    const select = new StringSelectMenuBuilder()
      .setCustomId('forcar_saida_select')
      .setPlaceholder('Selecione quem você quer finalizar')
      .addOptions(options);

    const row = new ActionRowBuilder().addComponents(select);

    return interaction.editReply({ content: '**Selecione um membro para finalizar o ponto:**', components: [row] });
  }

  // --- Zerar contagem (confirmação + relatório + backup/undo) ---
  if (interaction.customId === 'zerar') {
    if (!BOT_OWNER_ID) return interaction.reply({ content: '⚠️ BOT_OWNER_ID não configurado no .env.', flags: MessageFlags.Ephemeral });
    if (interaction.user.id !== BOT_OWNER_ID) {
      return interaction.reply({ content: '😅 **Ops! Só o DHorizonX pode zerar essa contagem. Você ainda não é ele!**', flags: MessageFlags.Ephemeral });
    }

    const last = getLastResetTsForGuild(guildId);
    const desde = last ? fmtDateTime.format(new Date(last)) : 'nunca (contagem total)';

    const confirmEmbed = new EmbedBuilder()
      .setTitle('**⚠️ Atenção**')
      .setDescription(`Você está preste a **zerar a contagem** a partir de agora. A contagem atual considera horas **desde ${desde}**.\n\nTem certeza que deseja fazer isso?`)
      .setColor(0xED4245);

    const row = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('confirmar_zerar').setLabel('Confirmar').setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId('cancelar_zerar').setLabel('Cancelar').setStyle(ButtonStyle.Secondary),
    );

    return interaction.reply({ embeds: [confirmEmbed], components: [row], flags: MessageFlags.Ephemeral });
  }

  if (interaction.customId === 'cancelar_zerar') {
    return interaction.update({ content: `**${cumprido} | Reset cancelado com sucesso!**`, embeds: [], components: [] });
  }

  if (interaction.customId === 'confirmar_zerar') {
    if (interaction.user.id !== BOT_OWNER_ID) {
      return interaction.reply({ content: '**[ERRO]** Você não tem permissão para confirmar o reset.', flags: MessageFlags.Ephemeral });
    }

    await interaction.update({ content: '**Processando...**', embeds: [], components: [] });

    const guildIdLocal = interaction.guildId || 'default';
    const agoraTs = Date.now();

    // 1) Resumo ANTES do reset
    const { rows, sinceTs } = getWeeklyRowsForGuild(guildIdLocal);
    const lista = await formatWeeklyRowsForLog(interaction.guild, rows, 30);
    const desdeFmt = sinceTs ? fmtDateTime.format(new Date(sinceTs)) : '—';

    const embedResumo = new EmbedBuilder()
      .setTitle('**Controle de Metas - Horas semanais**')
      .setColor(0x5865f2)
      .setDescription(truncate(lista))
      .addFields({ name: 'Contando desde', value: `\`${desdeFmt}\`` })
      .setFooter({ text: "Ponto eletrônico - DH Bot's" })
      .setTimestamp(agoraTs);

    // 2) Posta no canal de relatórios (fallback logs)
    try {
      let ch = await getOrCreateResetReportChannel(interaction.guild);
      if (!ch) ch = await getOrCreateLogChannel(interaction.guild);
      if (ch) {
        const mentionEquipe = TEAM_ROLE_ID ? `<@&${TEAM_ROLE_ID}>` : '';
        await ch.send({
          content: `⚠️ **Atenção ${mentionEquipe}!**\n\n**${interaction.user}** zerou a contagem semanal.\nSegue abaixo o relatório das horas registradas na semana passada.`,
          embeds: [embedResumo],
        });
      }
    } catch (e) {
      console.error('[ZERAR] Falha ao enviar resumo no canal de RELATÓRIOS:', e);
    }

    // 3) Salva BACKUP e zera oficialmente
    const prevTs = getLastResetTsForGuild(guildIdLocal) || 0;
    setResetBackup(guildIdLocal, prevTs, interaction.user.id, agoraTs);
    setLastResetTsForGuild(guildIdLocal, agoraTs);

    // 4) Sucesso + botão DESFAZER
    const quando = fmtDateTime.format(new Date(agoraTs));
    const undoRow = new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId('desfazer_reset').setLabel('Desfazer último reset').setStyle(ButtonStyle.Primary),
    );

    return interaction.followUp({
      content: `**${cumprido} | **A Contagem foi zerada agora (${quando}), e passa a contar a partir deste momento.`,
      components: [undoRow],
      flags: MessageFlags.Ephemeral,
    });
  }

  if (interaction.customId === 'desfazer_reset') {
    if (interaction.user.id !== BOT_OWNER_ID) {
      return interaction.reply({ content: `**${semacesso} | Você não tem permissão pra desfazer o reset.`, flags: MessageFlags.Ephemeral });
    }

    const guildIdLocal = interaction.guildId || 'default';
    const { prev } = getResetBackup(guildIdLocal);
    if (!prev || prev <= 0) {
      return interaction.reply({ content: 'Não consegui encontrar nenhum reset para ser desfeito.', flags: MessageFlags.Ephemeral });
    }

    setLastResetTsForGuild(guildIdLocal, prev);
    clearResetBackup(guildIdLocal);

    try {
      let ch = await getOrCreateResetReportChannel(interaction.guild);
      if (!ch) ch = await getOrCreateLogChannel(interaction.guild);
      if (ch) {
        await ch.send(`**Reset desfeito por ${interaction.user}.** A contagem voltou a considerar horas desde **${fmtDateTime.format(new Date(prev))}**.`);
      }
    } catch {}

    return interaction.reply({
      content: `Reset desfeito. O ranking voltou a contar desde **${fmtDateTime.format(new Date(prev))}**.`,
      flags: MessageFlags.Ephemeral,
    });
  }
});

// -------------------------------------------------------------
// LOGIN
// -------------------------------------------------------------
client.login(process.env.DISCORD_TOKEN);
