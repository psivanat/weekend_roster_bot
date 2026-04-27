require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { HttpsProxyAgent } = require('https-proxy-agent');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const WEBEX_BOT_TOKEN = process.env.WEBEX_BOT_TOKEN;
const BOT_EMAIL = process.env.BOT_EMAIL || 'TAC-Weekend-Roster-Manager@webex.bot';
const TEST_EMAIL = 'psivanat@cisco.com';

// Proxy support (required in your VM environment)
const PROXY_URL = process.env.HTTPS_PROXY || process.env.HTTP_PROXY;
const httpsAgent = PROXY_URL ? new HttpsProxyAgent(PROXY_URL) : undefined;

// Webex API client
const webex = axios.create({
  baseURL: 'https://webexapis.com/v1',
  headers: {
    Authorization: `Bearer ${WEBEX_BOT_TOKEN}`,
    'Content-Type': 'application/json'
  },
  timeout: 20000,
  httpsAgent,
  proxy: false // important when using custom proxy agent
});

// Temporary roster data
const roster = {
  today: ['Engineer A', 'Engineer B'],
  weekend: ['Engineer X', 'Engineer Y']
};

function normalize(text = '') {
  return text.trim().toLowerCase();
}

async function getMe() {
  const r = await webex.get('/people/me');
  return r.data;
}

async function getMessage(messageId) {
  const r = await webex.get(`/messages/${messageId}`);
  return r.data;
}

async function sendToRoom(roomId, text) {
  const r = await webex.post('/messages', { roomId, text });
  return r.data;
}

async function sendToPersonEmail(toPersonEmail, text) {
  const r = await webex.post('/messages', { toPersonEmail, text });
  return r.data;
}

function buildHelp() {
  return [
    '👋 TAC Weekend Roster Bot Commands:',
    '- help',
    '- ping',
    '- roster today',
    '- roster weekend'
  ].join('\n');
}

function handleCommand(rawText) {
  const text = normalize(rawText);

  if (text === 'help') return buildHelp();
  if (text === 'ping') return 'pong ✅';
  if (text === 'roster today') return `📅 Today roster:\n- ${roster.today.join('\n- ')}`;
  if (text === 'roster weekend') return `🗓️ Weekend roster:\n- ${roster.weekend.join('\n- ')}`;

  return `Sorry, I didn't understand that.\nType "help" to see commands.`;
}

// Health
app.get('/', (req, res) => {
  res.send('TAC Weekend Roster Bot is running');
});

app.get('/healthz', (req, res) => {
  res.json({
    ok: true,
    service: 'weekend-roster-bot',
    proxyConfigured: Boolean(PROXY_URL)
  });
});

// Diagnostic: bot identity
app.get('/me', async (req, res) => {
  try {
    const me = await getMe();
    res.json({ ok: true, me });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: (error.response && error.response.data) || error.message
    });
  }
});

// Fixed test notification (always to psivanat@cisco.com)
app.get('/notify-test', async (req, res) => {
  try {
    const msg = await sendToPersonEmail(
      TEST_EMAIL,
      '🔔 Test notification from TAC Weekend Roster Bot'
    );

    res.json({
      ok: true,
      sentTo: TEST_EMAIL,
      messageId: msg.id
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: (error.response && error.response.data) || error.message
    });
  }
});

// Simulate command locally
app.get('/simulate', (req, res) => {
  const text = req.query.text || '';
  const reply = handleCommand(text);
  res.json({ ok: true, input: text, reply });
});

// Webhook endpoint (activate when FQDN + HTTPS is ready)
app.post('/webhook', async (req, res) => {
  res.status(200).send('ok');

  try {
    const messageId = req.body && req.body.data && req.body.data.id;
    if (!messageId) return;

    const incoming = await getMessage(messageId);

    const sender = (incoming.personEmail || '').toLowerCase();
    if (sender === BOT_EMAIL.toLowerCase() || sender.endsWith('@webex.bot')) return;
    if (incoming.roomType !== 'direct') return;

    const reply = handleCommand(incoming.text || '');
    await sendToRoom(incoming.roomId, reply);
  } catch (error) {
    console.error('Webhook error:', (error.response && error.response.data) || error.message);
  }
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
  console.log(`Proxy in use: ${PROXY_URL || 'none'}`);
});
