import 'dotenv/config';
import express, { type Request, type Response } from 'express';
import * as cheerio from 'cheerio';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { Readable } from 'stream';
import { fileURLToPath } from 'url';
import { cert, getApps, initializeApp as initAdminApp } from 'firebase-admin/app';
import { getAuth as getAdminAuth } from 'firebase-admin/auth';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const AUTHORIZED_ADMIN_EMAIL = 'amd712785@gmail.com';
const DATA_DIR = path.resolve(__dirname, '.kurostream-data');
const BANNER_UPLOADS_DIR = path.join(DATA_DIR, 'uploads', 'banners');
const ANALYTICS_FILE_PATH = path.join(DATA_DIR, 'analytics.json');
fs.mkdirSync(BANNER_UPLOADS_DIR, { recursive: true });

interface FirebaseAppletConfig {
  projectId: string;
  apiKey: string;
  firestoreDatabaseId?: string;
}

function loadFirebaseAppletConfig(): FirebaseAppletConfig | null {
  try {
    const raw = fs.readFileSync(path.resolve(__dirname, 'firebase-applet-config.json'), 'utf8');
    const parsed = JSON.parse(raw) as Partial<FirebaseAppletConfig>;
    if (parsed.projectId && parsed.apiKey) {
      return {
        projectId: parsed.projectId,
        apiKey: parsed.apiKey,
        firestoreDatabaseId: parsed.firestoreDatabaseId || '(default)',
      };
    }
  } catch {
    // ignore missing config
  }
  return null;
}

const FIREBASE_CONFIG = loadFirebaseAppletConfig();

// Initialize Firebase Admin SDK for server-side token verification and custom claims
if (getApps().length === 0 && FIREBASE_CONFIG?.projectId) {
  try {
    const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    if (serviceAccountJson && serviceAccountJson.trim()) {
      const sa = JSON.parse(serviceAccountJson);
      initAdminApp({
        credential: cert(sa),
        projectId: FIREBASE_CONFIG.projectId,
      });
    } else {
      initAdminApp({
        projectId: FIREBASE_CONFIG.projectId,
      });
    }
  } catch (err) {
    // Falls back gracefully to Google Identity Toolkit token verification
  }
}

interface DailyAnalyticsBucket {
  date: string;
  visits: number;
  activeClientIds: string[];
  activeUserIds: string[];
  guestClientIds: string[];
}

interface AnalyticsStore {
  totalWebsiteVisits: number;
  knownClientIds: string[];
  daily: Record<string, DailyAnalyticsBucket>;
}

function getTodayDateKey(): string {
  return new Date().toISOString().slice(0, 10);
}

function sanitizeIdToken(val: unknown, maxLen = 128): string {
  if (typeof val !== 'string') return '';
  const trimmed = val.trim().slice(0, maxLen);
  return /^[a-zA-Z0-9_.\-:]+$/.test(trimmed) ? trimmed : '';
}

function loadAnalyticsStore(): AnalyticsStore {
  try {
    if (fs.existsSync(ANALYTICS_FILE_PATH)) {
      const raw = fs.readFileSync(ANALYTICS_FILE_PATH, 'utf8');
      const parsed = JSON.parse(raw) as Partial<AnalyticsStore>;
      return {
        totalWebsiteVisits:
          typeof parsed.totalWebsiteVisits === 'number' && parsed.totalWebsiteVisits >= 0
            ? parsed.totalWebsiteVisits
            : 0,
        knownClientIds: Array.isArray(parsed.knownClientIds) ? parsed.knownClientIds : [],
        daily: parsed.daily && typeof parsed.daily === 'object' ? parsed.daily : {},
      };
    }
  } catch {
    // start fresh if analytics file is unreadable
  }
  return {
    totalWebsiteVisits: 0,
    knownClientIds: [],
    daily: {},
  };
}

const analyticsStore: AnalyticsStore = loadAnalyticsStore();
const knownClientsSet = new Set<string>(analyticsStore.knownClientIds);

interface ActivePresenceSession {
  sessionId: string;
  clientId: string;
  uid?: string;
  lastSeen: number;
}

const activePresenceSessions = new Map<string, ActivePresenceSession>();
const seenVisitSessions = new Map<string, number>();

let analyticsPersistTimer: NodeJS.Timeout | null = null;
function scheduleAnalyticsPersist() {
  if (analyticsPersistTimer) return;
  analyticsPersistTimer = setTimeout(() => {
    analyticsPersistTimer = null;
    try {
      analyticsStore.knownClientIds = Array.from(knownClientsSet).slice(-50000);
      // Prune daily buckets older than 90 days to keep disk usage bounded
      const keys = Object.keys(analyticsStore.daily).sort();
      if (keys.length > 90) {
        for (const oldKey of keys.slice(0, keys.length - 90)) {
          delete analyticsStore.daily[oldKey];
        }
      }
      const tmpPath = `${ANALYTICS_FILE_PATH}.tmp`;
      fs.writeFileSync(tmpPath, JSON.stringify(analyticsStore, null, 2), 'utf8');
      fs.renameSync(tmpPath, ANALYTICS_FILE_PATH);
    } catch (err) {
      console.error('Failed to persist analytics store:', err);
    }
  }, 1000);
}

function getOrCreateDailyBucket(dateKey: string): DailyAnalyticsBucket {
  if (!analyticsStore.daily[dateKey]) {
    analyticsStore.daily[dateKey] = {
      date: dateKey,
      visits: 0,
      activeClientIds: [],
      activeUserIds: [],
      guestClientIds: [],
    };
  }
  const bucket = analyticsStore.daily[dateKey];
  if (!Array.isArray(bucket.activeClientIds)) bucket.activeClientIds = [];
  if (!Array.isArray(bucket.activeUserIds)) bucket.activeUserIds = [];
  if (!Array.isArray(bucket.guestClientIds)) bucket.guestClientIds = [];
  return bucket;
}

function recordActorActivity(clientId: string, uid?: string) {
  const today = getTodayDateKey();
  const bucket = getOrCreateDailyBucket(today);
  if (clientId) {
    knownClientsSet.add(clientId);
    if (!bucket.activeClientIds.includes(clientId)) {
      bucket.activeClientIds.push(clientId);
    }
  }
  if (uid) {
    if (!bucket.activeUserIds.includes(uid)) {
      bucket.activeUserIds.push(uid);
    }
    if (clientId) {
      // Remove this client from unauthenticated guest bucket once signed in today
      bucket.guestClientIds = bucket.guestClientIds.filter((c) => c !== clientId);
    }
  } else if (clientId && !bucket.guestClientIds.includes(clientId)) {
    bucket.guestClientIds.push(clientId);
  }
}

function pruneStalePresence(now = Date.now()): ActivePresenceSession[] {
  const active: ActivePresenceSession[] = [];
  for (const [sid, sess] of activePresenceSessions.entries()) {
    if (now - sess.lastSeen > 75_000) {
      activePresenceSessions.delete(sid);
    } else {
      active.push(sess);
    }
  }
  for (const [sid, ts] of seenVisitSessions.entries()) {
    if (now - ts > 12 * 60 * 60 * 1000) {
      seenVisitSessions.delete(sid);
    }
  }
  return active;
}

async function verifyAdminRequest(req: Request): Promise<{
  ok: boolean;
  status: number;
  error?: string;
  uid?: string;
  email?: string;
}> {
  if (!FIREBASE_CONFIG) {
    return { ok: false, status: 503, error: 'Firebase configuration is unavailable on server.' };
  }
  const authHeader = req.headers.authorization || '';
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match || !match[1]) {
    return { ok: false, status: 401, error: 'Missing Authorization Bearer token.' };
  }
  const idToken = match[1].trim();

  let verifiedEmail: string | undefined;
  let verifiedUid: string | undefined;
  let isEmailVerified = false;

  // 1. Try Firebase Admin SDK verifyIdToken if initialized
  if (getApps().length > 0) {
    try {
      const decoded = await getAdminAuth().verifyIdToken(idToken, true);
      verifiedEmail = decoded.email;
      verifiedUid = decoded.uid;
      isEmailVerified = decoded.email_verified === true;
    } catch {
      // Admin SDK verification failed or lacked service account certs; fall back to Identity Toolkit
    }
  }

  // 2. Identity Toolkit REST verification (authoritative Google verification of ID token)
  if (!verifiedEmail) {
    try {
      const lookupRes = await fetchWithTimeout(
        `https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${encodeURIComponent(FIREBASE_CONFIG.apiKey)}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ idToken }),
        },
        6000
      );

      if (!lookupRes.ok) {
        return { ok: false, status: 401, error: 'Invalid or expired authentication token.' };
      }

      const lookupData = (await lookupRes.json()) as {
        users?: Array<{ localId?: string; email?: string; emailVerified?: boolean }>;
      };
      const account = lookupData.users?.[0];
      verifiedEmail = account?.email;
      verifiedUid = account?.localId;
      isEmailVerified = account?.emailVerified === true;
    } catch (err) {
      return {
        ok: false,
        status: 500,
        error: err instanceof Error ? err.message : 'Failed to verify administrator credentials.',
      };
    }
  }

  if (!verifiedUid || !verifiedEmail || !isEmailVerified) {
    return { ok: false, status: 403, error: 'Verified email authentication is required.' };
  }

  const normalizedEmail = verifiedEmail.toLowerCase();
  if (normalizedEmail !== AUTHORIZED_ADMIN_EMAIL.toLowerCase()) {
    return {
      ok: false,
      status: 403,
      error: 'Access denied: Only amd712785@gmail.com is authorized as administrator.',
    };
  }

  // Assign Firebase Admin custom claims { admin: true } to the authorized admin if available
  if (getApps().length > 0) {
    try {
      const adminAuth = getAdminAuth();
      const userRec = await adminAuth.getUser(verifiedUid);
      if (!userRec.customClaims?.admin) {
        await adminAuth.setCustomUserClaims(verifiedUid, { admin: true });
      }
    } catch {
      // non-fatal if IAM permissions do not allow modifying custom claims
    }
  }

  return { ok: true, status: 200, uid: verifiedUid, email: verifiedEmail };
}

function detectVerifiedImageFormat(buffer: Buffer): { mime: string; ext: string } | null {
  if (buffer.length < 12) return null;
  // JPEG: FF D8 FF
  if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
    return { mime: 'image/jpeg', ext: '.jpg' };
  }
  // PNG: 89 50 4E 47 0D 0A 1A 0A
  if (
    buffer[0] === 0x89 &&
    buffer[1] === 0x50 &&
    buffer[2] === 0x4e &&
    buffer[3] === 0x47 &&
    buffer[4] === 0x0d &&
    buffer[5] === 0x0a &&
    buffer[6] === 0x1a &&
    buffer[7] === 0x0a
  ) {
    return { mime: 'image/png', ext: '.png' };
  }
  // GIF: GIF87a / GIF89a
  const header6 = buffer.subarray(0, 6).toString('ascii');
  if (header6 === 'GIF87a' || header6 === 'GIF89a') {
    return { mime: 'image/gif', ext: '.gif' };
  }
  // WEBP: RIFF....WEBP
  if (
    buffer.subarray(0, 4).toString('ascii') === 'RIFF' &&
    buffer.subarray(8, 12).toString('ascii') === 'WEBP'
  ) {
    return { mime: 'image/webp', ext: '.webp' };
  }
  return null;
}

const PORT = Number(process.env.PORT || 3000);
const DEFAULT_STREAMING_API = (
  process.env.VITE_STREAMING_API_BASE || 'https://ashanime-api.vercel.app'
).replace(/\/+$/, '');
const ANILIST_API_URL = process.env.VITE_ANILIST_API_URL || 'https://graphql.anilist.co';
const ANIYOMI_REPO_URL =
  'https://gitea.k3vinb5.dev/Backups/kohi-den-extensions/raw/branch/main/index.min.json';
const TACHIYOMI_REPO_URL =
  'https://gitea.k3vinb5.dev/Backups/keiyoushi-extensions/raw/branch/repo/index.min.json';
const ANIZIP_API_URL = 'https://api.ani.zip/mappings';
const ANISKIP_API_URL = 'https://api.aniskip.com/v2/skip-times';
const ANIMEWORLD_UPSTREAM = (
  process.env.ANIMEWORLD_UPSTREAM_URL || 'https://animesky.app'
).replace(/\/+$/, '');

const BROWSER_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36';

interface CacheEntry<T> {
  expiresAt: number;
  data: T;
}

const aniZipCache = new Map<number, CacheEntry<Record<string, unknown>>>();
const aniSkipCache = new Map<string, CacheEntry<Record<string, unknown>>>();
let extensionsCache: CacheEntry<Record<string, unknown>> | null = null;

interface ResolvedSource {
  url: string;
  quality: string;
  isM3U8: boolean;
  type: 'hls' | 'mp4' | 'embed';
}

interface ResolvedSubtitle {
  url: string;
  lang: string;
  label: string;
}

function normalizeUrl(raw: string): string {
  return raw.trim().replace(/^https:\/{3,}/i, 'https://').replace(/^http:\/{3,}/i, 'http://');
}

function isBlockedPrivateHost(hostname: string): boolean {
  const h = hostname.toLowerCase();
  if (
    h === 'localhost' ||
    h === '127.0.0.1' ||
    h === '::1' ||
    h === '0.0.0.0' ||
    h.endsWith('.local') ||
    h.endsWith('.internal')
  ) {
    return true;
  }
  if (/^10\./.test(h) || /^192\.168\./.test(h) || /^169\.254\./.test(h)) {
    return true;
  }
  const m = h.match(/^172\.(\d+)\./);
  if (m) {
    const second = Number(m[1]);
    if (second >= 16 && second <= 31) return true;
  }
  return false;
}

function convertSrtToVtt(srt: string): string {
  const cleaned = srt.replace(/\r+/g, '').trim();
  const converted = cleaned.replace(
    /(\d{2}:\d{2}:\d{2}),(\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}),(\d{3})/g,
    '$1.$2 --> $3.$4'
  );
  return `WEBVTT\n\n${converted}\n`;
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit = {},
  timeoutMs = 7000
): Promise<globalThis.Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function resolveMalIdFromAnilist(anilistId: number): Promise<number | null> {
  if (!Number.isFinite(anilistId) || anilistId <= 0) return null;
  try {
    const res = await fetchWithTimeout(
      ANILIST_API_URL,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({
          query: `query ($id: Int) { Media(id: $id, type: ANIME) { id idMal } }`,
          variables: { id: anilistId },
        }),
      },
      5000
    );
    if (!res.ok) return null;
    const json = (await res.json()) as { data?: { Media?: { idMal?: number | null } } };
    const idMal = json.data?.Media?.idMal;
    return typeof idMal === 'number' && idMal > 0 ? idMal : null;
  } catch {
    return null;
  }
}

async function verifyStreamPlaylistReachable(url: string, referer: string): Promise<boolean> {
  try {
    const res = await fetchWithTimeout(
      url,
      {
        method: 'GET',
        headers: {
          Referer: referer,
          'User-Agent': BROWSER_UA,
          Accept: '*/*',
        },
      },
      5000
    );
    if (!res.ok) return false;
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (url.toLowerCase().includes('.m3u8') || ct.includes('mpegurl')) {
      const text = await res.text();
      return text.includes('#EXTM3U');
    }
    return true;
  } catch {
    return false;
  }
}

async function fetchAniZipMetadata(anilistId: number): Promise<Record<string, unknown>> {
  const cached = aniZipCache.get(anilistId);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.data;
  }

  try {
    const res = await fetchWithTimeout(
      `${ANIZIP_API_URL}?anilist_id=${encodeURIComponent(String(anilistId))}`,
      { headers: { Accept: 'application/json', 'User-Agent': BROWSER_UA } },
      6500
    );
    if (!res.ok) {
      return {
        found: false,
        anilistId,
        mappings: {},
        episodeCount: 0,
        episodes: {},
        error: `AniZip returned HTTP ${res.status}`,
      };
    }

    const raw = (await res.json()) as {
      mappings?: Record<string, unknown>;
      episodeCount?: number;
      episodes?: Record<string, Record<string, unknown>>;
    };

    const rawMappings = raw.mappings || {};
    const mappings = {
      anilistId: typeof rawMappings.anilist_id === 'number' ? rawMappings.anilist_id : anilistId,
      malId: typeof rawMappings.mal_id === 'number' ? rawMappings.mal_id : null,
      anidbId: typeof rawMappings.anidb_id === 'number' ? rawMappings.anidb_id : null,
      thetvdbId: typeof rawMappings.thetvdb_id === 'number' ? rawMappings.thetvdb_id : null,
      kitsuId: typeof rawMappings.kitsu_id === 'number' ? rawMappings.kitsu_id : null,
      imdbId: typeof rawMappings.imdb_id === 'string' ? rawMappings.imdb_id : null,
      themoviedbId:
        rawMappings.themoviedb_id !== undefined && rawMappings.themoviedb_id !== null
          ? String(rawMappings.themoviedb_id)
          : null,
    };

    const episodes: Record<string, Record<string, unknown>> = {};
    const rawEpisodes = raw.episodes || {};
    for (const [key, epObj] of Object.entries(rawEpisodes)) {
      if (!/^\d+$/.test(key) || !epObj || typeof epObj !== 'object') continue;
      const epNum = Number(key);
      if (!Number.isFinite(epNum) || epNum <= 0) continue;

      const titleObj =
        epObj.title && typeof epObj.title === 'object'
          ? (epObj.title as Record<string, string>)
          : {};
      const preferredTitle =
        titleObj.en ||
        titleObj['x-jat'] ||
        titleObj.ja ||
        `Episode ${epNum}`;

      episodes[String(epNum)] = {
        episodeNumber: epNum,
        absoluteEpisodeNumber:
          typeof epObj.absoluteEpisodeNumber === 'number' ? epObj.absoluteEpisodeNumber : undefined,
        seasonNumber: typeof epObj.seasonNumber === 'number' ? epObj.seasonNumber : undefined,
        title: preferredTitle,
        nativeTitle: titleObj.ja || undefined,
        romajiTitle: titleObj['x-jat'] || undefined,
        overview:
          typeof epObj.overview === 'string'
            ? epObj.overview
            : typeof epObj.summary === 'string'
              ? epObj.summary
              : undefined,
        image: typeof epObj.image === 'string' ? epObj.image : null,
        airDate:
          typeof epObj.airDate === 'string'
            ? epObj.airDate
            : typeof epObj.airdate === 'string'
              ? epObj.airdate
              : null,
        runtime:
          typeof epObj.runtime === 'number'
            ? epObj.runtime
            : typeof epObj.length === 'number'
              ? epObj.length
              : null,
        rating: typeof epObj.rating === 'string' ? epObj.rating : null,
      };
    }

    const normalized = {
      found: Object.keys(episodes).length > 0 || Boolean(mappings.malId),
      anilistId,
      mappings,
      episodeCount:
        typeof raw.episodeCount === 'number' ? raw.episodeCount : Object.keys(episodes).length,
      episodes,
    };

    aniZipCache.set(anilistId, {
      expiresAt: Date.now() + 30 * 60 * 1000,
      data: normalized,
    });
    return normalized;
  } catch (err) {
    return {
      found: false,
      anilistId,
      mappings: {},
      episodeCount: 0,
      episodes: {},
      error: err instanceof Error ? err.message : 'Failed to query AniZip.',
    };
  }
}

async function fetchAniSkipTimestamps(
  malId: number,
  episodeNumber: number,
  episodeLength = 0
): Promise<{
  found: boolean;
  malId: number;
  episodeNumber: number;
  intro?: { start: number; end: number; type: string; source: 'aniskip' };
  outro?: { start: number; end: number; type: string; source: 'aniskip' };
  recap?: { start: number; end: number; type: string; source: 'aniskip' };
  skipTimes: Array<{ start: number; end: number; type: string; source: 'aniskip' }>;
}> {
  const cacheKey = `${malId}:${episodeNumber}`;
  const cached = aniSkipCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.data as ReturnType<typeof fetchAniSkipTimestamps> extends Promise<infer R> ? R : never;
  }

  try {
    const url =
      `${ANISKIP_API_URL}/${malId}/${episodeNumber}` +
      `?types[]=op&types[]=ed&types[]=mixed-op&types[]=mixed-ed&types[]=recap&episodeLength=${Math.max(
        0,
        Math.floor(episodeLength)
      )}`;
    const res = await fetchWithTimeout(
      url,
      { headers: { Accept: 'application/json', 'User-Agent': BROWSER_UA } },
      5000
    );

    if (!res.ok) {
      const empty = { found: false, malId, episodeNumber, skipTimes: [] };
      aniSkipCache.set(cacheKey, { expiresAt: Date.now() + 10 * 60 * 1000, data: empty });
      return empty;
    }

    const json = (await res.json()) as {
      found?: boolean;
      results?: Array<{
        skipType?: string;
        interval?: { startTime?: number; endTime?: number };
      }>;
    };

    const skipTimes: Array<{ start: number; end: number; type: string; source: 'aniskip' }> = [];
    for (const item of json.results || []) {
      const start = item.interval?.startTime;
      const end = item.interval?.endTime;
      const type = item.skipType || 'op';
      if (typeof start === 'number' && typeof end === 'number' && end > start) {
        skipTimes.push({ start, end, type, source: 'aniskip' });
      }
    }

    const intro =
      skipTimes.find((s) => s.type === 'op') || skipTimes.find((s) => s.type === 'mixed-op');
    const outro =
      skipTimes.find((s) => s.type === 'ed') || skipTimes.find((s) => s.type === 'mixed-ed');
    const recap = skipTimes.find((s) => s.type === 'recap');

    const payload = {
      found: skipTimes.length > 0,
      malId,
      episodeNumber,
      intro,
      outro,
      recap,
      skipTimes,
    };

    aniSkipCache.set(cacheKey, {
      expiresAt: Date.now() + 30 * 60 * 1000,
      data: payload,
    });
    return payload;
  } catch {
    return { found: false, malId, episodeNumber, skipTimes: [] };
  }
}

interface RawRepoExt {
  name?: string;
  pkg?: string;
  apk?: string;
  lang?: string;
  code?: number;
  version?: string;
  nsfw?: number;
  sources?: Array<{ name?: string; lang?: string; id?: string; baseUrl?: string }>;
}

const ADAPTED_ANIYOMI_SOURCES: Record<string, { server: string; note: string }> = {
  'eu.kanade.tachiyomi.animeextension.en.zoro': {
    server: 'vidstreaming',
    note: 'Backend Node.js adapter enabled (HiAnime/Zoro → Vidstreaming/Megacloud HLS stream).',
  },
  'eu.kanade.tachiyomi.animeextension.en.aniwatch': {
    server: 'megacloud',
    note: 'Backend Node.js adapter enabled (AniWatch → Megacloud/Vidstreaming HLS stream).',
  },
  'eu.kanade.tachiyomi.animeextension.en.kaido': {
    server: 'vidstreaming',
    note: 'Backend Node.js adapter enabled (Kaido → Vidstreaming/Megacloud HLS stream).',
  },
  'eu.kanade.tachiyomi.animeextension.en.animepahe': {
    server: 'streamsb',
    note: 'Backend Node.js adapter enabled (AnimePahe → Streamsb/Megacloud HLS fallback).',
  },
  'eu.kanade.tachiyomi.animeextension.all.animeonsen': {
    server: 'vidstreaming',
    note: 'Backend Node.js adapter enabled (AnimeOnsen → Vidstreaming HLS stream).',
  },
  'eu.kanade.tachiyomi.animeextension.all.anizone': {
    server: 'megacloud',
    note: 'Backend Node.js adapter enabled (AniZone → Megacloud HLS stream).',
  },
};

async function fetchExtensionsCatalog(): Promise<Record<string, unknown>> {
  if (extensionsCache && extensionsCache.expiresAt > Date.now()) {
    return extensionsCache.data;
  }

  const [aniyomiResult, tachiyomiResult] = await Promise.allSettled([
    fetchWithTimeout(
      ANIYOMI_REPO_URL,
      { headers: { Accept: 'application/json', 'User-Agent': BROWSER_UA } },
      10000
    ).then(async (r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return (await r.json()) as RawRepoExt[];
    }),
    fetchWithTimeout(
      TACHIYOMI_REPO_URL,
      { headers: { Accept: 'application/json', 'User-Agent': BROWSER_UA } },
      10000
    ).then(async (r) => {
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return (await r.json()) as RawRepoExt[];
    }),
  ]);

  const extensions: Array<Record<string, unknown>> = [];

  const mapAniyomi = (items: RawRepoExt[]) => {
    let compatCount = 0;
    for (const item of items) {
      const pkg = String(item.pkg || '');
      const adapter = ADAPTED_ANIYOMI_SOURCES[pkg];
      const compatible = Boolean(adapter);
      if (compatible) compatCount += 1;

      extensions.push({
        id: `aniyomi:${pkg || item.name}`,
        repoType: 'aniyomi',
        repoLabel: 'Aniyomi (Kohi-den)',
        name: String(item.name || 'Unknown Aniyomi Extension').replace(/^Aniyomi:\s*/i, ''),
        pkg,
        apk: String(item.apk || ''),
        lang: String(item.lang || 'all'),
        code: Number(item.code || 0),
        version: String(item.version || '0.0'),
        nsfw: Number(item.nsfw || 0) === 1,
        sources: Array.isArray(item.sources)
          ? item.sources.map((s) => ({
              id: String(s.id || ''),
              name: String(s.name || ''),
              lang: String(s.lang || item.lang || 'all'),
              baseUrl: String(s.baseUrl || ''),
            }))
          : [],
        compatibility: compatible ? 'adapted' : 'android-runtime-required',
        compatible,
        adapterServer: adapter?.server,
        limitationReason: adapter
          ? adapter.note
          : 'Requires Android Aniyomi runtime (.apk); Dalvik/JVM extractors cannot execute directly in a web browser.',
      });
    }
    return compatCount;
  };

  const mapTachiyomi = (items: RawRepoExt[]) => {
    for (const item of items) {
      const pkg = String(item.pkg || '');
      extensions.push({
        id: `tachiyomi:${pkg || item.name}`,
        repoType: 'tachiyomi',
        repoLabel: 'Tachiyomi (Keiyoushi)',
        name: String(item.name || 'Unknown Tachiyomi Extension'),
        pkg,
        apk: String(item.apk || ''),
        lang: String(item.lang || 'all'),
        code: Number(item.code || 0),
        version: String(item.version || '0.0'),
        nsfw: Number(item.nsfw || 0) === 1,
        sources: Array.isArray(item.sources)
          ? item.sources.map((s) => ({
              id: String(s.id || ''),
              name: String(s.name || ''),
              lang: String(s.lang || item.lang || 'all'),
              baseUrl: String(s.baseUrl || ''),
            }))
          : [],
        compatibility: 'manga-incompatible',
        compatible: false,
        limitationReason:
          'Tachiyomi/Mihon Android manga extension (.apk); incompatible with browser anime video streaming.',
      });
    }
  };

  const aniyomiItems = aniyomiResult.status === 'fulfilled' && Array.isArray(aniyomiResult.value) ? aniyomiResult.value : [];
  const tachiyomiItems =
    tachiyomiResult.status === 'fulfilled' && Array.isArray(tachiyomiResult.value) ? tachiyomiResult.value : [];

  const aniyomiCompatCount = mapAniyomi(aniyomiItems);
  mapTachiyomi(tachiyomiItems);

  // Sort adapted extensions to the top for convenience
  extensions.sort((a, b) => {
    if (Boolean(a.compatible) !== Boolean(b.compatible)) {
      return a.compatible ? -1 : 1;
    }
    return String(a.name).localeCompare(String(b.name));
  });

  const payload = {
    repositories: [
      {
        id: 'aniyomi',
        name: 'Aniyomi Extensions (Kohi-den)',
        url: ANIYOMI_REPO_URL,
        status: aniyomiResult.status === 'fulfilled' ? 'ok' : 'error',
        totalCount: aniyomiItems.length,
        compatibleCount: aniyomiCompatCount,
        error:
          aniyomiResult.status === 'rejected'
            ? aniyomiResult.reason instanceof Error
              ? aniyomiResult.reason.message
              : 'Failed to load Aniyomi repository'
            : undefined,
      },
      {
        id: 'tachiyomi',
        name: 'Tachiyomi Extensions (Keiyoushi)',
        url: TACHIYOMI_REPO_URL,
        status: tachiyomiResult.status === 'fulfilled' ? 'ok' : 'error',
        totalCount: tachiyomiItems.length,
        compatibleCount: 0,
        error:
          tachiyomiResult.status === 'rejected'
            ? tachiyomiResult.reason instanceof Error
              ? tachiyomiResult.reason.message
              : 'Failed to load Tachiyomi repository'
            : undefined,
      },
    ],
    extensions,
    fetchedAt: new Date().toISOString(),
  };

  extensionsCache = {
    expiresAt: Date.now() + 15 * 60 * 1000,
    data: payload,
  };
  return payload;
}

function normalizeAnimeWorldImg(src?: string | null): string | null {
  if (!src) return null;
  const trimmed = src.trim();
  if (!trimmed || trimmed.startsWith('data:image/svg')) return null;
  if (trimmed.startsWith('//')) return `https:${trimmed}`;
  return trimmed;
}

function extractSlugFromHref(href: string | undefined, segment: string): string | null {
  if (!href) return null;
  const match = href.match(new RegExp(`/${segment}/([^/?#]+)`, 'i'));
  return match ? match[1].trim() : null;
}

function parseAnimeWorldCards($: cheerio.CheerioAPI, root?: cheerio.Cheerio<any>) {
  const container = root || $.root();
  const items: Array<Record<string, unknown>> = [];
  const seen = new Set<string>();

  container.find('article.tr-movie-card').each((_, el) => {
    const card = $(el);
    const linkEl = card.find('a.tr-movie-link').first();
    const href = linkEl.attr('href') || '';
    const seriesId = extractSlugFromHref(href, 'series');
    const movieId =
      extractSlugFromHref(href, 'movies') || extractSlugFromHref(href, 'movie');
    const idKey = seriesId ? `series:${seriesId}` : movieId ? `movie:${movieId}` : '';
    if (!idKey || seen.has(idKey)) return;
    seen.add(idKey);

    const imgEl = card.find('img').first();
    const imgRaw = imgEl.attr('data-src') || imgEl.attr('src');
    const title = card.find('.tr-movie-title, h3, h2').first().text().trim();
    const year = card.find('.year, .tr-year').first().text().trim() || null;
    const rating = card.find('.vote, .tr-rating').first().text().trim() || null;
    if (!title) return;

    items.push({
      title,
      image: normalizeAnimeWorldImg(imgRaw),
      year,
      rating,
      type: seriesId ? 'series' : 'movie',
      ...(seriesId ? { seriesId } : {}),
      ...(movieId ? { movieId } : {}),
    });
  });

  // Fallback for legacy Torofilm post-lst markup if present
  if (items.length === 0) {
    container.find('ul.post-lst li.status-publish').each((_, el) => {
      const li = $(el);
      const href = li.find('a.lnk-blk').first().attr('href') || '';
      const seriesId = extractSlugFromHref(href, 'series');
      const movieId =
        extractSlugFromHref(href, 'movies') || extractSlugFromHref(href, 'movie');
      const idKey = seriesId ? `series:${seriesId}` : movieId ? `movie:${movieId}` : '';
      if (!idKey || seen.has(idKey)) return;
      seen.add(idKey);

      const imgEl = li.find('img').first();
      const imgRaw = imgEl.attr('data-src') || imgEl.attr('src');
      const title = li.find('h2.entry-title, h3').first().text().trim();
      const year = li.find('span.year').first().text().trim() || null;
      const rating = li.find('span.vote').first().text().trim() || null;
      if (!title) return;

      items.push({
        title,
        image: normalizeAnimeWorldImg(imgRaw),
        year,
        rating,
        type: seriesId ? 'series' : 'movie',
        ...(seriesId ? { seriesId } : {}),
        ...(movieId ? { movieId } : {}),
      });
    });
  }

  return items;
}

function classifyAnimeWorldStreamType(url: string): 'hls' | 'mp4' | 'embed' {
  const lower = url.toLowerCase();
  if (lower.includes('.m3u8')) return 'hls';
  if (/\.(mp4|webm|mkv)(\?|$)/i.test(lower)) return 'mp4';
  return 'embed';
}

async function fetchAnimeWorldHtml(url: string): Promise<{ ok: boolean; status: number; html: string }> {
  try {
    const res = await fetchWithTimeout(
      url,
      {
        headers: {
          'User-Agent': BROWSER_UA,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept-Encoding': 'gzip, deflate, br',
          'Referer': `${ANIMEWORLD_UPSTREAM}/`,
          'Origin': ANIMEWORLD_UPSTREAM,
          'Sec-Ch-Ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
          'Sec-Ch-Ua-Mobile': '?0',
          'Sec-Ch-Ua-Platform': '"Windows"',
          'Sec-Fetch-Dest': 'document',
          'Sec-Fetch-Mode': 'navigate',
          'Sec-Fetch-Site': 'same-origin',
          'Upgrade-Insecure-Requests': '1',
        },
      },
      10000
    );
    const html = res.ok ? await res.text() : '';
    if (!res.ok) {
      console.warn(`[AnimeWorld Diagnostic] Upstream request failed: URL=${url}, Status=${res.status} ${res.statusText}`);
    }
    return { ok: res.ok, status: res.status, html };
  } catch (err) {
    console.error(`[AnimeWorld Diagnostic] Upstream fetch error: URL=${url}, Error=${err instanceof Error ? err.message : String(err)}`);
    return { ok: false, status: 502, html: '' };
  }
}

async function verifyRavokAudioLanguages(ravokUrl: string): Promise<string[]> {
  try {
    const res = await fetchWithTimeout(
      ravokUrl,
      {
        headers: {
          Referer: `${ANIMEWORLD_UPSTREAM}/`,
          'User-Agent': BROWSER_UA,
          Accept: 'text/html,*/*',
        },
      },
      4500
    );
    if (!res.ok) return [];
    const html = await res.text();
    const match = html.match(/var\s+defaultAudio\s*=\s*(\[[^\]]+\])/);
    if (match) {
      const codes = JSON.parse(match[1]) as string[];
      const codeMap: Record<string, string> = {
        hin: 'Hindi',
        eng: 'English',
        jpn: 'Japanese',
        tam: 'Tamil',
        tel: 'Telugu',
        ben: 'Bengali',
        mal: 'Malayalam',
        mar: 'Marathi',
        kan: 'Kannada',
        kor: 'Korean',
      };
      return codes
        .map((c) => codeMap[c.toLowerCase()])
        .filter((x): x is string => Boolean(x));
    }
  } catch {
    // ignore ravok metadata inspection timeout
  }
  return [];
}

async function resolveFromMegavid(
  malId: number,
  episodeNumber: number,
  category: 'sub' | 'dub' | 'hindi',
  requestedServer: string
): Promise<{
  ok: boolean;
  statusCode?: number;
  message?: string;
  payload?: Record<string, unknown>;
}> {
  const catToUse = category === 'hindi' ? 'dub' : category;
  const refererUrl = `https://megavid.buzz/mal/${malId}/${episodeNumber}/${catToUse}`;
  const baseSourceUrl = `${refererUrl}/source`;

  // Map requested server names (including adapted Aniyomi source identifiers) to megavid query strategies
  const serverLower = requestedServer.toLowerCase();
  const candidateQueries: Array<{ qs: string; label: string }> =
    serverLower === 'streamsb' || serverLower.includes('animepahe')
      ? [
          { qs: 'axsv=sora', label: 'Streamsb' },
          { qs: 'noax=1', label: 'Megacloud' },
        ]
      : serverLower === 'vidstreaming' ||
          serverLower.includes('hianime') ||
          serverLower.includes('zoro') ||
          serverLower.includes('kaido') ||
          serverLower.includes('animeonsen')
        ? [
            { qs: 'noax=1', label: 'Vidstreaming' },
            { qs: 'axsv=yuki', label: 'Vidstreaming' },
          ]
        : [
            { qs: 'noax=1', label: 'Megacloud' },
            { qs: 'axsv=nero', label: 'Megacloud' },
          ];

  let lastMessage = 'Stream not available for this episode.';
  let lastStatus = 404;

  for (const candidate of candidateQueries) {
    try {
      const targetUrl = `${baseSourceUrl}?${candidate.qs}`;
      const res = await fetchWithTimeout(
        targetUrl,
        {
          headers: {
            Referer: refererUrl,
            'User-Agent': BROWSER_UA,
            Accept: 'application/json',
          },
        },
        7000
      );

      if (!res.ok) {
        lastStatus = res.status;
        lastMessage = `Upstream stream endpoint returned HTTP ${res.status}.`;
        continue;
      }

      const data = (await res.json()) as {
        status?: string;
        code?: number;
        message?: string;
        source?: string;
        type?: string;
        tracks?: Array<{ file?: string; label?: string; kind?: string; default?: boolean }>;
        chapters?: Array<{ title?: string; start?: number; end?: number }>;
      };

      if (data.status !== 'ok' || !data.source) {
        lastStatus = typeof data.code === 'number' ? data.code : 404;
        lastMessage = data.message || 'Episode stream not found on provider.';
        continue;
      }

      const cleanSourceUrl = normalizeUrl(data.source);
      const isReachable = await verifyStreamPlaylistReachable(cleanSourceUrl, 'https://megavid.buzz/');
      if (!isReachable) {
        // Try next candidate (for example noax=1 when axsv CDN host fails DNS)
        continue;
      }

      const sources: ResolvedSource[] = [
        {
          url: cleanSourceUrl,
          quality: `${candidate.label} · Auto HLS`,
          isM3U8: data.type === 'hls' || cleanSourceUrl.includes('.m3u8'),
          type: 'hls',
        },
      ];

      // Parse variant qualities from master playlist so quality selector has real 720p / 480p options
      try {
        const masterRes = await fetchWithTimeout(
          cleanSourceUrl,
          { headers: { Referer: 'https://megavid.buzz/', 'User-Agent': BROWSER_UA } },
          4500
        );
        if (masterRes.ok) {
          const playlistText = await masterRes.text();
          const lines = playlistText.split(/\r?\n/);
          for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            if (line.startsWith('#EXT-X-STREAM-INF:')) {
              const resMatch = line.match(/RESOLUTION=\d+x(\d+)/i);
              const nextLine = (lines[i + 1] || '').trim();
              if (nextLine && !nextLine.startsWith('#')) {
                const variantUrl = new URL(nextLine, cleanSourceUrl).toString();
                sources.push({
                  url: variantUrl,
                  quality: resMatch ? `${resMatch[1]}p` : 'HD',
                  isM3U8: true,
                  type: 'hls',
                });
              }
            }
          }
        }
      } catch {
        // keep Auto HLS source if variant parsing fails
      }

      const subtitles: ResolvedSubtitle[] = (data.tracks || [])
        .filter((t) => typeof t.file === 'string' && t.file.trim())
        .map((t) => {
          const trackUrl = normalizeUrl(t.file!);
          const label = t.label?.trim() || 'Subtitles';
          const needsProxy =
            trackUrl.includes('megavid.buzz/sub/') ||
            trackUrl.toLowerCase().endsWith('.srt') ||
            !trackUrl.includes('cp.megavid.buzz');
          return {
            url: needsProxy
              ? `/api/anime/proxy?url=${encodeURIComponent(trackUrl)}&referer=${encodeURIComponent(
                  'https://megavid.buzz/'
                )}`
              : trackUrl,
            lang: label,
            label,
          };
        });

      const introChapter = (data.chapters || []).find((c) =>
        c.title?.toLowerCase().includes('intro')
      );
      const outroChapter = (data.chapters || []).find((c) =>
        c.title?.toLowerCase().includes('outro')
      );

      return {
        ok: true,
        payload: {
          sources,
          subtitles,
          servers: [
            { name: 'animeworld-india' },
            { name: 'vidstreaming' },
            { name: 'megacloud' },
            { name: 'streamsb' },
          ],
          headers: { Referer: 'https://megavid.buzz/' },
          hasHindiAudio: false,
          audioLanguages: [catToUse === 'dub' ? 'English' : 'Japanese'],
          intro:
            typeof introChapter?.start === 'number' && typeof introChapter?.end === 'number'
              ? { start: introChapter.start, end: introChapter.end }
              : undefined,
          outro:
            typeof outroChapter?.start === 'number' && typeof outroChapter?.end === 'number'
              ? { start: outroChapter.start, end: outroChapter.end }
              : undefined,
          providerMeta: {
            source: 'megavid',
            activeServer: requestedServer || 'vidstreaming',
            category,
          },
        },
      };
    } catch (err) {
      lastMessage = err instanceof Error ? err.message : 'Failed to reach streaming resolver.';
    }
  }

  return {
    ok: false,
    statusCode: lastStatus,
    message: lastMessage,
  };
}

async function createApp() {
  const app = express();
  const jsonParser = express.json({ limit: '8mb' });

  app.get('/api/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
  });

  // Real website visit tracking (session-deduplicated so route navigation does not inflate visits)
  app.post('/api/analytics/visit', jsonParser, (req: Request, res: Response) => {
    const body = (req.body || {}) as Record<string, unknown>;
    const sessionId = sanitizeIdToken(body.sessionId, 128);
    const clientId = sanitizeIdToken(body.clientId, 128);
    const uid = sanitizeIdToken(body.uid, 128) || undefined;

    if (!sessionId || !clientId) {
      res.status(400).json({ error: 'Missing valid sessionId or clientId.' });
      return;
    }

    const now = Date.now();
    const today = getTodayDateKey();
    const bucket = getOrCreateDailyBucket(today);

    let countedNewVisit = false;
    if (!seenVisitSessions.has(sessionId)) {
      seenVisitSessions.set(sessionId, now);
      analyticsStore.totalWebsiteVisits += 1;
      bucket.visits += 1;
      countedNewVisit = true;
    }

    recordActorActivity(clientId, uid);
    activePresenceSessions.set(sessionId, { sessionId, clientId, uid, lastSeen: now });
    scheduleAnalyticsPersist();

    res.json({ ok: true, countedNewVisit });
  });

  // Real heartbeat presence tracking
  app.post('/api/analytics/heartbeat', jsonParser, (req: Request, res: Response) => {
    const body = (req.body || {}) as Record<string, unknown>;
    const sessionId = sanitizeIdToken(body.sessionId, 128);
    const clientId = sanitizeIdToken(body.clientId, 128);
    const uid = sanitizeIdToken(body.uid, 128) || undefined;

    if (!sessionId || !clientId) {
      res.status(400).json({ error: 'Missing valid sessionId or clientId.' });
      return;
    }

    const now = Date.now();
    activePresenceSessions.set(sessionId, { sessionId, clientId, uid, lastSeen: now });
    recordActorActivity(clientId, uid);
    scheduleAnalyticsPersist();

    res.json({ ok: true, serverTime: now });
  });

  // Immediate offline signal on tab close / pagehide
  app.post('/api/analytics/offline', jsonParser, (req: Request, res: Response) => {
    const body = (req.body || {}) as Record<string, unknown>;
    const sessionId = sanitizeIdToken(body.sessionId, 128);
    if (sessionId) {
      activePresenceSessions.delete(sessionId);
    }
    res.status(204).end();
  });

  // Protected Admin User & Visit Analytics endpoint
  app.get('/api/admin/stats', async (req: Request, res: Response) => {
    const authCheck = await verifyAdminRequest(req);
    if (!authCheck.ok) {
      res.status(authCheck.status).json({ error: authCheck.error });
      return;
    }

    const activeList = pruneStalePresence();
    const onlineActors = new Set<string>();
    const onlineRegisteredUids = new Set<string>();
    const onlineGuestClients = new Set<string>();

    for (const s of activeList) {
      if (s.uid) {
        onlineActors.add(`u:${s.uid}`);
        onlineRegisteredUids.add(s.uid);
      } else {
        onlineActors.add(`c:${s.clientId}`);
        onlineGuestClients.add(s.clientId);
      }
    }

    const today = getTodayDateKey();
    const todayBucket = getOrCreateDailyBucket(today);

    // Last 7 days real activity series (oldest -> newest)
    const dailySeries: Array<{
      date: string;
      visits: number;
      activeUsers: number;
      registeredActive: number;
    }> = [];
    for (let offset = 6; offset >= 0; offset -= 1) {
      const d = new Date(Date.now() - offset * 86400_000).toISOString().slice(0, 10);
      const bucket = analyticsStore.daily[d];
      const uniqueToday = bucket
        ? new Set([
            ...(bucket.activeUserIds || []).map((id) => `u:${id}`),
            ...(bucket.guestClientIds || []).map((id) => `c:${id}`),
          ]).size
        : 0;
      dailySeries.push({
        date: d,
        visits: bucket?.visits || 0,
        activeUsers: uniqueToday,
        registeredActive: bucket?.activeUserIds?.length || 0,
      });
    }

    const todayCombinedActors = new Set([
      ...(todayBucket.activeUserIds || []).map((id) => `u:${id}`),
      ...(todayBucket.guestClientIds || []).map((id) => `c:${id}`),
    ]).size;

    res.json({
      currentlyOnlineUsers: onlineActors.size,
      onlineRegisteredUsers: onlineRegisteredUids.size,
      onlineGuestUsers: onlineGuestClients.size,
      usersActiveToday: todayCombinedActors,
      registeredActiveToday: todayBucket.activeUserIds.length,
      guestActiveToday: todayBucket.guestClientIds.length,
      totalWebsiteVisits: analyticsStore.totalWebsiteVisits,
      visitsToday: todayBucket.visits,
      totalUniqueVisitors: knownClientsSet.size,
      dailySeries,
      updatedAt: new Date().toISOString(),
    });
  });

  // Protected Admin Banner Image Upload (validated magic bytes + size <= 5 MB)
  app.post('/api/admin/banners/upload', jsonParser, async (req: Request, res: Response) => {
    const authCheck = await verifyAdminRequest(req);
    if (!authCheck.ok) {
      res.status(authCheck.status).json({ error: authCheck.error });
      return;
    }

    const body = (req.body || {}) as Record<string, unknown>;
    const rawData = typeof body.dataBase64 === 'string' ? body.dataBase64.trim() : '';
    const declaredMime = typeof body.mimeType === 'string' ? body.mimeType.trim().toLowerCase() : '';

    if (!rawData) {
      res.status(400).json({ error: 'Missing image data.' });
      return;
    }

    const allowedMimes = new Set(['image/jpeg', 'image/jpg', 'image/png', 'image/webp', 'image/gif']);
    if (declaredMime && !allowedMimes.has(declaredMime)) {
      res.status(400).json({ error: 'Unsupported image format. Allowed: JPG, PNG, WebP, GIF.' });
      return;
    }

    const cleanedBase64 = rawData.replace(/^data:image\/[a-z0-9.+-]+;base64,/i, '');
    let buffer: Buffer;
    try {
      buffer = Buffer.from(cleanedBase64, 'base64');
    } catch {
      res.status(400).json({ error: 'Invalid base64 image payload.' });
      return;
    }

    if (buffer.length === 0 || buffer.length > 5 * 1024 * 1024) {
      res.status(400).json({ error: 'Banner image must be between 1 byte and 5 MB.' });
      return;
    }

    const verified = detectVerifiedImageFormat(buffer);
    if (!verified) {
      res.status(400).json({ error: 'Invalid or corrupted image binary. Allowed: JPG, PNG, WebP, GIF.' });
      return;
    }

    const safeFileName = `banner_${Date.now()}_${crypto.randomBytes(6).toString('hex')}${verified.ext}`;
    const destPath = path.join(BANNER_UPLOADS_DIR, safeFileName);
    fs.writeFileSync(destPath, buffer);

    res.json({
      imageUrl: `/api/banners/image/${safeFileName}`,
      mimeType: verified.mime,
      size: buffer.length,
    });
  });

  // Publicly serve validated banner image uploads safely
  app.get('/api/banners/image/:filename', (req: Request, res: Response) => {
    const filename = String(req.params.filename || '');
    if (!/^[a-zA-Z0-9_\-]+\.(jpg|jpeg|png|webp|gif)$/i.test(filename)) {
      res.status(400).json({ error: 'Invalid banner filename.' });
      return;
    }

    const targetPath = path.join(BANNER_UPLOADS_DIR, filename);
    if (!fs.existsSync(targetPath)) {
      res.status(404).json({ error: 'Banner image not found.' });
      return;
    }

    const ext = path.extname(filename).toLowerCase();
    const mimeMap: Record<string, string> = {
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg',
      '.png': 'image/png',
      '.webp': 'image/webp',
      '.gif': 'image/gif',
    };

    res.setHeader('Content-Type', mimeMap[ext] || 'application/octet-stream');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    fs.createReadStream(targetPath).pipe(res);
  });

  // Protected Admin cleanup for local uploaded banner images
  app.delete('/api/admin/banners/image/:filename', async (req: Request, res: Response) => {
    const authCheck = await verifyAdminRequest(req);
    if (!authCheck.ok) {
      res.status(authCheck.status).json({ error: authCheck.error });
      return;
    }

    const filename = String(req.params.filename || '');
    if (!/^[a-zA-Z0-9_\-]+\.(jpg|jpeg|png|webp|gif)$/i.test(filename)) {
      res.status(400).json({ error: 'Invalid banner filename.' });
      return;
    }

    const targetPath = path.join(BANNER_UPLOADS_DIR, filename);
    if (fs.existsSync(targetPath)) {
      fs.unlinkSync(targetPath);
    }
    res.json({ ok: true });
  });

  app.get('/api/extensions/repos', async (_req: Request, res: Response) => {
    try {
      const catalog = await fetchExtensionsCatalog();
      res.json(catalog);
    } catch (err) {
      res.status(502).json({
        repositories: [],
        extensions: [],
        error: err instanceof Error ? err.message : 'Failed to load extension repositories.',
      });
    }
  });

  // Actual AnimeWorld India PHP API v1 endpoints
  app.get('/api/anime-world-india/v1/home.php', async (_req: Request, res: Response) => {
    try {
      const { ok, status, html } = await fetchAnimeWorldHtml(`${ANIMEWORLD_UPSTREAM}/`);
      if (!ok || !html) {
        res.status(status || 502).json({ success: false, error: `AnimeWorld India upstream returned HTTP ${status}` });
        return;
      }
      const $ = cheerio.load(html);
      const allCards = parseAnimeWorldCards($);
      const latest_series = allCards.filter((item) => item.type === 'series');
      const latest_movies = allCards.filter((item) => item.type === 'movie');
      res.json({
        success: true,
        source: ANIMEWORLD_UPSTREAM,
        latest_series,
        latest_movies,
      });
    } catch (err) {
      res.status(502).json({
        success: false,
        error: err instanceof Error ? err.message : 'Failed to load AnimeWorld India homepage.',
      });
    }
  });

  app.get('/api/anime-world-india/v1/series.php', async (req: Request, res: Response) => {
    const page = Math.max(1, Number(req.query.p || 1));
    const targetUrl =
      page > 1 ? `${ANIMEWORLD_UPSTREAM}/series/page/${page}/` : `${ANIMEWORLD_UPSTREAM}/series/`;
    try {
      const { ok, status, html } = await fetchAnimeWorldHtml(targetUrl);
      if (!ok || !html) {
        res.status(status || 404).json({ success: false, error: `AnimeWorld India series returned HTTP ${status}` });
        return;
      }
      const $ = cheerio.load(html);
      const series = parseAnimeWorldCards($).filter((x) => x.seriesId);
      const pages: number[] = [];
      $('nav.pagination a.page-numbers, nav.pagination a.page-link, .pagination a').each((_, el) => {
        const num = Number($(el).text().trim());
        if (Number.isFinite(num) && num > 0) pages.push(num);
      });
      const totalPages = pages.length > 0 ? Math.max(...pages, page) : page;
      res.json({
        success: true,
        source: `${ANIMEWORLD_UPSTREAM}/series`,
        current_page: page,
        total_pages: totalPages,
        has_next: page < totalPages || $('a.next').length > 0,
        has_prev: page > 1,
        pages: Array.from(new Set(pages)).sort((a, b) => a - b),
        total_results: series.length,
        series,
      });
    } catch (err) {
      res.status(502).json({
        success: false,
        error: err instanceof Error ? err.message : 'Failed to fetch series list.',
      });
    }
  });

  app.get('/api/anime-world-india/v1/search.php', async (req: Request, res: Response) => {
    const query = typeof req.query.query === 'string' ? req.query.query.trim() : '';
    const page = Math.max(1, Number(req.query.p || 1));
    if (!query) {
      res.status(400).json({ success: false, error: 'Missing query parameter' });
      return;
    }
    const targetUrl =
      page > 1
        ? `${ANIMEWORLD_UPSTREAM}/page/${page}/?s=${encodeURIComponent(query)}`
        : `${ANIMEWORLD_UPSTREAM}/?s=${encodeURIComponent(query)}`;
    try {
      const { ok, status, html } = await fetchAnimeWorldHtml(targetUrl);
      if (!ok || !html) {
        res.status(status || 404).json({ success: false, error: `Search returned HTTP ${status}` });
        return;
      }
      const $ = cheerio.load(html);
      const results = parseAnimeWorldCards($);
      res.json({
        success: true,
        query,
        currentPage: page,
        source: `${ANIMEWORLD_UPSTREAM}/search`,
        results,
      });
    } catch (err) {
      res.status(502).json({
        success: false,
        error: err instanceof Error ? err.message : 'Failed to search AnimeWorld India.',
      });
    }
  });

  app.get('/api/anime-world-india/v1/seasons.php', async (req: Request, res: Response) => {
    const seriesID =
      typeof req.query.seriesID === 'string'
        ? req.query.seriesID.trim()
        : typeof req.query.seriesId === 'string'
          ? req.query.seriesId.trim()
          : '';
    if (!seriesID) {
      res.status(400).json({ success: false, error: 'Missing seriesID parameter' });
      return;
    }
    const targetUrl = `${ANIMEWORLD_UPSTREAM}/series/${encodeURIComponent(seriesID)}/`;
    try {
      const { ok, status, html } = await fetchAnimeWorldHtml(targetUrl);
      if (!ok || !html) {
        res.status(status || 404).json({
          success: false,
          error: `AnimeWorld India series "${seriesID}" returned HTTP ${status}`,
        });
        return;
      }
      const $ = cheerio.load(html);
      const title = $('h1.hero-title, h1.entry-title').first().text().trim() || seriesID;
      const poster = normalizeAnimeWorldImg(
        $('.hero-poster img, .post-thumbnail img').first().attr('data-src') ||
          $('.hero-poster img, .post-thumbnail img').first().attr('src')
      );
      const description =
        $('#overview-text p, .description p').first().text().trim() || null;

      const heroBadges: string[] = [];
      $('.hero-badge').each((_, el) => {
        const txt = $(el).text().trim();
        if (txt) heroBadges.push(txt);
      });
      const year = heroBadges.find((b) => /^\d{4}$/.test(b)) || null;
      const languages: string[] = [];
      $('a.language-tag').each((_, el) => {
        const lang = $(el).text().trim();
        if (lang && !languages.includes(lang)) languages.push(lang);
      });

      const seasons: Array<Record<string, unknown>> = [];
      $('a.season-btn.sel-temp').each((_, el) => {
        const btn = $(el);
        const postId = btn.attr('data-post') || '';
        const seasonNum = btn.attr('data-season') || '1';
        const seasonName = btn.find('.season-label').text().trim() || `Season ${seasonNum}`;
        const episodesText = btn.find('.season-episodes').text().trim() || '';
        const isSubOnly = btn.hasClass('non-regional');
        const seasonId = postId
          ? `${seriesID}__post-${postId}__season-${seasonNum}`
          : `${seriesID}-season-${seasonNum}`;
        seasons.push({
          seasonNumber: seasonNum,
          seasonName,
          episodes: episodesText,
          seasonId,
          postId,
          isSubOnly,
        });
      });

      // Fallback if single-season series has no season selector buttons
      if (seasons.length === 0) {
        const epCount = $('article.tr-episode-card').length;
        seasons.push({
          seasonNumber: '1',
          seasonName: 'Season 1',
          episodes: `${epCount} Episodes`,
          seasonId: `${seriesID}__season-1`,
          isSubOnly: false,
        });
      }

      res.json({
        success: true,
        source: `${ANIMEWORLD_UPSTREAM}/series`,
        series: {
          seriesId: seriesID,
          title,
          poster,
          year,
          totalSeasons: String(seasons.length),
          languages,
          description,
        },
        seasons,
      });
    } catch (err) {
      res.status(502).json({
        success: false,
        error: err instanceof Error ? err.message : 'Failed to load seasons.',
      });
    }
  });

  app.get('/api/anime-world-india/v1/episodes.php', async (req: Request, res: Response) => {
    const seasonId = typeof req.query.seasonId === 'string' ? req.query.seasonId.trim() : '';
    if (!seasonId) {
      res.status(400).json({ success: false, error: 'Missing seasonId parameter' });
      return;
    }

    const parseEpisodesFromHtml = (html: string) => {
      const $ = cheerio.load(html);
      const episodes: Array<Record<string, unknown>> = [];
      const seen = new Set<string>();
      $('article.tr-episode-card').each((_, el) => {
        const card = $(el);
        const href = card.find('a.tr-episode-link').first().attr('href') || '';
        const episodeId = extractSlugFromHref(href, 'episode');
        if (!episodeId || seen.has(episodeId)) return;
        seen.add(episodeId);

        const epTitle = card.find('.tr-episode-title, h3, h2').first().text().trim() || episodeId;
        const epNumber = card.find('.tr-episode-number').first().text().trim() || '';
        const imgEl = card.find('img').first();
        const image = normalizeAnimeWorldImg(imgEl.attr('data-src') || imgEl.attr('src'));
        episodes.push({
          episodeId,
          title: epTitle,
          episodeNumber: epNumber,
          airDate: null,
          image,
          overview: null,
        });
      });
      return { $, episodes };
    };

    try {
      const encodedMatch = seasonId.match(/^(.+?)__post-(\d+)__season-(\d+)$/);
      if (encodedMatch) {
        const [, seriesSlug, postId, seasonNum] = encodedMatch;
        const ajaxUrl = `${ANIMEWORLD_UPSTREAM}/wp-admin/admin-ajax.php?action=action_select_season&season=${encodeURIComponent(
          seasonNum
        )}&post=${encodeURIComponent(postId)}`;
        const { ok, status, html } = await fetchAnimeWorldHtml(ajaxUrl);
        if (!ok || !html) {
          res.status(status || 404).json({
            success: false,
            error: `AnimeWorld India episodes endpoint returned HTTP ${status}`,
          });
          return;
        }
        const { episodes } = parseEpisodesFromHtml(html);
        res.json({
          success: true,
          source: `${ANIMEWORLD_UPSTREAM}/season`,
          season: {
            seasonId,
            animeTitle: seriesSlug,
            seasonName: `Season ${seasonNum}`,
            totalEpisodes: String(episodes.length),
          },
          episodes,
        });
        return;
      }

      // Support slug-style seasonId like "<seriesSlug>__season-<N>" or "<seriesSlug>-season-<N>"
      const slugMatch = seasonId.match(/^(.+?)(?:__|-)season-(\d+)$/i);
      const seriesSlug = slugMatch ? slugMatch[1] : seasonId;
      const targetSeasonNum = slugMatch ? slugMatch[2] : '1';
      const seriesUrl = `${ANIMEWORLD_UPSTREAM}/series/${encodeURIComponent(seriesSlug)}/`;
      const seriesPage = await fetchAnimeWorldHtml(seriesUrl);
      if (!seriesPage.ok || !seriesPage.html) {
        res.status(seriesPage.status || 404).json({
          success: false,
          error: `AnimeWorld India season "${seasonId}" returned HTTP ${seriesPage.status}`,
        });
        return;
      }

      const $series = cheerio.load(seriesPage.html);
      const animeTitle = $series('h1.hero-title, h1.entry-title').first().text().trim() || seriesSlug;
      const matchingBtn = $series(`a.season-btn.sel-temp[data-season="${targetSeasonNum}"]`).first();
      const postId = matchingBtn.attr('data-post');

      let episodes: Array<Record<string, unknown>> = [];
      if (postId && targetSeasonNum !== '1') {
        const ajaxUrl = `${ANIMEWORLD_UPSTREAM}/wp-admin/admin-ajax.php?action=action_select_season&season=${encodeURIComponent(
          targetSeasonNum
        )}&post=${encodeURIComponent(postId)}`;
        const ajaxRes = await fetchAnimeWorldHtml(ajaxUrl);
        if (ajaxRes.ok && ajaxRes.html) {
          episodes = parseEpisodesFromHtml(ajaxRes.html).episodes;
        }
      } else {
        episodes = parseEpisodesFromHtml(seriesPage.html).episodes;
      }

      res.json({
        success: true,
        source: `${ANIMEWORLD_UPSTREAM}/season`,
        season: {
          seasonId,
          animeTitle,
          seasonName: `Season ${targetSeasonNum}`,
          totalEpisodes: String(episodes.length),
        },
        episodes,
      });
    } catch (err) {
      res.status(502).json({
        success: false,
        error: err instanceof Error ? err.message : 'Failed to fetch episodes.',
      });
    }
  });

  app.get('/api/anime-world-india/v1/stream.php', async (req: Request, res: Response) => {
    const episodeId = typeof req.query.episodeId === 'string' ? req.query.episodeId.trim() : '';
    const movieId = typeof req.query.movieId === 'string' ? req.query.movieId.trim() : '';

    if (!episodeId && !movieId) {
      res.status(400).json({ success: false, error: 'Missing episodeId or movieId' });
      return;
    }

    const type = episodeId ? 'episode' : 'movie';
    const targetUrl = episodeId
      ? `${ANIMEWORLD_UPSTREAM}/episode/${encodeURIComponent(episodeId)}/`
      : `${ANIMEWORLD_UPSTREAM}/movies/${encodeURIComponent(movieId)}/`;

    try {
      const { ok, status, html } = await fetchAnimeWorldHtml(targetUrl);
      if (!ok || !html) {
        res.status(status === 404 ? 404 : status || 502).json({
          success: false,
          error: `AnimeWorld India API returned HTTP ${status || 404}`,
        });
        return;
      }

      const $ = cheerio.load(html);
      const iframeSrcs: string[] = [];
      $('#aa-options iframe, .video-player iframe, .video-embed iframe').each((_, el) => {
        const s = ($(el).attr('src') || $(el).attr('data-src') || '').trim();
        if (s && s.startsWith('http') && !iframeSrcs.includes(s)) {
          iframeSrcs.push(s);
        }
      });

      const downloadLink =
        $('.archive-link-wrap a, a[download], a.btn-download').first().attr('href') || null;

      const streams: Array<{
        language: string;
        url: string;
        type: 'hls' | 'mp4' | 'embed';
        label: string;
      }> = [];
      const audioLanguages: string[] = [];
      let hindiStreamLink: string | null = null;
      let hasHindiAudio = false;

      // 1. Parse multi-lang-plyr.php?data=<base64 JSON> (explicit per-language streams!)
      for (const src of iframeSrcs) {
        if (src.includes('multi-lang-plyr.php') && src.includes('data=')) {
          try {
            const parsedUrl = new URL(src);
            const rawData = parsedUrl.searchParams.get('data');
            if (rawData) {
              const entries = JSON.parse(Buffer.from(rawData, 'base64').toString('utf8')) as Array<{
                language?: string;
                link?: string;
              }>;
              for (const entry of entries) {
                const lang = (entry.language || '').trim();
                const link = (entry.link || '').trim();
                if (!lang || !link) continue;
                if (!audioLanguages.includes(lang)) audioLanguages.push(lang);
                const lowerLang = lang.toLowerCase();
                if (lowerLang === 'hindi' || lowerLang === 'hin') {
                  hasHindiAudio = true;
                  if (!hindiStreamLink) hindiStreamLink = link;
                }
                streams.push({
                  language: lang,
                  url: link,
                  type: classifyAnimeWorldStreamType(link),
                  label: `AnimeWorld India · ${lang} Audio`,
                });
              }
            }
          } catch {
            // ignore malformed base64 payload
          }
        }
      }

      // 2. Inspect non-multi-lang iframes (ravok.buzz multi-audio or megaplay sub/dub)
      let ravokUrl: string | null = null;
      for (const src of iframeSrcs) {
        if (src.includes('multi-lang-plyr.php')) continue;
        if (src.includes('ravok.buzz')) {
          ravokUrl = src;
          const ravokLangs = await verifyRavokAudioLanguages(src);
          for (const l of ravokLangs) {
            if (!audioLanguages.includes(l)) audioLanguages.push(l);
          }
          const ravokHasHindi = ravokLangs.some((l) => l.toLowerCase() === 'hindi');
          if (ravokHasHindi) {
            hasHindiAudio = true;
          }
          streams.push({
            language: ravokHasHindi ? 'Hindi' : ravokLangs[0] || 'Multi-Audio',
            url: src,
            type: 'embed',
            label: ravokHasHindi
              ? 'AnimeWorld India · Multi-Audio (Hindi Default)'
              : 'AnimeWorld India · PlayX Mirror',
          });
        } else if (src.includes('megaplay.buzz')) {
          const isDub = src.toLowerCase().includes('/dub');
          const lang = isDub ? 'English' : 'Japanese';
          if (!audioLanguages.includes(lang)) audioLanguages.push(lang);
          streams.push({
            language: lang,
            url: src,
            type: 'embed',
            label: isDub ? 'AnimeWorld India · English Dub' : 'AnimeWorld India · Japanese Sub',
          });
        } else {
          streams.push({
            language: 'Unknown',
            url: src,
            type: classifyAnimeWorldStreamType(src),
            label: 'AnimeWorld India · Mirror',
          });
        }
      }

      // Order verified Hindi stream FIRST so it never defaults to English/Japanese when Hindi exists
      streams.sort((a, b) => {
        const aHindi = a.language.toLowerCase() === 'hindi' ? 0 : 1;
        const bHindi = b.language.toLowerCase() === 'hindi' ? 0 : 1;
        return aHindi - bHindi;
      });

      const primaryLink = hindiStreamLink || (hasHindiAudio && ravokUrl) || streams[0]?.url || downloadLink || null;
      if (!primaryLink) {
        res.status(404).json({
          success: false,
          error: 'AnimeWorld India returned no playable stream link for this item.',
        });
        return;
      }

      const title =
        $('h1.hero-title, h1.episode-title, h1.entry-title').first().text().trim() ||
        episodeId ||
        movieId;

      res.json({
        success: true,
        type,
        source: targetUrl,
        title,
        hasHindiAudio,
        audioLanguages,
        stream: {
          streamLink: primaryLink,
          file: downloadLink,
          type: classifyAnimeWorldStreamType(primaryLink),
        },
        streams,
      });
    } catch (err) {
      res.status(502).json({
        success: false,
        error: err instanceof Error ? err.message : 'Failed to load AnimeWorld India stream.',
      });
    }
  });

  app.get('/api/anime/anizip/:anilistId', async (req: Request, res: Response) => {
    const anilistId = Number(req.params.anilistId);
    if (!Number.isFinite(anilistId) || anilistId <= 0) {
      res.status(400).json({ found: false, error: 'Invalid AniList ID.' });
      return;
    }
    const data = await fetchAniZipMetadata(anilistId);
    res.json(data);
  });

  app.get('/api/anime/skip/:malId/:episodeNumber', async (req: Request, res: Response) => {
    const malId = Number(req.params.malId);
    const episodeNumber = Number(req.params.episodeNumber);
    const episodeLength = Number(req.query.length || 0);
    if (!Number.isFinite(malId) || malId <= 0 || !Number.isFinite(episodeNumber) || episodeNumber <= 0) {
      res.status(400).json({ found: false, skipTimes: [] });
      return;
    }
    const data = await fetchAniSkipTimestamps(malId, episodeNumber, episodeLength);
    res.json(data);
  });

  app.get('/api/anime/info/:id', async (req: Request, res: Response) => {
    const anilistId = req.params.id;
    const base =
      typeof req.query.base === 'string' && req.query.base.startsWith('http')
        ? req.query.base.replace(/\/+$/, '')
        : DEFAULT_STREAMING_API;
    const dub = req.query.dub === 'true' ? '?dub=true' : '';
    const upstreamUrl = `${base}/meta/anilist/info/${encodeURIComponent(anilistId)}${dub}`;

    try {
      const upstream = await fetchWithTimeout(
        upstreamUrl,
        {
          headers: { Accept: 'application/json', 'User-Agent': BROWSER_UA },
        },
        5000
      );

      if (!upstream.ok) {
        const text = await upstream.text().catch(() => '');
        res.status(200).json({
          episodes: [],
          upstreamError: {
            statusCode: upstream.status,
            endpointUrl: upstreamUrl,
            message:
              upstream.status === 402
                ? 'Configured external streaming API returned HTTP 402 (Deployment Disabled).'
                : `External streaming info API returned HTTP ${upstream.status}.`,
            detail: text.slice(0, 200),
          },
        });
        return;
      }

      const json = await upstream.json();
      res.json(json);
    } catch (err) {
      res.status(200).json({
        episodes: [],
        upstreamError: {
          endpointUrl: upstreamUrl,
          message: err instanceof Error ? err.message : 'Upstream info request failed.',
        },
      });
    }
  });

  app.get('/api/anime/servers/:episodeId', async (req: Request, res: Response) => {
    const episodeId = req.params.episodeId;
    const base =
      typeof req.query.base === 'string' && req.query.base.startsWith('http')
        ? req.query.base.replace(/\/+$/, '')
        : DEFAULT_STREAMING_API;
    const upstreamUrl = `${base}/meta/anilist/servers/${encodeURIComponent(episodeId)}`;

    try {
      const upstream = await fetchWithTimeout(
        upstreamUrl,
        { headers: { Accept: 'application/json', 'User-Agent': BROWSER_UA } },
        4000
      );
      if (upstream.ok) {
        const data = await upstream.json();
        if (Array.isArray(data) && data.length > 0) {
          res.json(data);
          return;
        }
      }
    } catch {
      // fall through to standard provider list
    }

    res.json([
      { name: 'animeworld-india' },
      { name: 'vidstreaming' },
      { name: 'megacloud' },
      { name: 'streamsb' },
    ]);
  });

  app.get('/api/anime/watch/:episodeId', async (req: Request, res: Response) => {
    const episodeId = (req.params.episodeId || '').trim();
    if (!episodeId || episodeId === 'EPISODE_ID') {
      res.status(400).json({
        message: 'A valid episodeId is required instead of the placeholder EPISODE_ID.',
      });
      return;
    }

    const base =
      typeof req.query.base === 'string' && req.query.base.startsWith('http')
        ? req.query.base.replace(/\/+$/, '')
        : DEFAULT_STREAMING_API;
    const server = typeof req.query.server === 'string' ? req.query.server.trim() : 'vidstreaming';
    const category: 'sub' | 'dub' | 'hindi' = req.query.category === 'hindi' ? 'hindi' : req.query.category === 'dub' ? 'dub' : 'sub';
    const epQuery = Number(req.query.ep || 0);
    const anilistId = Number(req.query.anilistId || 0);
    let malId = Number(req.query.malId || 0);

    const serverLower = server.toLowerCase();
    if (serverLower === 'animeworld-india') {
      res.status(400).json({
        message:
          'AnimeWorld-India uses /api/anime-world-india/v1/ (search.php, seasons.php, episodes.php, stream.php) with native AnimeWorld IDs rather than AniList IDs.',
      });
      return;
    }

    // 1. Try primary configured Consumet-compatible endpoint first
    const params = new URLSearchParams();
    if (server) params.set('server', server);
    params.set('category', category);
    const primaryUrl = `${base}/meta/anilist/watch/${encodeURIComponent(episodeId)}?${params.toString()}`;

    let primaryErrorMsg = '';
    let primaryStatus = 0;

    try {
      const upstream = await fetchWithTimeout(
        primaryUrl,
        {
          headers: {
            Accept: 'application/json',
            'User-Agent': BROWSER_UA,
          },
        },
        5500
      );

      if (upstream.ok) {
        const data = (await upstream.json()) as Record<string, unknown>;
        const sources = Array.isArray(data.sources) ? data.sources : [];
        if (sources.length > 0) {
          res.json(data);
          return;
        }
        primaryStatus = 404;
        primaryErrorMsg = 'Primary streaming API returned no playable sources.';
      } else {
        primaryStatus = upstream.status;
        const body = await upstream.text().catch(() => '');
        primaryErrorMsg =
          upstream.status === 402
            ? `Primary endpoint (${base}) returned HTTP 402 (DEPLOYMENT_DISABLED).`
            : `Primary endpoint returned HTTP ${upstream.status}: ${body.slice(0, 120)}`;
      }
    } catch (err) {
      primaryErrorMsg =
        err instanceof Error ? err.message : 'Primary streaming endpoint unreachable.';
    }

    // 2. Fallback to verified MAL-indexed stream resolver when primary Consumet endpoint is disabled/unavailable
    if (!malId && anilistId > 0) {
      const resolved = await resolveMalIdFromAnilist(anilistId);
      if (resolved) {
        malId = resolved;
      } else {
        const aniZip = await fetchAniZipMetadata(anilistId);
        const zipMal = (aniZip.mappings as { malId?: number | null } | undefined)?.malId;
        if (typeof zipMal === 'number' && zipMal > 0) {
          malId = zipMal;
        }
      }
    }

    const epMatch = episodeId.match(/(?:episode|ep)[-_]?(\d+)/i);
    const episodeNumber = epQuery > 0 ? epQuery : epMatch ? Number(epMatch[1]) : 1;

    if (malId > 0 && episodeNumber > 0) {
      const [fallback, aniSkip] = await Promise.all([
        resolveFromMegavid(malId, episodeNumber, category, server),
        fetchAniSkipTimestamps(malId, episodeNumber),
      ]);

      if (fallback.ok && fallback.payload) {
        const payload = { ...fallback.payload } as Record<string, unknown>;
        if (!payload.intro && aniSkip.intro) payload.intro = aniSkip.intro;
        if (!payload.outro && aniSkip.outro) payload.outro = aniSkip.outro;
        if (!payload.recap && aniSkip.recap) payload.recap = aniSkip.recap;

        res.json({
          ...payload,
          malId,
          primaryEndpointNotice: primaryErrorMsg || undefined,
        });
        return;
      }

      res.status(fallback.statusCode || 404).json({
        message: fallback.message || primaryErrorMsg || 'Episode stream unavailable.',
        endpointUrl: primaryUrl,
        primaryStatus,
      });
      return;
    }

    res.status(primaryStatus || 502).json({
      message:
        primaryErrorMsg ||
        'Unable to resolve episode stream. Missing MyAnimeList ID for fallback resolver.',
      endpointUrl: primaryUrl,
      primaryStatus,
    });
  });

  app.options('/api/anime/proxy', (_req: Request, res: Response) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Range, If-Range, Content-Type');
    res.setHeader(
      'Access-Control-Expose-Headers',
      'Content-Length, Content-Range, Accept-Ranges, Content-Type'
    );
    res.status(204).end();
  });

  app.get('/api/anime/proxy', async (req: Request, res: Response) => {
    const rawUrl = typeof req.query.url === 'string' ? req.query.url : '';
    const referer =
      typeof req.query.referer === 'string' && req.query.referer.startsWith('http')
        ? req.query.referer
        : 'https://megavid.buzz/';

    if (!rawUrl) {
      res.status(400).json({ message: 'Missing url parameter.' });
      return;
    }

    let parsedUrl: URL;
    try {
      parsedUrl = new URL(normalizeUrl(rawUrl));
    } catch {
      res.status(400).json({ message: 'Invalid url parameter.' });
      return;
    }

    if (!['http:', 'https:'].includes(parsedUrl.protocol) || isBlockedPrivateHost(parsedUrl.hostname)) {
      res.status(403).json({ message: 'Blocked upstream host.' });
      return;
    }

    const upstreamHeaders: Record<string, string> = {
      Referer: referer,
      'User-Agent': BROWSER_UA,
      Accept: '*/*',
    };
    if (typeof req.headers.range === 'string' && req.headers.range.trim()) {
      upstreamHeaders.Range = req.headers.range;
    }
    if (typeof req.headers['if-range'] === 'string' && req.headers['if-range'].trim()) {
      upstreamHeaders['If-Range'] = req.headers['if-range'];
    }

    try {
      const upstream = await fetchWithTimeout(
        parsedUrl.toString(),
        {
          headers: upstreamHeaders,
        },
        25000
      );

      if (!upstream.ok && upstream.status !== 206) {
        res.status(upstream.status).send(`Upstream proxy error: HTTP ${upstream.status}`);
        return;
      }

      const lowerPath = parsedUrl.pathname.toLowerCase();
      let contentType = upstream.headers.get('content-type') || 'application/octet-stream';
      if (lowerPath.endsWith('.ts') && (!contentType || contentType.includes('linguist') || contentType.includes('octet-stream'))) {
        contentType = 'video/mp2t';
      } else if (lowerPath.endsWith('.mp4') && contentType.includes('octet-stream')) {
        contentType = 'video/mp4';
      }

      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Range, If-Range, Content-Type');
      res.setHeader(
        'Access-Control-Expose-Headers',
        'Content-Length, Content-Range, Accept-Ranges, Content-Type'
      );

      if (lowerPath.endsWith('.srt') || contentType.includes('subrip')) {
        const srtText = await upstream.text();
        res.setHeader('Content-Type', 'text/vtt; charset=utf-8');
        res.send(convertSrtToVtt(srtText));
        return;
      }

      if (contentType.includes('mpegurl') || lowerPath.endsWith('.m3u8')) {
        const playlist = await upstream.text();
        const proxyHref = (target: string) => {
          const absolute = new URL(target, parsedUrl).toString();
          return `/api/anime/proxy?url=${encodeURIComponent(absolute)}&referer=${encodeURIComponent(
            referer
          )}`;
        };
        const rewritten = playlist
          .split(/\r?\n/)
          .map((line) => {
            const trimmed = line.trim();
            if (!trimmed) return line;
            if (trimmed.startsWith('#')) {
              return line.replace(/URI="([^"]+)"/gi, (_m, uri: string) => `URI="${proxyHref(uri)}"`);
            }
            return proxyHref(trimmed);
          })
          .join('\n');
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl; charset=utf-8');
        res.setHeader('Cache-Control', 'no-cache');
        res.send(rewritten);
        return;
      }

      res.status(upstream.status);
      res.setHeader('Content-Type', contentType);
      const acceptRanges = upstream.headers.get('accept-ranges') || 'bytes';
      res.setHeader('Accept-Ranges', acceptRanges);

      const passthroughHeaders = ['content-range', 'content-length', 'cache-control', 'etag', 'last-modified'];
      for (const h of passthroughHeaders) {
        const val = upstream.headers.get(h);
        if (val) res.setHeader(h, val);
      }

      if (upstream.body) {
        const nodeStream = Readable.fromWeb(upstream.body as Parameters<typeof Readable.fromWeb>[0]);
        nodeStream.on('error', () => {
          if (!res.headersSent) {
            res.status(502).end();
          } else {
            res.destroy();
          }
        });
        res.on('close', () => {
          if (!nodeStream.destroyed) nodeStream.destroy();
        });
        nodeStream.pipe(res);
      } else {
        const buffer = Buffer.from(await upstream.arrayBuffer());
        res.send(buffer);
      }
    } catch (err) {
      if (!res.headersSent) {
        res.status(502).send(err instanceof Error ? err.message : 'Proxy fetch failed.');
      }
    }
  });

  if (process.env.NODE_ENV !== 'production') {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.resolve(__dirname, 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req: Request, res: Response) => {
      const indexHtml = path.join(distPath, 'index.html');
      if (fs.existsSync(indexHtml)) {
        res.sendFile(indexHtml);
      } else {
        res.status(404).send('Build output not found.');
      }
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`KuroStream server listening on http://0.0.0.0:${PORT}`);
  });
}

createApp();
