import express from 'express';
import cors from 'cors';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs/promises';
import { existsSync } from 'fs';
import { fileURLToPath } from 'url';
import winston from 'winston';
import fetch from 'node-fetch';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Logger setup
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.colorize(),
    winston.format.simple()
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'app.log' })
  ]
});

// Express app setup
const app = express();
const server = createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function getYouTubeCookieHeader() {
  try {
    const cookiesPath = path.join(__dirname, 'cookies.txt');
    if (!existsSync(cookiesPath)) return null;
    const text = await fs.readFile(cookiesPath, 'utf8');
    const lines = text.split(/\r?\n/);
    const pairs = [];
    for (const line of lines) {
      if (!line || line.startsWith('#')) continue;
      const parts = line.split(/\t/);
      if (parts.length < 7) continue;
      const domain = parts[0];
      const name = parts[5];
      const value = parts[6];
      if (!domain || !/youtube\.com$/i.test(domain.replace(/^\./, '')) && !/\.youtube\.com$/i.test(domain)) continue;
      if (!name || !value) continue;
      pairs.push(`${name}=${value}`);
    }
    if (!pairs.length) return null;
    return pairs.join('; ');
  } catch {
    return null;
  }
}

function findCandidatesNearClipId(html, clipId) {
  if (!clipId) return [];
  const re = new RegExp(escapeRegex(clipId), 'g');
  const idxs = [];
  let m;
  while ((m = re.exec(html)) && idxs.length < 3) idxs.push(m.index);
  const out = [];
  for (const idx of idxs) {
    let l = idx;
    let attempts = 0;
    while (l > 0 && attempts < 3) {
      while (l > 0 && html[l] !== '{') l--;
      if (html[l] !== '{') break;
      const obj = extractBalancedJson(html, l);
      if (obj) {
        const jsonStr = JSON.stringify(obj);
        if (jsonStr.includes(clipId)) {
          out.push(...collectClipCandidates(obj));
          break;
        }
      }
      l = l - 1;
      attempts++;
    }
  }
  return out;
}

// Configuration
const PORT = process.env.PORT || 3000;
const DOWNLOADS_DIR = path.join(__dirname, 'downloads');
const PUBLIC_DIR = path.join(__dirname, 'public');
const YTDLP_PATH = path.join(__dirname, 'yt-dlp.exe');

// Ensure directories exist
async function ensureDirectories() {
  try {
    await fs.mkdir(DOWNLOADS_DIR, { recursive: true });
    await fs.mkdir(PUBLIC_DIR, { recursive: true });
    logger.info('✓ Directories initialized');
  } catch (error) {
    logger.error('Failed to create directories:', error);
  }
}

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(PUBLIC_DIR));

// Active downloads tracking
const activeDownloads = new Map();

// Socket.IO connection handling
io.on('connection', (socket) => {
  logger.info(`Client connected: ${socket.id}`);

  socket.on('disconnect', () => {
    logger.info(`Client disconnected: ${socket.id}`);
  });

  socket.on('download:start', async (data) => {
    const { url, socketId } = data;
    await handleDownload(url, socketId);
  });

  socket.on('download:cancel', (downloadId) => {
    cancelDownload(downloadId);
  });
});

// Extract video ID from URL
function extractVideoInfo(url) {
  const clipMatch = url.match(/clip\/([A-Za-z0-9_-]+)/);
  const videoMatch = url.match(/(?:v=|\/)([\w-]{11})/);
  const shortsMatch = url.match(/shorts\/([\w-]{11})/);

  return {
    isClip: !!clipMatch,
    id: clipMatch?.[1] || videoMatch?.[1] || shortsMatch?.[1] || Date.now().toString(),
    type: clipMatch ? 'clip' : (shortsMatch ? 'short' : 'video')
  };
}

// Simplified clip timing normalization
function normalizeClipTimes(s, e) {
  let start = Number(s);
  let end = Number(e);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  // Convert microseconds to seconds
  if (start > 1e6 && end > 1e6) {
    start = start / 1000 / 1000;
    end = end / 1000 / 1000;
  } 
  // Convert milliseconds to seconds
  else if (start > 1e3 && end > 1e3) {
    start = start / 1000;
    end = end / 1000;
  }
  if (end <= start) return null;
  const dur = end - start;
  // Reject clips shorter than 1s or longer than 20 minutes
  if (dur < 1 || dur > 1200) return null;
  return { start, end };
}

// Simplified clip metadata extraction using regex patterns only
async function resolveClipMeta(clipUrl) {
  try {
    const cookieHeader = await getYouTubeCookieHeader();
    const res = await fetch(clipUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(cookieHeader ? { 'Cookie': cookieHeader } : {})
      }
    });
    const html = await res.text();
    
    // Extract video ID
    const videoIdMatch = html.match(/"videoId":"([\w-]{11})"/);
    if (!videoIdMatch) {
      logger.warn('Could not find video ID in clip page');
      return null;
    }
    const videoId = videoIdMatch[1];
    
    // Pattern 1: clipConfig with startTimeMs/endTimeMs (most reliable)
    const clipConfigMatch = html.match(/"clipConfig":\s*\{[^}]*"startTimeMs":"(\d+)"[^}]*"endTimeMs":"(\d+)"[^}]*\}/);
    if (clipConfigMatch) {
      const timing = normalizeClipTimes(clipConfigMatch[1], clipConfigMatch[2]);
      if (timing) {
        logger.info(`Found clip timing via clipConfig: ${timing.start}s - ${timing.end}s`);
        return { videoId, start: timing.start, end: timing.end };
      }
    }
    
    // Pattern 2: startMs/endMs (absolute timestamps)
    const startMsMatch = html.match(/"startMs":"(\d+)"/);
    const endMsMatch = html.match(/"endMs":"(\d+)"/);
    if (startMsMatch && endMsMatch) {
      const timing = normalizeClipTimes(startMsMatch[1], endMsMatch[1]);
      if (timing) {
        logger.info(`Found clip timing via startMs/endMs: ${timing.start}s - ${timing.end}s`);
        return { videoId, start: timing.start, end: timing.end };
      }
    }
    
    // Pattern 3: startTimeMs/endTimeMs (legacy format)
    const startTimeMatch = html.match(/"startTimeMs":"(\d+)"/);
    const endTimeMatch = html.match(/"endTimeMs":"(\d+)"/);
    if (startTimeMatch && endTimeMatch) {
      const timing = normalizeClipTimes(startTimeMatch[1], endTimeMatch[1]);
      if (timing) {
        logger.info(`Found clip timing via startTimeMs/endTimeMs: ${timing.start}s - ${timing.end}s`);
        return { videoId, start: timing.start, end: timing.end };
      }
    }
    
    logger.warn('Clip metadata not found in page; proceeding with full video');
    return null;
  } catch (e) {
    logger.warn('Failed to fetch/parse clip page', e);
    return null;
  }
}

function secondsToTimestamp(totalSeconds) {
  const s = Math.max(0, totalSeconds);
  const hours = Math.floor(s / 3600);
  const minutes = Math.floor((s % 3600) / 60);
  const seconds = Math.floor(s % 60);
  const millis = Math.round((s - Math.floor(s)) * 1000);
  const hh = String(hours).padStart(2, '0');
  const mm = String(minutes).padStart(2, '0');
  const ss = String(seconds).padStart(2, '0');
  const mmm = String(millis).padStart(3, '0');
  return `${hh}:${mm}:${ss}.${mmm}`;
}

// Handle download process
async function handleDownload(url, socketId) {
  const downloadId = `download_${Date.now()}`;
  const videoInfo = extractVideoInfo(url);

  // Check if yt-dlp exists
  if (!existsSync(YTDLP_PATH)) {
    io.to(socketId).emit('download:error', {
      message: 'yt-dlp.exe not found. Please ensure it is in the project directory.'
    });
    return;
  }

  logger.info(`Starting download: ${url} (${videoInfo.type})`);

  // Prepare yt-dlp arguments
  const outputTemplate = path.join(DOWNLOADS_DIR, '%(title)s_%(id)s.%(ext)s');
  const args = [
    '--no-warnings',
    '--no-playlist',
    '--no-check-certificate',
    '--progress',
    '--newline',
    '-o', outputTemplate
  ];
  let postArgsApplied = false;

  let effectiveUrl = url;
  let clipSection = null;
  if (videoInfo.isClip) {
    const meta = await resolveClipMeta(url);
    if (meta?.videoId) {
      effectiveUrl = `https://www.youtube.com/watch?v=${meta.videoId}`;
      clipSection = `${secondsToTimestamp(meta.start)}-${secondsToTimestamp(meta.end)}`;
      args.push('--download-sections', `*${clipSection}`);
      args.push('--force-keyframes-at-cuts');
      postArgsApplied = true;
      
      logger.info(`Resolved clip meta for ${url} -> videoId=${meta.videoId} section=${clipSection}`);
    } else {
      // Fallback: metadata extraction failed, cannot extract clip timing
      logger.info(`Clip metadata extraction failed for ${url}`);
    }
  }

  // Add cookies if exists
  const cookiesPath = path.join(__dirname, 'cookies.txt');
  if (existsSync(cookiesPath)) {
    args.push('--cookies', cookiesPath);
  }

  // Add format selection for better quality and compatibility (prefer H.264 in MP4)
  args.push('-f', 'bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best');
  
  // Ensure merged output is MP4
  args.push('--merge-output-format', 'mp4');
  
  // Apply lightweight timestamp stabilization for all downloads
  args.push('--postprocessor-args', 'ffmpeg:-avoid_negative_ts make_zero -fflags +genpts -movflags +faststart');

  // Add the URL
  args.push(effectiveUrl);

  // Spawn yt-dlp process
  const ytdlp = spawn(YTDLP_PATH, args, {
    windowsHide: true
  });

  // Store active download
  activeDownloads.set(downloadId, {
    process: ytdlp,
    socketId,
    url,
    startTime: Date.now()
  });

  // Send initial status
  io.to(socketId).emit('download:init', {
    downloadId,
    url,
    type: videoInfo.type
  });

  // Handle stdout (progress and info)
  ytdlp.stdout.on('data', (data) => {
    const output = data.toString();

    // Parse progress
    const progressMatch = output.match(/\[download\]\s+(\d+\.?\d*)%/);
    const speedMatch = output.match(/at\s+([\d.]+\w+\/s)/);
    const etaMatch = output.match(/ETA\s+([\d:]+)/);
    const sizeMatch = output.match(/of\s+([\d.]+\w+)/);

    if (progressMatch) {
      io.to(socketId).emit('download:progress', {
        downloadId,
        percent: parseFloat(progressMatch[1]),
        speed: speedMatch?.[1] || 'N/A',
        eta: etaMatch?.[1] || 'N/A',
        size: sizeMatch?.[1] || 'N/A'
      });
    }

    // Parse destination
    const destMatch = output.match(/\[download\] Destination: (.+)/);
    if (destMatch) {
      io.to(socketId).emit('download:info', {
        downloadId,
        filename: path.basename(destMatch[1])
      });
    }

    // Parse merger
    if (output.includes('[Merger]')) {
      io.to(socketId).emit('download:status', {
        downloadId,
        status: 'Merging video and audio...'
      });
    }

    // Check for already downloaded
    if (output.includes('has already been downloaded')) {
      io.to(socketId).emit('download:status', {
        downloadId,
        status: 'File already exists, using cached version'
      });
    }
  });

  // Handle stderr (errors and warnings)
  ytdlp.stderr.on('data', (data) => {
    const error = data.toString();
    logger.error(`yt-dlp error: ${error}`);

    // Send non-critical warnings as status updates
    if (error.includes('WARNING')) {
      io.to(socketId).emit('download:warning', {
        downloadId,
        message: error
      });
    }
  });

  // Handle process completion
  ytdlp.on('close', async (code) => {
    activeDownloads.delete(downloadId);

    if (code === 0) {
      logger.info(`Download completed: ${downloadId}`);

      // Find the downloaded file
      try {
        const files = await fs.readdir(DOWNLOADS_DIR);
        const recentFiles = await Promise.all(
          files.map(async (file) => {
            const filePath = path.join(DOWNLOADS_DIR, file);
            const stats = await fs.stat(filePath);
            return { file, time: stats.mtime.getTime(), path: filePath };
          })
        );

        // Get most recent file
        recentFiles.sort((a, b) => b.time - a.time);
        const downloadedFile = recentFiles[0];

        if (downloadedFile && (Date.now() - downloadedFile.time < 60000)) {
          const stats = await fs.stat(downloadedFile.path);

          io.to(socketId).emit('download:complete', {
            downloadId,
            filename: downloadedFile.file,
            size: (stats.size / (1024 * 1024)).toFixed(2) + ' MB',
            downloadUrl: `/api/download/${encodeURIComponent(downloadedFile.file)}`
          });
        } else {
          io.to(socketId).emit('download:error', {
            downloadId,
            message: 'Download completed but file not found'
          });
        }
      } catch (error) {
        logger.error('Error finding downloaded file:', error);
        io.to(socketId).emit('download:error', {
          downloadId,
          message: 'Error processing downloaded file'
        });
      }
    } else {
      logger.error(`Download failed with code ${code}`);
      io.to(socketId).emit('download:error', {
        downloadId,
        message: 'Download failed. Please check the URL and try again.'
      });
    }
  });

  // Handle process errors
  ytdlp.on('error', (error) => {
    logger.error('Failed to start yt-dlp:', error);
    activeDownloads.delete(downloadId);
    io.to(socketId).emit('download:error', {
      downloadId,
      message: 'Failed to start download process'
    });
  });
}

// Cancel download
function cancelDownload(downloadId) {
  const download = activeDownloads.get(downloadId);
  if (download) {
    download.process.kill();
    activeDownloads.delete(downloadId);
    logger.info(`Download cancelled: ${downloadId}`);
  }
}

// API Routes

// Health check
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    ytdlp: existsSync(YTDLP_PATH),
    downloads: activeDownloads.size,
    uptime: process.uptime()
  });
});

// List downloads
app.get('/api/downloads', async (req, res) => {
  try {
    const files = await fs.readdir(DOWNLOADS_DIR);
    const fileDetails = await Promise.all(
      files.map(async (file) => {
        const filePath = path.join(DOWNLOADS_DIR, file);
        const stats = await fs.stat(filePath);
        return {
          name: file,
          size: (stats.size / (1024 * 1024)).toFixed(2) + ' MB',
          date: stats.mtime,
          url: `/api/download/${encodeURIComponent(file)}`
        };
      })
    );

    fileDetails.sort((a, b) => b.date - a.date);
    res.json(fileDetails);
  } catch (error) {
    res.status(500).json({ error: 'Failed to list downloads' });
  }
});

// Download file
app.get('/api/download/:filename', (req, res) => {
  const filename = decodeURIComponent(req.params.filename);
  const filePath = path.join(DOWNLOADS_DIR, filename);

  if (!existsSync(filePath)) {
    return res.status(404).json({ error: 'File not found' });
  }

  res.download(filePath);
});

// Delete file
app.delete('/api/download/:filename', async (req, res) => {
  const filename = decodeURIComponent(req.params.filename);
  const filePath = path.join(DOWNLOADS_DIR, filename);

  try {
    await fs.unlink(filePath);
    res.json({ message: 'File deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: 'Failed to delete file' });
  }
});

// Clean old downloads (older than 24 hours)
app.post('/api/clean', async (req, res) => {
  try {
    const files = await fs.readdir(DOWNLOADS_DIR);
    const now = Date.now();
    let deleted = 0;

    for (const file of files) {
      const filePath = path.join(DOWNLOADS_DIR, file);
      const stats = await fs.stat(filePath);
      const age = now - stats.mtime.getTime();

      if (age > 24 * 60 * 60 * 1000) {
        await fs.unlink(filePath);
        deleted++;
      }
    }

    res.json({ message: `Cleaned ${deleted} old files` });
  } catch (error) {
    res.status(500).json({ error: 'Failed to clean downloads' });
  }
});

// Validate URL
app.post('/api/validate', (req, res) => {
  const { url } = req.body;

  const isValid = url && (
    url.includes('youtube.com') ||
    url.includes('youtu.be')
  );

  const videoInfo = extractVideoInfo(url);

  res.json({
    valid: isValid,
    type: videoInfo.type,
    id: videoInfo.id
  });
});

// Check yt-dlp status
app.get('/api/ytdlp/status', (req, res) => {
  const exists = existsSync(YTDLP_PATH);

  if (exists) {
    const ytdlp = spawn(YTDLP_PATH, ['--version'], { windowsHide: true });
    let version = '';

    ytdlp.stdout.on('data', (data) => {
      version = data.toString().trim();
    });

    ytdlp.on('close', () => {
      res.json({ installed: true, version });
    });
  } else {
    res.json({ installed: false });
  }
});

// Start server
async function startServer() {
  await ensureDirectories();

  server.listen(PORT, () => {
    logger.info(`\n${'='.repeat(60)}`);
    logger.info(`  YouTube Clip Downloader Server`);
    logger.info(`${'='.repeat(60)}`);
    logger.info(`\n✓ Server running at: http://localhost:${PORT}`);
    logger.info(`✓ Downloads folder: ${DOWNLOADS_DIR}`);
    logger.info(`✓ yt-dlp.exe: ${existsSync(YTDLP_PATH) ? 'Found' : 'Not found'}`);
    logger.info(`\nPress Ctrl+C to stop the server\n`);
  });
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  logger.info('\nShutting down server...');

  // Kill all active downloads
  activeDownloads.forEach((download) => {
    download.process.kill();
  });

  server.close(() => {
    logger.info('Server closed');
    process.exit(0);
  });
});

// Start the server
startServer().catch((error) => {
  logger.error('Failed to start server:', error);
  process.exit(1);
});
