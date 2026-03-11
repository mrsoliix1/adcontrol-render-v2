import express from 'express';
import cors from 'cors';
import ffmpeg from 'fluent-ffmpeg';
import { randomUUID } from 'crypto';
import { writeFile, mkdir, stat, access, rm, rename } from 'fs/promises';
import { createWriteStream, createReadStream } from 'fs';
import { join } from 'path';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import { execSync, spawn } from 'child_process';

// ─── Config ───────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3001;
const RENDER_SECRET = process.env.RENDER_SECRET;
const TEMP_DIR = '/tmp/renders';
const JOB_TTL_MS = 60 * 60 * 1000; // 1 hour
const CLEANUP_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

// ─── Mappings ─────────────────────────────────────────────────────────────────

const XFADE_MAP = {
  'fade': 'fade',
  'dissolve': 'dissolve',
  'fade-black': 'fadeblack',
  'fade-white': 'fadewhite',
  'slide-left': 'slideleft',
  'slide-right': 'slideright',
  'slide-up': 'slideup',
  'slide-down': 'slidedown',
  'wipe-left': 'wipeleft',
  'wipe-right': 'wiperight',
  'wipe-up': 'wipeup',
  'wipe-down': 'wipedown',
  'zoom-in': 'zoomin',
  'pixelate': 'pixelize',
  'blur-dissolve': 'smoothleft',
};

const RESOLUTIONS = {
  '1080p': { w: 1920, h: 1080 },
  '720p': { w: 1280, h: 720 },
  '480p': { w: 854, h: 480 },
  '4k': { w: 3840, h: 2160 },
};

const ASPECT_RATIOS = {
  '16:9': { num: 16, den: 9 },
  '9:16': { num: 9, den: 16 },
  '1:1': { num: 1, den: 1 },
  '4:5': { num: 4, den: 5 },
};

// ─── Job Store ────────────────────────────────────────────────────────────────

const jobs = new Map();

function createJob(taskId, projectId, segmentCount) {
  const job = {
    taskId,
    projectId,
    status: 'pending',
    progress: 0,
    outputUrl: null,
    outputPath: null,
    error: null,
    createdAt: Date.now(),
    completedAt: null,
    segmentCount,
  };
  jobs.set(taskId, job);
  return job;
}

function updateJob(taskId, updates) {
  const job = jobs.get(taskId);
  if (job) Object.assign(job, updates);
  return job;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function log(taskId, phase, msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] [${taskId?.slice(0, 8) ?? '--------'}] [${phase}] ${msg}`);
}

async function ensureTempDir(taskId) {
  const dir = join(TEMP_DIR, taskId);
  await mkdir(dir, { recursive: true });
  return dir;
}

async function downloadFile(url, destPath, taskId, label, retries = 1) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      log(taskId, 'download', `Downloading ${label}: ${url} (attempt ${attempt + 1})`);
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
      const fileStream = createWriteStream(destPath);
      await pipeline(Readable.fromWeb(res.body), fileStream);
      log(taskId, 'download', `Downloaded ${label} to ${destPath}`);
      return destPath;
    } catch (err) {
      if (attempt >= retries) {
        throw new Error(`Failed to download ${label} after ${retries + 1} attempts: ${err.message}`);
      }
      log(taskId, 'download', `Retry downloading ${label}: ${err.message}`);
    }
  }
}

// Convert a still image to a video clip of specified duration
async function imageToVideo(taskId, imagePath, outputPath, duration, outW, outH, fps) {
  log(taskId, 'image', `Converting image to ${duration}s video: ${imagePath}`);
  const args = [
    '-loop', '1',
    '-i', imagePath,
    '-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=stereo',
    '-t', String(duration),
    '-vf', `scale=${outW}:${outH}:force_original_aspect_ratio=decrease,pad=${outW}:${outH}:(ow-iw)/2:(oh-ih)/2,fps=${fps},format=yuv420p`,
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-threads', '2',
    '-c:a', 'aac', '-b:a', '128k', '-ar', '48000', '-ac', '2',
    '-map', '0:v', '-map', '1:a',
    '-shortest',
    '-y', outputPath,
  ];
  await runFfmpeg(taskId, args);
  return outputPath;
}

// Check if a file has an audio stream
function hasAudioStream(filePath) {
  return new Promise((resolve) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) return resolve(false);
      const hasAudio = metadata.streams.some(s => s.codec_type === 'audio');
      resolve(hasAudio);
    });
  });
}

function getVideoDuration(filePath) {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(filePath, (err, metadata) => {
      if (err) return reject(err);
      resolve(metadata.format.duration || 0);
    });
  });
}

function computeAspectDimensions(baseW, baseH, aspectRatio) {
  if (!aspectRatio || !ASPECT_RATIOS[aspectRatio]) return { w: baseW, h: baseH };
  const { num, den } = ASPECT_RATIOS[aspectRatio];
  const targetRatio = num / den;
  const currentRatio = baseW / baseH;

  if (Math.abs(currentRatio - targetRatio) < 0.01) return { w: baseW, h: baseH };

  if (targetRatio > currentRatio) {
    return { w: baseW, h: Math.round(baseW / targetRatio / 2) * 2 };
  } else {
    return { w: Math.round(baseH * targetRatio / 2) * 2, h: baseH };
  }
}

// ─── FFmpeg Runner ────────────────────────────────────────────────────────────

function runFfmpeg(taskId, args) {
  return new Promise((resolve, reject) => {
    log(taskId, 'ffmpeg', `Running: ffmpeg ${args.slice(0, 6).join(' ')} ... (${args.length} args total)`);
    const proc = spawn('ffmpeg', args, { stdio: ['pipe', 'pipe', 'pipe'] });

    // Only keep the last 4KB of stderr to avoid memory bloat
    let stderrBuf = '';
    const MAX_STDERR = 4096;
    proc.stderr.on('data', (data) => {
      stderrBuf += data.toString();
      if (stderrBuf.length > MAX_STDERR * 2) {
        stderrBuf = stderrBuf.slice(-MAX_STDERR);
      }
    });

    proc.on('close', (code) => {
      if (code === 0) {
        log(taskId, 'ffmpeg', 'FFmpeg completed successfully');
        resolve();
      } else {
        const lastLines = stderrBuf.slice(-2000).split('\n').slice(-15).join('\n');
        log(taskId, 'ffmpeg', `FFmpeg failed (code ${code}): ${lastLines}`);
        reject(new Error(`FFmpeg exited with code ${code}: ${lastLines}`));
      }
    });

    proc.on('error', (err) => {
      reject(new Error(`FFmpeg spawn error: ${err.message}`));
    });
  });
}

// ─── Codec Helpers ────────────────────────────────────────────────────────────

function buildVideoCodecArgs(outputFormat) {
  if (outputFormat === 'webm') {
    return ['-c:v', 'libvpx-vp9', '-b:v', '2M', '-c:a', 'libopus'];
  }
  return ['-c:v', 'libx264', '-preset', 'fast', '-crf', '23', '-threads', '2', '-c:a', 'aac', '-b:a', '128k'];
}

// ─── Audio Mix Filter Builder ─────────────────────────────────────────────────

function buildAudioMixFilter(videoInputIdx, segVol, speed, voiceoverPath, musicPath, musicVolume, voiceoverVolume) {
  if (!voiceoverPath && !musicPath) return null;

  let idx = videoInputIdx + 1;
  const filterParts = [];
  const mixLabels = [];

  let mainAf = `[${videoInputIdx}:a]volume=${segVol}`;
  if (speed !== 1.0) mainAf = `[${videoInputIdx}:a]atempo=${speed},volume=${segVol}`;
  filterParts.push(`${mainAf}[amain]`);
  mixLabels.push('[amain]');

  if (voiceoverPath) {
    filterParts.push(`[${idx}:a]volume=${voiceoverVolume}[avo]`);
    mixLabels.push('[avo]');
    idx++;
  }

  if (musicPath) {
    filterParts.push(`[${idx}:a]volume=${musicVolume},aloop=loop=-1:size=2e+09[amusic]`);
    mixLabels.push('[amusic]');
    idx++;
  }

  filterParts.push(`${mixLabels.join('')}amix=inputs=${mixLabels.length}:duration=first:dropout_transition=2[aout]`);

  return {
    filter: filterParts.join(';'),
    outputLabel: 'aout',
  };
}

// ─── Filter Preset Map (matches FILTER_PRESETS in types.ts) ──────────────────

const FILTER_PRESET_MAP = {
  'none': '',
  'vivid': 'eq=saturation=1.4:contrast=1.1',
  'warm': 'eq=saturation=1.2:brightness=0.05,colorbalance=rs=0.1:gs=0.05:bs=-0.1',
  'cool': 'hue=h=15,eq=saturation=0.9',
  'bw': 'hue=s=0',
  'vintage': 'eq=saturation=0.6:contrast=0.9:brightness=0.1,colorbalance=rs=0.15:gs=0.05:bs=-0.1',
  'cinematic': 'eq=contrast=1.2:saturation=0.85:brightness=-0.05',
  'moody': 'eq=contrast=1.3:brightness=-0.15:saturation=0.7',
  'bright': 'eq=brightness=0.2:saturation=1.1',
  'faded': 'eq=contrast=0.85:brightness=0.1:saturation=0.6',
  'dramatic': 'eq=contrast=1.4:brightness=-0.1',
  'pastel': 'eq=brightness=0.15:saturation=0.5',
  'muted': 'eq=saturation=0.4:contrast=0.95',
};

// ─── Visual Filter Builder ───────────────────────────────────────────────────

function needsOverlayCompositing(seg) {
  return (seg.positionX || 0) !== 0 ||
         (seg.positionY || 0) !== 0 ||
         (seg.scale ?? 1) !== 1 ||
         (seg.rotation || 0) !== 0 ||
         (seg.opacity ?? 1) < 1;
}

function buildSegmentVisualFilters(seg, outW, outH, fps) {
  const filters = [];

  // 1. Crop
  const cT = seg.cropTop || 0, cB = seg.cropBottom || 0;
  const cL = seg.cropLeft || 0, cR = seg.cropRight || 0;
  if (cT > 0 || cB > 0 || cL > 0 || cR > 0) {
    filters.push(`crop=iw*(${(100 - cL - cR) / 100}):ih*(${(100 - cT - cB) / 100}):iw*(${cL / 100}):ih*(${cT / 100})`);
  }

  // 2. Speed
  const speed = seg.speed ?? 1.0;
  if (speed !== 1.0) {
    filters.push(`setpts=${(1 / speed).toFixed(4)}*PTS`);
  }

  // 3. Scale based on fitMode
  const fitMode = seg.fitMode || 'fit';
  switch (fitMode) {
    case 'fill':
    case 'crop':
      filters.push(`scale=${outW}:${outH}:force_original_aspect_ratio=increase`);
      filters.push(`crop=${outW}:${outH}`);
      break;
    case 'stretch':
      filters.push(`scale=${outW}:${outH}`);
      break;
    default: // fit
      filters.push(`scale=${outW}:${outH}:force_original_aspect_ratio=decrease`);
      filters.push(`pad=${outW}:${outH}:(ow-iw)/2:(oh-ih)/2`);
  }

  filters.push(`fps=${fps}`);
  filters.push('format=yuv420p');

  // 4. Filter preset
  const presetFilter = FILTER_PRESET_MAP[seg.filter || 'none'] || '';
  if (presetFilter) filters.push(presetFilter);

  // 5. Color adjustments (stacks on top of preset)
  const brightness = (seg.brightness || 0) / 100;
  const contrast = 1 + (seg.contrast || 0) / 100;
  const saturation = 1 + (seg.saturation || 0) / 100;
  if (brightness !== 0 || contrast !== 1 || saturation !== 1) {
    filters.push(`eq=brightness=${brightness.toFixed(3)}:contrast=${contrast.toFixed(3)}:saturation=${saturation.toFixed(3)}`);
  }

  // 6. Blur
  const blur = seg.blur || 0;
  if (blur > 0) {
    filters.push(`boxblur=${Math.min(blur, 20)}:${Math.min(blur, 20)}`);
  }

  // 7. Fade in/out
  const fadeIn = seg.fadeInDuration || 0;
  const fadeOut = seg.fadeOutDuration || 0;
  const duration = seg.duration || 5;
  if (fadeIn > 0) filters.push(`fade=t=in:st=0:d=${fadeIn}`);
  if (fadeOut > 0) {
    filters.push(`fade=t=out:st=${Math.max(0, duration - fadeOut).toFixed(3)}:d=${fadeOut}`);
  }

  return filters.join(',');
}

// Build overlay-based filter_complex for position/scale/rotation/opacity
function buildOverlayFilter(seg, outW, outH, fps, audioIdx, af) {
  const parts = [];
  let label = '[0:v]';
  let step = 0;

  // 1. Crop
  const cT = seg.cropTop || 0, cB = seg.cropBottom || 0;
  const cL = seg.cropLeft || 0, cR = seg.cropRight || 0;
  if (cT > 0 || cB > 0 || cL > 0 || cR > 0) {
    const next = `[c${step}]`;
    parts.push(`${label}crop=iw*(${(100-cL-cR)/100}):ih*(${(100-cT-cB)/100}):iw*(${cL/100}):ih*(${cT/100})${next}`);
    label = next;
    step++;
  }

  // 2. Speed
  const speed = seg.speed ?? 1.0;
  if (speed !== 1.0) {
    const next = `[c${step}]`;
    parts.push(`${label}setpts=${(1/speed).toFixed(4)}*PTS${next}`);
    label = next;
    step++;
  }

  // 3. Scale based on fitMode (to intermediate size, not padded yet)
  const fitMode = seg.fitMode || 'fit';
  const scale = seg.scale ?? 1;
  let scaleChain;
  switch (fitMode) {
    case 'fill':
    case 'crop':
      scaleChain = `scale=${outW}:${outH}:force_original_aspect_ratio=increase,crop=${outW}:${outH}`;
      break;
    case 'stretch':
      scaleChain = `scale=${outW}:${outH}`;
      break;
    default:
      scaleChain = `scale=${outW}:${outH}:force_original_aspect_ratio=decrease`;
  }

  // Apply scale factor
  if (scale !== 1) {
    scaleChain += `,scale=trunc(iw*${scale.toFixed(3)}/2)*2:trunc(ih*${scale.toFixed(3)}/2)*2`;
  }

  // 4. Filter preset + color adjustments + blur
  let colorChain = '';
  const presetFilter = FILTER_PRESET_MAP[seg.filter || 'none'] || '';
  if (presetFilter) colorChain += presetFilter;

  const brightness = (seg.brightness || 0) / 100;
  const contrast = 1 + (seg.contrast || 0) / 100;
  const saturation = 1 + (seg.saturation || 0) / 100;
  if (brightness !== 0 || contrast !== 1 || saturation !== 1) {
    if (colorChain) colorChain += ',';
    colorChain += `eq=brightness=${brightness.toFixed(3)}:contrast=${contrast.toFixed(3)}:saturation=${saturation.toFixed(3)}`;
  }

  const blur = seg.blur || 0;
  if (blur > 0) {
    if (colorChain) colorChain += ',';
    colorChain += `boxblur=${Math.min(blur,20)}:${Math.min(blur,20)}`;
  }

  // 5. Rotation
  const rotation = seg.rotation || 0;
  let rotateChain = '';
  if (rotation !== 0) {
    const rad = (rotation * Math.PI / 180).toFixed(4);
    rotateChain = `,rotate=${rad}:c=black@0:ow=rotw(${rad}):oh=roth(${rad})`;
  }

  // 6. Opacity
  const opacity = seg.opacity ?? 1;
  let opacityChain = '';
  if (opacity < 1) {
    opacityChain = `,format=rgba,colorchannelmixer=aa=${opacity.toFixed(3)}`;
  }

  // Combine into foreground chain
  let fgChain = scaleChain;
  if (colorChain) fgChain += ',' + colorChain;
  if (rotateChain) fgChain += rotateChain;
  if (opacityChain) fgChain += opacityChain;

  const next = `[fg]`;
  parts.push(`${label}${fgChain}${next}`);

  // 7. Background canvas
  parts.push(`color=black:s=${outW}x${outH}:r=${fps}[bg]`);

  // 8. Overlay with position offset
  const posX = seg.positionX || 0;
  const posY = seg.positionY || 0;
  const offsetX = Math.round((posX / 100) * outW);
  const offsetY = Math.round((posY / 100) * outH);
  const overlayX = `(W-w)/2+${offsetX}`;
  const overlayY = `(H-h)/2+${offsetY}`;

  parts.push(`[bg][fg]overlay=${overlayX}:${overlayY}:shortest=1,fps=${fps},format=yuv420p[prefade]`);

  // 9. Fade in/out
  const fadeIn = seg.fadeInDuration || 0;
  const fadeOut = seg.fadeOutDuration || 0;
  const duration = seg.duration || 5;
  let fadeChain = '';
  if (fadeIn > 0) fadeChain += `fade=t=in:st=0:d=${fadeIn}`;
  if (fadeOut > 0) {
    if (fadeChain) fadeChain += ',';
    fadeChain += `fade=t=out:st=${Math.max(0, duration - fadeOut).toFixed(3)}:d=${fadeOut}`;
  }

  if (fadeChain) {
    parts.push(`[prefade]${fadeChain}[vout]`);
  } else {
    // Rename [prefade] to [vout]
    parts[parts.length - 1] = parts[parts.length - 1].replace('[prefade]', '[vout]');
  }

  // 10. Audio
  parts.push(`[${audioIdx}:a]${af}[aout]`);

  return parts.join(';');
}

function buildSegmentAudioFilters(seg) {
  const parts = [];
  const speed = seg.speed ?? 1.0;
  const volume = seg.volume ?? 1.0;
  if (speed !== 1.0) parts.push(`atempo=${speed}`);
  parts.push(`volume=${volume}`);
  return parts.join(',');
}

// ─── Unified Segment Normalizer ──────────────────────────────────────────────

async function normalizeOneSegment(taskId, inputPath, outputPath, seg, outW, outH, fps) {
  const af = buildSegmentAudioFilters(seg);
  const inputHasAudio = await hasAudioStream(inputPath);
  log(taskId, 'render', `Normalizing segment (audio: ${inputHasAudio}, overlay: ${needsOverlayCompositing(seg)})`);

  const args = ['-i', inputPath];
  let audioIdx = 0;

  if (!inputHasAudio) {
    args.push('-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=stereo');
    audioIdx = 1;
  }

  let filterComplex;
  if (needsOverlayCompositing(seg)) {
    filterComplex = buildOverlayFilter(seg, outW, outH, fps, audioIdx, af);
  } else {
    const vf = buildSegmentVisualFilters(seg, outW, outH, fps);
    filterComplex = `[0:v]${vf}[vout];[${audioIdx}:a]${af}[aout]`;
  }

  args.push('-filter_complex', filterComplex);
  args.push('-map', '[vout]', '-map', '[aout]');
  if (!inputHasAudio) args.push('-shortest');

  args.push(
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-threads', '2',
    '-c:a', 'aac', '-b:a', '128k', '-ar', '48000', '-ac', '2',
    '-y', outputPath,
  );

  await runFfmpeg(taskId, args);
}

// ─── Text Overlay Post-Processing ────────────────────────────────────────────

async function applyTextOverlays(taskId, inputPath, outputPath, textOverlays, outW, outH, outputFormat) {
  if (!textOverlays || textOverlays.length === 0) return false;

  log(taskId, 'text', `Applying ${textOverlays.length} text overlay(s)`);

  const drawtextFilters = textOverlays.map(t => {
    const text = (t.text || '').replace(/'/g, "'\\\\\\''").replace(/:/g, '\\:').replace(/\n/g, '\\n');
    const fontSize = Math.round((t.fontSize || 48) * outW / 1920);
    const fontColor = (t.fontColor || 'white').replace('#', '0x');
    const fontFamily = t.fontFamily || 'Arial';

    // Position
    let x = '(w-text_w)/2';
    let yBase = 'h*0.85-text_h/2';
    if (t.textPosition === 'top') yBase = 'h*0.15-text_h/2';
    else if (t.textPosition === 'center') yBase = '(h-text_h)/2';

    const startTime = t.startTime || 0;
    const endTime = startTime + (t.duration || 5);
    const animDur = 0.5;

    // Text animation expressions
    let alphaExpr = '1';
    let yExpr = yBase;
    const anim = t.textAnimation || 'none';
    const tVar = `(t-${startTime.toFixed(3)})`;

    switch (anim) {
      case 'fade-in':
        alphaExpr = `if(lt(${tVar},${animDur}),${tVar}/${animDur},1)`;
        break;
      case 'slide-up':
        alphaExpr = `if(lt(${tVar},${animDur}),${tVar}/${animDur},1)`;
        yExpr = `${yBase}+if(lt(${tVar},${animDur}),80*(1-${tVar}/${animDur}),0)`;
        break;
      case 'slide-down':
        alphaExpr = `if(lt(${tVar},${animDur}),${tVar}/${animDur},1)`;
        yExpr = `${yBase}-if(lt(${tVar},${animDur}),80*(1-${tVar}/${animDur}),0)`;
        break;
      case 'bounce':
        yExpr = `${yBase}+if(lt(${tVar},0.6),sin(${tVar}/0.6*PI*3)*20*(1-${tVar}/0.6),0)`;
        break;
      default:
        break;
    }

    let filter = `drawtext=text='${text}':fontsize=${fontSize}:fontcolor=${fontColor}:fontfamily='${fontFamily}'`;
    filter += `:x=${x}:y=${yExpr}`;
    filter += `:alpha='if(between(t,${startTime.toFixed(3)},${endTime.toFixed(3)}),${alphaExpr},0)'`;

    // Bold
    if (t.fontWeight === 'bold' || t.fontWeight === '700') {
      filter += ':borderw=1:bordercolor=' + fontColor;
    }

    // Text background
    if (t.textBg) {
      const bgColor = t.textBg.replace('#', '0x');
      filter += `:box=1:boxcolor=${bgColor}@0.7:boxborderw=10`;
    }

    // Text stroke/outline
    if (t.textStroke && (t.textStrokeWidth || 0) > 0) {
      const strokeColor = t.textStroke.replace('#', '0x');
      filter += `:borderw=${t.textStrokeWidth}:bordercolor=${strokeColor}`;
    }

    return filter;
  });

  const vf = drawtextFilters.join(',');

  const args = [
    '-i', inputPath,
    '-vf', vf,
    '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
    '-c:a', 'copy',
    '-movflags', '+faststart',
    '-y', outputPath,
  ];

  await runFfmpeg(taskId, args);
  return true;
}

// ─── Render Pipeline ──────────────────────────────────────────────────────────

async function processJob(taskId, params) {
  const {
    segments,
    textOverlays = [],
    voiceoverUrl,
    musicUrl,
    musicVolume = 0.3,
    voiceoverVolume = 1.0,
    outputFormat = 'mp4',
    resolution = '1080p',
    aspectRatio = '16:9',
    fps = 30,
  } = params;

  const workDir = await ensureTempDir(taskId);
  const sortedSegments = [...segments].sort((a, b) => a.index - b.index);

  try {
    // ── Phase 1: Download ──────────────────────────────────────────────────
    updateJob(taskId, { status: 'downloading', progress: 0 });
    log(taskId, 'download', `Starting download of ${sortedSegments.length} segments`);

    const segmentPaths = [];
    const res0 = RESOLUTIONS[resolution] || RESOLUTIONS['1080p'];
    const { w: preW, h: preH } = computeAspectDimensions(res0.w, res0.h, aspectRatio);

    for (let i = 0; i < sortedSegments.length; i++) {
      const seg = sortedSegments[i];
      const url = seg.videoUrl || seg.imageUrl;
      if (!url) throw new Error(`segment[${i}] has no videoUrl or imageUrl`);

      const isImage = seg.type === 'image' || /\.(jpg|jpeg|png|gif|webp|bmp|svg)(\?|$)/i.test(url);
      const ext = url.split('.').pop()?.split('?')[0] || (isImage ? 'jpg' : 'mp4');
      const destPath = join(workDir, `segment_${i}.${ext}`);
      await downloadFile(url, destPath, taskId, `segment ${i}`);

      if (isImage) {
        // Convert still image to video clip
        const videoPath = join(workDir, `segment_${i}_img.mp4`);
        const dur = seg.duration || 5;
        await imageToVideo(taskId, destPath, videoPath, dur, preW, preH, fps);
        segmentPaths.push(videoPath);
      } else {
        segmentPaths.push(destPath);
      }
      updateJob(taskId, { progress: Math.round((i + 1) / sortedSegments.length * 25) });
    }

    let voiceoverPath = null;
    if (voiceoverUrl) {
      const ext = voiceoverUrl.split('.').pop()?.split('?')[0] || 'mp3';
      voiceoverPath = join(workDir, `voiceover.${ext}`);
      await downloadFile(voiceoverUrl, voiceoverPath, taskId, 'voiceover');
    }

    let musicPath = null;
    if (musicUrl) {
      const ext = musicUrl.split('.').pop()?.split('?')[0] || 'mp3';
      musicPath = join(workDir, `music.${ext}`);
      await downloadFile(musicUrl, musicPath, taskId, 'music');
    }

    updateJob(taskId, { progress: 30 });
    log(taskId, 'download', 'All downloads complete');

    // ── Phase 2: Render ────────────────────────────────────────────────────
    updateJob(taskId, { status: 'rendering', progress: 30 });

    const res = RESOLUTIONS[resolution] || RESOLUTIONS['1080p'];
    const { w: outW, h: outH } = computeAspectDimensions(res.w, res.h, aspectRatio);
    const ext = outputFormat === 'webm' ? 'webm' : 'mp4';
    const outputPath = join(workDir, `output.${ext}`);

    const segDurations = [];
    for (let i = 0; i < segmentPaths.length; i++) {
      const dur = sortedSegments[i].duration || await getVideoDuration(segmentPaths[i]);
      segDurations.push(dur);
    }

    const hasTransitions = sortedSegments.some(
      (s, i) => i > 0 && s.transition && s.transition !== 'none' && s.transition !== 'cut'
    );

    if (segmentPaths.length === 1) {
      log(taskId, 'render', 'Single segment, reencoding directly');
      await renderSingleSegment(
        taskId, segmentPaths[0], outputPath, sortedSegments[0],
        outW, outH, fps, outputFormat, voiceoverPath, musicPath,
        musicVolume, voiceoverVolume, workDir
      );
    } else if (hasTransitions) {
      log(taskId, 'render', `Rendering ${segmentPaths.length} segments with xfade transitions`);
      await renderWithXfade(
        taskId, segmentPaths, segDurations, sortedSegments, outputPath,
        outW, outH, fps, outputFormat, voiceoverPath, musicPath,
        musicVolume, voiceoverVolume, workDir
      );
    } else {
      log(taskId, 'render', `Concat ${segmentPaths.length} segments without transitions`);
      await renderWithConcat(
        taskId, segmentPaths, sortedSegments, outputPath,
        outW, outH, fps, outputFormat, voiceoverPath, musicPath,
        musicVolume, voiceoverVolume, workDir
      );
    }

    updateJob(taskId, { progress: 85 });
    log(taskId, 'render', 'FFmpeg render complete');

    // ── Phase 2.5: Text Overlays ────────────────────────────────────────────
    if (textOverlays.length > 0) {
      const textOutputPath = join(workDir, `output_text.${ext}`);
      const applied = await applyTextOverlays(taskId, outputPath, textOutputPath, textOverlays, outW, outH, outputFormat);
      if (applied) {
        // Replace output with text-overlaid version
        await rename(textOutputPath, outputPath);
        log(taskId, 'text', 'Text overlays applied successfully');
      }
    }

    updateJob(taskId, { progress: 95 });

    // ── Phase 3: Finalize ──────────────────────────────────────────────────
    updateJob(taskId, { status: 'uploading', progress: 95 });

    await access(outputPath);
    const outputStat = await stat(outputPath);
    log(taskId, 'finalize', `Output file: ${outputPath} (${(outputStat.size / 1024 / 1024).toFixed(1)} MB)`);

    updateJob(taskId, {
      status: 'completed',
      progress: 100,
      outputPath,
      outputUrl: `/download/${taskId}`,
      completedAt: Date.now(),
    });
    log(taskId, 'finalize', 'Job completed successfully');

  } catch (err) {
    log(taskId, 'error', `Job failed: ${err.message}`);
    updateJob(taskId, { status: 'failed', error: err.message, completedAt: Date.now() });
  }
}

// ─── Render Strategies ────────────────────────────────────────────────────────

async function renderSingleSegment(taskId, inputPath, outputPath, segment, outW, outH, fps, outputFormat, voiceoverPath, musicPath, musicVolume, voiceoverVolume, workDir) {
  // If we have voiceover/music, normalize first then mix audio separately
  if (voiceoverPath || musicPath) {
    const normPath = join(workDir || '/tmp', `single_norm.mp4`);
    await normalizeOneSegment(taskId, inputPath, normPath, segment, outW, outH, fps);

    const segVol = segment.volume ?? 1.0;
    const speed = segment.speed ?? 1.0;
    const audioFilter = buildAudioMixFilter(0, segVol, speed, voiceoverPath, musicPath, musicVolume, voiceoverVolume);

    const args = ['-i', normPath];
    if (voiceoverPath) args.push('-i', voiceoverPath);
    if (musicPath) args.push('-i', musicPath);

    if (audioFilter) {
      args.push('-filter_complex', audioFilter.filter);
      args.push('-map', '0:v', '-map', `[${audioFilter.outputLabel}]`);
    }

    const codecArgs = buildVideoCodecArgs(outputFormat);
    args.push(...codecArgs, '-movflags', '+faststart', '-shortest', '-y', outputPath);
    await runFfmpeg(taskId, args);
  } else {
    // Simple case — just normalize directly to output
    await normalizeOneSegment(taskId, inputPath, outputPath, segment, outW, outH, fps);
  }
}

async function renderWithConcat(taskId, segmentPaths, segments, outputPath, outW, outH, fps, outputFormat, voiceoverPath, musicPath, musicVolume, voiceoverVolume, workDir) {
  // Step 1: Normalize all segments
  const normalizedPaths = [];
  for (let i = 0; i < segmentPaths.length; i++) {
    const normPath = join(workDir, `norm_${i}.ts`);
    log(taskId, 'render', `Normalizing segment ${i}`);
    await normalizeOneSegment(taskId, segmentPaths[i], normPath, segments[i], outW, outH, fps);
    normalizedPaths.push(normPath);

    const pct = 30 + Math.round((i + 1) / segmentPaths.length * 20);
    updateJob(taskId, { progress: pct });
  }

  // Step 2: Create concat demuxer file
  const concatFilePath = join(workDir, 'concat.txt');
  const concatContent = normalizedPaths.map(p => `file '${p}'`).join('\n');
  await writeFile(concatFilePath, concatContent);

  // Step 3: Concat and optionally mix additional audio
  const codecArgs = buildVideoCodecArgs(outputFormat);
  const args = ['-f', 'concat', '-safe', '0', '-i', concatFilePath];

  if (voiceoverPath) args.push('-i', voiceoverPath);
  if (musicPath) args.push('-i', musicPath);

  const inputCount = 1 + (voiceoverPath ? 1 : 0) + (musicPath ? 1 : 0);

  if (inputCount > 1) {
    const filterParts = [];
    const mixInputs = [];
    let idx = 0;

    filterParts.push(`[0:a]volume=1.0[a0]`);
    mixInputs.push('[a0]');
    idx++;

    if (voiceoverPath) {
      filterParts.push(`[${idx}:a]volume=${voiceoverVolume}[avo]`);
      mixInputs.push('[avo]');
      idx++;
    }

    if (musicPath) {
      filterParts.push(`[${idx}:a]volume=${musicVolume},aloop=loop=-1:size=2e+09[amusic]`);
      mixInputs.push('[amusic]');
      idx++;
    }

    filterParts.push(`${mixInputs.join('')}amix=inputs=${mixInputs.length}:duration=first:dropout_transition=2[aout]`);

    args.push('-filter_complex', filterParts.join(';'));
    args.push('-map', '0:v', '-map', '[aout]');
  }

  args.push(...codecArgs);
  args.push('-movflags', '+faststart');
  args.push('-shortest');
  args.push('-y', outputPath);

  log(taskId, 'render', 'Concatenating segments');
  updateJob(taskId, { progress: 60 });
  await runFfmpeg(taskId, args);
}

async function renderWithXfade(taskId, segmentPaths, segDurations, segments, outputPath, outW, outH, fps, outputFormat, voiceoverPath, musicPath, musicVolume, voiceoverVolume, workDir) {
  // Step 1: Normalize all segments
  const normalizedPaths = [];
  const normalizedDurations = [];
  for (let i = 0; i < segmentPaths.length; i++) {
    const normPath = join(workDir, `xnorm_${i}.mp4`);
    log(taskId, 'render', `Normalizing segment ${i} for xfade`);
    await normalizeOneSegment(taskId, segmentPaths[i], normPath, segments[i], outW, outH, fps);
    normalizedPaths.push(normPath);

    const actualDur = await getVideoDuration(normPath);
    normalizedDurations.push(actualDur);

    const pct = 30 + Math.round((i + 1) / segmentPaths.length * 15);
    updateJob(taskId, { progress: pct });
  }

  // Step 2: Build xfade filter chain with per-segment transition durations
  const inputs = normalizedPaths.map(p => ['-i', p]).flat();

  let videoFilter = '';
  let audioFilter = '';
  let prevVideoLabel = '[0:v]';
  let prevAudioLabel = '[0:a]';
  let cumulativeOffset = normalizedDurations[0];

  for (let i = 1; i < normalizedPaths.length; i++) {
    const seg = segments[i];
    const transType = seg.transition || 'fade';
    const xfadeName = XFADE_MAP[transType] || 'fade';
    const tDuration = Math.min(seg.transitionDuration || 0.5, normalizedDurations[i] * 0.8, cumulativeOffset * 0.8);
    const offset = Math.max(0, cumulativeOffset - tDuration);
    const isLast = i === normalizedPaths.length - 1;
    const vOutLabel = isLast ? '[vout]' : `[v${i}]`;
    const aOutLabel = isLast ? '[aout_raw]' : `[a${i}]`;

    videoFilter += `${prevVideoLabel}[${i}:v]xfade=transition=${xfadeName}:duration=${tDuration.toFixed(3)}:offset=${offset.toFixed(3)}${vOutLabel};`;
    audioFilter += `${prevAudioLabel}[${i}:a]acrossfade=d=${tDuration.toFixed(3)}:c1=tri:c2=tri${aOutLabel};`;

    prevVideoLabel = vOutLabel;
    prevAudioLabel = aOutLabel;
    cumulativeOffset = offset + normalizedDurations[i];
  }

  // Combine video and audio filter chains, remove trailing semicolon
  let fullFilter = (videoFilter + audioFilter).replace(/;$/, '');

  // Determine the audio output label from the xfade chain
  const audioOutLabel = normalizedPaths.length > 1 ? '[aout_raw]' : '[0:a]';
  let finalAudioLabel = audioOutLabel;

  // Add extra inputs and audio mixing for voiceover/music
  const extraInputs = [];
  let extraIdx = normalizedPaths.length;

  if (voiceoverPath) extraInputs.push('-i', voiceoverPath);
  if (musicPath) extraInputs.push('-i', musicPath);

  if (voiceoverPath || musicPath) {
    const mixParts = [];
    const mixInputLabels = [];

    mixParts.push(`${audioOutLabel}volume=1.0[amain]`);
    mixInputLabels.push('[amain]');

    if (voiceoverPath) {
      mixParts.push(`[${extraIdx}:a]volume=${voiceoverVolume}[avo]`);
      mixInputLabels.push('[avo]');
      extraIdx++;
    }

    if (musicPath) {
      mixParts.push(`[${extraIdx}:a]volume=${musicVolume},aloop=loop=-1:size=2e+09[amusic]`);
      mixInputLabels.push('[amusic]');
      extraIdx++;
    }

    mixParts.push(`${mixInputLabels.join('')}amix=inputs=${mixInputLabels.length}:duration=first:dropout_transition=2[afinal]`);

    fullFilter += ';' + mixParts.join(';');
    finalAudioLabel = '[afinal]';
  }

  const codecArgs = buildVideoCodecArgs(outputFormat);

  const args = [
    ...inputs,
    ...extraInputs,
    '-filter_complex', fullFilter,
    '-map', '[vout]',
    '-map', finalAudioLabel,
    ...codecArgs,
    '-movflags', '+faststart',
    '-shortest',
    '-y', outputPath,
  ];

  log(taskId, 'render', 'Running xfade render');
  updateJob(taskId, { progress: 60 });
  await runFfmpeg(taskId, args);
}

// ─── Cleanup ──────────────────────────────────────────────────────────────────

async function cleanupOldJobs() {
  const now = Date.now();
  for (const [taskId, job] of jobs) {
    if (now - job.createdAt > JOB_TTL_MS) {
      log(taskId, 'cleanup', 'Removing expired job');
      jobs.delete(taskId);
      try {
        const dir = join(TEMP_DIR, taskId);
        await rm(dir, { recursive: true, force: true });
      } catch { /* ignore */ }
    }
  }
}

// ─── Auth Middleware ───────────────────────────────────────────────────────────

function requireAuth(req, res, next) {
  if (!RENDER_SECRET) {
    return next();
  }
  const auth = req.headers.authorization;
  if (!auth || auth !== `Bearer ${RENDER_SECRET}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ─── Express App ──────────────────────────────────────────────────────────────

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Health check
app.get('/health', (_req, res) => {
  let ffmpegAvailable = false;
  try {
    execSync('ffmpeg -version', { stdio: 'pipe' });
    ffmpegAvailable = true;
  } catch { /* not available */ }

  res.json({
    status: 'ok',
    ffmpeg: ffmpegAvailable,
    uptime: process.uptime(),
    activeJobs: jobs.size,
  });
});

// Start render job
app.post('/render', requireAuth, async (req, res) => {
  try {
    const {
      projectId,
      segments,
      textOverlays,
      voiceoverUrl,
      musicUrl,
      musicVolume,
      voiceoverVolume,
      outputFormat,
      resolution,
      aspectRatio,
      fps,
    } = req.body;

    if (!segments || !Array.isArray(segments) || segments.length === 0) {
      return res.status(400).json({ error: 'segments array is required and must not be empty' });
    }

    for (let i = 0; i < segments.length; i++) {
      if (!segments[i].videoUrl && !segments[i].imageUrl) {
        return res.status(400).json({ error: `segment[${i}] is missing videoUrl or imageUrl` });
      }
    }

    const taskId = randomUUID();
    createJob(taskId, projectId, segments.length);

    const estimatedDuration = segments.length * 5 + 10;

    log(taskId, 'api', `New render job: ${segments.length} segments, format=${outputFormat || 'mp4'}, resolution=${resolution || '1080p'}`);

    // Start processing in background (don't await)
    processJob(taskId, {
      segments,
      textOverlays: textOverlays || [],
      voiceoverUrl,
      musicUrl,
      musicVolume,
      voiceoverVolume,
      outputFormat,
      resolution,
      aspectRatio,
      fps,
    }).catch((err) => {
      log(taskId, 'error', `Unhandled error in processJob: ${err?.message || err}`);
      try { updateJob(taskId, { status: 'failed', error: err?.message || 'Unknown error' }); } catch {}
    });

    res.json({
      taskId,
      status: 'rendering',
      estimatedDuration,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Check job status
app.get('/status/:taskId', (req, res) => {
  const job = jobs.get(req.params.taskId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  res.json({
    taskId: job.taskId,
    status: job.status,
    progress: job.progress,
    outputUrl: job.outputUrl,
    error: job.error,
  });
});

// Download rendered file
app.get('/download/:taskId', async (req, res) => {
  const job = jobs.get(req.params.taskId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  if (job.status !== 'completed' || !job.outputPath) {
    return res.status(400).json({
      error: 'Render not yet complete',
      taskId: job.taskId,
      status: job.status,
      progress: job.progress,
    });
  }

  try {
    await access(job.outputPath);
  } catch {
    return res.status(410).json({ error: 'Rendered file has been cleaned up' });
  }

  const ext = job.outputPath.endsWith('.webm') ? 'webm' : 'mp4';
  const mime = ext === 'webm' ? 'video/webm' : 'video/mp4';

  res.setHeader('Content-Type', mime);
  res.setHeader('Content-Disposition', `attachment; filename="render_${job.taskId.slice(0, 8)}.${ext}"`);

  const stream = createReadStream(job.outputPath);
  stream.pipe(res);
});

// Download info endpoint
app.get('/download-info/:taskId', (req, res) => {
  const job = jobs.get(req.params.taskId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  if (job.status !== 'completed') {
    return res.status(400).json({
      taskId: job.taskId,
      status: job.status,
    });
  }

  res.json({
    taskId: job.taskId,
    url: job.outputUrl,
    downloadUrl: `/download/${job.taskId}`,
    status: 'ready',
  });
});

// ─── Start Server ─────────────────────────────────────────────────────────────

await mkdir(TEMP_DIR, { recursive: true });

setInterval(cleanupOldJobs, CLEANUP_INTERVAL_MS);

process.on('unhandledRejection', (err) => {
  console.error('[UNHANDLED REJECTION]', err);
});

process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT EXCEPTION]', err);
  // Don't exit — keep the server alive
});

app.listen(PORT, () => {
  console.log(`Render server listening on port ${PORT}`);
  console.log(`Auth: ${RENDER_SECRET ? 'enabled' : 'DISABLED (no RENDER_SECRET set)'}`);
});
