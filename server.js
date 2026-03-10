import express from 'express';
import cors from 'cors';
import ffmpeg from 'fluent-ffmpeg';
import { randomUUID } from 'crypto';
import { writeFile, mkdir, stat, access, rm } from 'fs/promises';
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

// ─── Render Pipeline ──────────────────────────────────────────────────────────

async function processJob(taskId, params) {
  const {
    segments,
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
        musicVolume, voiceoverVolume
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

    updateJob(taskId, { progress: 90 });
    log(taskId, 'render', 'FFmpeg render complete');

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

async function renderSingleSegment(taskId, inputPath, outputPath, segment, outW, outH, fps, outputFormat, voiceoverPath, musicPath, musicVolume, voiceoverVolume) {
  const segVol = segment.volume ?? 1.0;
  const speed = segment.speed ?? 1.0;

  let vf = `scale=${outW}:${outH}:force_original_aspect_ratio=decrease,pad=${outW}:${outH}:(ow-iw)/2:(oh-ih)/2,fps=${fps}`;
  if (speed !== 1.0) {
    vf = `setpts=${(1 / speed).toFixed(4)}*PTS,${vf}`;
  }

  const args = ['-i', inputPath];

  if (voiceoverPath) args.push('-i', voiceoverPath);
  if (musicPath) args.push('-i', musicPath);

  const audioFilter = buildAudioMixFilter(0, segVol, speed, voiceoverPath, musicPath, musicVolume, voiceoverVolume);

  if (audioFilter) {
    // When we have a complex audio filter, we can't use -vf and -filter_complex together.
    // So embed the video filter into the complex filter as well.
    const fullFilter = `[0:v]${vf}[vout];${audioFilter.filter}`;
    args.push('-filter_complex', fullFilter);
    args.push('-map', '[vout]');
    args.push('-map', `[${audioFilter.outputLabel}]`);
  } else {
    args.push('-vf', vf);
    if (segVol !== 1.0 || speed !== 1.0) {
      let af = `volume=${segVol}`;
      if (speed !== 1.0) af = `atempo=${speed},${af}`;
      args.push('-af', af);
    }
  }

  const codecArgs = buildVideoCodecArgs(outputFormat);
  args.push(...codecArgs);
  args.push('-movflags', '+faststart');
  args.push('-shortest');
  args.push('-y', outputPath);

  await runFfmpeg(taskId, args);
}

async function renderWithConcat(taskId, segmentPaths, segments, outputPath, outW, outH, fps, outputFormat, voiceoverPath, musicPath, musicVolume, voiceoverVolume, workDir) {
  // Step 1: Normalize all segments to same resolution/fps/codec
  const normalizedPaths = [];
  for (let i = 0; i < segmentPaths.length; i++) {
    const seg = segments[i];
    const normPath = join(workDir, `norm_${i}.ts`);
    const segVol = seg.volume ?? 1.0;
    const speed = seg.speed ?? 1.0;

    let vf = `scale=${outW}:${outH}:force_original_aspect_ratio=decrease,pad=${outW}:${outH}:(ow-iw)/2:(oh-ih)/2,fps=${fps},format=yuv420p`;
    if (speed !== 1.0) {
      vf = `setpts=${(1 / speed).toFixed(4)}*PTS,${vf}`;
    }

    let af = `volume=${segVol}`;
    if (speed !== 1.0) af = `atempo=${speed},${af}`;

    // Check if input has audio — if not, add silent audio source
    const inputHasAudio = await hasAudioStream(segmentPaths[i]);

    const args = ['-i', segmentPaths[i]];

    if (!inputHasAudio) {
      args.push('-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=stereo');
    }

    args.push(
      '-vf', vf,
      '-af', af,
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-threads', '2',
      '-c:a', 'aac', '-b:a', '128k',
      '-ar', '48000', '-ac', '2',
    );

    if (!inputHasAudio) {
      args.push('-map', '0:v', '-map', '1:a', '-shortest');
    }

    args.push('-y', normPath);

    log(taskId, 'render', `Normalizing segment ${i}`);
    await runFfmpeg(taskId, args);
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
  // Step 1: Normalize all segments to same resolution/fps/codec
  const normalizedPaths = [];
  const normalizedDurations = [];
  for (let i = 0; i < segmentPaths.length; i++) {
    const seg = segments[i];
    const normPath = join(workDir, `xnorm_${i}.mp4`);
    const segVol = seg.volume ?? 1.0;
    const speed = seg.speed ?? 1.0;

    let vf = `scale=${outW}:${outH}:force_original_aspect_ratio=decrease,pad=${outW}:${outH}:(ow-iw)/2:(oh-ih)/2,fps=${fps},format=yuv420p`;
    if (speed !== 1.0) {
      vf = `setpts=${(1 / speed).toFixed(4)}*PTS,${vf}`;
    }

    let af = `volume=${segVol}`;
    if (speed !== 1.0) af = `atempo=${speed},${af}`;

    // Check if input has audio — if not, add silent audio source
    const inputHasAudio = await hasAudioStream(segmentPaths[i]);
    log(taskId, 'render', `Segment ${i} has audio: ${inputHasAudio}`);

    const args = ['-i', segmentPaths[i]];

    if (!inputHasAudio) {
      // Add silent audio as second input
      args.push('-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=stereo');
    }

    args.push(
      '-vf', vf,
      '-af', af,
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', '-threads', '2',
      '-c:a', 'aac', '-b:a', '128k',
      '-ar', '48000', '-ac', '2',
    );

    if (!inputHasAudio) {
      args.push('-map', '0:v', '-map', '1:a', '-shortest');
    }

    args.push('-y', normPath);

    log(taskId, 'render', `Normalizing segment ${i} for xfade`);
    await runFfmpeg(taskId, args);
    normalizedPaths.push(normPath);

    const actualDur = await getVideoDuration(normPath);
    normalizedDurations.push(actualDur);

    const pct = 30 + Math.round((i + 1) / segmentPaths.length * 15);
    updateJob(taskId, { progress: pct });
  }

  // Step 2: Build xfade filter chain
  const transitionDuration = 0.5;
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
    const offset = Math.max(0, cumulativeOffset - transitionDuration);
    const isLast = i === normalizedPaths.length - 1;
    const vOutLabel = isLast ? '[vout]' : `[v${i}]`;
    const aOutLabel = isLast ? '[aout_raw]' : `[a${i}]`;

    videoFilter += `${prevVideoLabel}[${i}:v]xfade=transition=${xfadeName}:duration=${transitionDuration}:offset=${offset.toFixed(3)}${vOutLabel};`;
    audioFilter += `${prevAudioLabel}[${i}:a]acrossfade=d=${transitionDuration}:c1=tri:c2=tri${aOutLabel};`;

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
