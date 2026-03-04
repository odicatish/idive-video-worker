require("dotenv").config();

const express = require("express");
const ffmpeg = require("fluent-ffmpeg");
const ffmpegPath = require("ffmpeg-static");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { createClient } = require("@supabase/supabase-js");

ffmpeg.setFfmpegPath(ffmpegPath);

const app = express();
app.use(express.json({ limit: "10mb" }));

function requireEnv(name) {
  const v = (process.env[name] || "").trim();
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function getSupabaseAdmin() {
  const url = requireEnv("SUPABASE_URL");
  const key = requireEnv("SUPABASE_SERVICE_ROLE_KEY");

  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

// 1x1 PNG negru (fără lavfi)
const BLACK_PNG_BASE64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO1k3u0AAAAASUVORK5CYII=";

function getBearerToken(req) {
  const h = (req.headers.authorization || "").trim();
  const m = h.match(/^Bearer\s+(.+)$/i);
  return (m?.[1] || "").trim();
}

app.get("/", (req, res) => {
  res.json({ ok: true, service: "idive-video-worker" });
});

app.post("/render-mp4", async (req, res) => {
  let tmpDir = null;

  try {
    const secret = getBearerToken(req);
    if (!secret || secret !== requireEnv("VIDEO_RENDERER_SECRET")) {
      return res.status(401).json({ error: "unauthorized" });
    }

    const { jobId, audioUrl, output } = req.body;
    if (!jobId || !audioUrl || !output?.bucket || !output?.path) {
      return res.status(400).json({ error: "missing_params" });
    }

    const supabase = getSupabaseAdmin();

    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "idive-video-"));
    const audioPath = path.join(tmpDir, "audio.mp3");
    const stillPath = path.join(tmpDir, "black.png");
    const videoPath = path.join(tmpDir, "video.mp4");

    // 1) download audio (folosim fetch global din Node 22)
    const audioResp = await fetch(audioUrl);
    if (!audioResp.ok) {
      return res.status(400).json({ error: `audio_download_failed_${audioResp.status}` });
    }
    const audioBuffer = Buffer.from(await audioResp.arrayBuffer());
    fs.writeFileSync(audioPath, audioBuffer);

    // 2) write black png
    fs.writeFileSync(stillPath, Buffer.from(BLACK_PNG_BASE64, "base64"));

    // 3) render mp4 (fără lavfi)
    await new Promise((resolve, reject) => {
      ffmpeg()
        .input(stillPath)
        .inputOptions(["-loop 1", "-framerate 30"])
        .input(audioPath)
        .outputOptions([
          "-c:v libx264",
          "-tune stillimage",
          "-vf scale=1080:1080,format=yuv420p",
          "-c:a aac",
          "-b:a 192k",
          "-shortest",
          "-movflags +faststart",
        ])
        .save(videoPath)
        .on("end", resolve)
        .on("error", reject);
    });

    const videoBuffer = fs.readFileSync(videoPath);

    // 4) upload mp4
    const { error: uploadErr } = await supabase.storage
      .from(output.bucket)
      .upload(output.path, videoBuffer, {
        contentType: "video/mp4",
        upsert: true,
        cacheControl: "3600",
      });

    if (uploadErr) throw uploadErr;

    // 5) signed URL 7 days
    const { data: signed, error: signErr } = await supabase.storage
      .from(output.bucket)
      .createSignedUrl(output.path, 60 * 60 * 24 * 7);

    if (signErr) throw signErr;

    const mp4Url = signed?.signedUrl || null;

    // 6) insert asset (best-effort)
    try {
      await supabase.from("video_assets").insert({
        job_id: jobId,
        asset_type: "video_mp4",
        provider: "ffmpeg-worker",
        status: "completed",
        storage_bucket: output.bucket,
        storage_path: output.path,
        public_url: mp4Url,
        meta: { engine: "ffmpeg_stillimage_v2", size: "1080x1080", fps: 30 },
      });
    } catch {}

    return res.json({ ok: true, mp4Url });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e?.message || "internal_error" });
  } finally {
    // cleanup best-effort
    try {
      if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("idive-video-worker listening on port", PORT);
});