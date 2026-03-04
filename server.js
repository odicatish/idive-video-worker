// server.js
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

function getFetch() {
  const f = globalThis.fetch;
  if (typeof f !== "function") {
    throw new Error("fetch is not a function (Node runtime has no global fetch)");
  }
  return f;
}

function getSupabaseAdmin() {
  const url = requireEnv("SUPABASE_URL");
  const key = requireEnv("SUPABASE_SERVICE_ROLE_KEY");

  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

app.get("/", (req, res) => {
  res.json({ ok: true, service: "idive-video-worker" });
});

app.post("/render-mp4", async (req, res) => {
  try {
    const secret = (req.headers.authorization || "").replace("Bearer ", "").trim();
    if (secret !== requireEnv("VIDEO_RENDERER_SECRET")) {
      return res.status(401).json({ error: "unauthorized" });
    }

    const { jobId, audioUrl, output } = req.body;
    if (!jobId || !audioUrl || !output?.bucket || !output?.path) {
      return res.status(400).json({ error: "missing_params" });
    }

    const supabase = getSupabaseAdmin();

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "idive-video-"));
    const audioPath = path.join(tmpDir, "audio.mp3");
    const videoPath = path.join(tmpDir, "video.mp4");

    // download audio (Node 22: use global fetch)
    const audioResp = await getFetch()(audioUrl);
    if (!audioResp.ok) {
      return res.status(400).json({ error: `audio_download_failed_${audioResp.status}` });
    }
    const audioBuffer = Buffer.from(await audioResp.arrayBuffer());
    fs.writeFileSync(audioPath, audioBuffer);

    // render simple video (black background + audio)
    await new Promise((resolve, reject) => {
      ffmpeg()
        .input("color=c=black:s=1080x1080:d=30")
        .inputFormat("lavfi")
        .input(audioPath)
        .outputOptions(["-c:v libx264", "-c:a aac", "-shortest", "-pix_fmt yuv420p"])
        .save(videoPath)
        .on("end", resolve)
        .on("error", reject);
    });

    const videoBuffer = fs.readFileSync(videoPath);

    // upload mp4
    const { error: uploadErr } = await supabase.storage
      .from(output.bucket)
      .upload(output.path, videoBuffer, {
        contentType: "video/mp4",
        upsert: true,
      });

    if (uploadErr) throw uploadErr;

    // signed URL 7 days
    const { data: signed, error: signErr } = await supabase.storage
      .from(output.bucket)
      .createSignedUrl(output.path, 60 * 60 * 24 * 7);

    if (signErr) throw signErr;

    const mp4Url = signed?.signedUrl || null;

    // insert asset (best-effort)
    try {
      await supabase.from("video_assets").insert({
        job_id: jobId,
        asset_type: "video_mp4",
        provider: "ffmpeg-worker",
        status: "completed",
        storage_bucket: output.bucket,
        storage_path: output.path,
        public_url: mp4Url,
        meta: { engine: "ffmpeg_v1" },
      });
    } catch {}

    res.json({ ok: true, mp4Url });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e?.message || "internal_error" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("idive-video-worker listening on port", PORT);
});