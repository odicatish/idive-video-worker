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
  const url =
    (process.env.SUPABASE_URL || "").trim() ||
    (process.env.NEXT_PUBLIC_SUPABASE_URL || "").trim();
  const key = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();

  if (!url) throw new Error("Missing SUPABASE_URL");
  if (!key) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

function getBearerToken(req) {
  const h = (req.headers.authorization || "").trim();
  const m = h.match(/^Bearer\s+(.+)$/i);
  return (m?.[1] || "").trim();
}

function guessImageExt(contentType, fallbackUrl = "") {
  const ct = String(contentType || "").toLowerCase();

  if (ct.includes("png")) return ".png";
  if (ct.includes("jpeg") || ct.includes("jpg")) return ".jpg";
  if (ct.includes("webp")) return ".webp";

  const clean = String(fallbackUrl || "").split("?")[0].toLowerCase();
  if (clean.endsWith(".png")) return ".png";
  if (clean.endsWith(".jpg") || clean.endsWith(".jpeg")) return ".jpg";
  if (clean.endsWith(".webp")) return ".webp";

  return ".png";
}

function parseStorageRef(raw) {
  const value = String(raw || "").trim();
  if (!value) return null;

  if (/^https?:\/\//i.test(value)) {
    return { type: "url", url: value };
  }

  const cleaned = value.replace(/^\/+/, "");

  if (cleaned.startsWith("storage/v1/object/public/")) {
    const rest = cleaned.replace(/^storage\/v1\/object\/public\//, "");
    const parts = rest.split("/").filter(Boolean);
    if (parts.length >= 2) {
      return {
        type: "storage",
        bucket: parts[0],
        objectPath: parts.slice(1).join("/"),
      };
    }
  }

  if (cleaned.startsWith("storage/v1/object/sign/")) {
    const rest = cleaned.replace(/^storage\/v1\/object\/sign\//, "");
    const parts = rest.split("/").filter(Boolean);
    if (parts.length >= 2) {
      return {
        type: "storage",
        bucket: parts[0],
        objectPath: parts.slice(1).join("/"),
      };
    }
  }

  const parts = cleaned.split("/").filter(Boolean);

  if (parts.length >= 2 && ["presenters", "renders", "images"].includes(parts[0])) {
    return {
      type: "storage",
      bucket: parts[0],
      objectPath: parts.slice(1).join("/"),
    };
  }

  return {
    type: "storage",
    bucket: "presenters",
    objectPath: cleaned,
  };
}

async function getPresenterImageUrl(supabase, jobId) {
  const { data: pipelineJob, error: jobErr } = await supabase
    .from("video_render_jobs")
    .select("id,presenter_id")
    .eq("id", jobId)
    .maybeSingle();

  if (jobErr) throw new Error(`pipeline_job_load_failed: ${jobErr.message}`);
  if (!pipelineJob?.presenter_id) throw new Error("pipeline_presenter_missing");

  const { data: presenter, error: presenterErr } = await supabase
    .from("presenters")
    .select("id,image_path")
    .eq("id", pipelineJob.presenter_id)
    .maybeSingle();

  if (presenterErr) throw new Error(`presenter_load_failed: ${presenterErr.message}`);
  if (!presenter?.image_path) throw new Error("presenter_image_path_missing");

  const ref = parseStorageRef(presenter.image_path);
  if (!ref) throw new Error("presenter_image_ref_invalid");

  if (ref.type === "url") {
    return ref.url;
  }

  const { data: signed, error: signErr } = await supabase.storage
    .from(ref.bucket)
    .createSignedUrl(ref.objectPath, 60 * 60);

  if (signErr) throw new Error(`presenter_image_sign_failed: ${signErr.message}`);
  if (!signed?.signedUrl) throw new Error("presenter_image_signed_url_missing");

  return signed.signedUrl;
}

async function downloadToFile(url, outPath) {
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`download_failed_${resp.status}`);
  }

  const contentType = resp.headers.get("content-type") || "";
  const buffer = Buffer.from(await resp.arrayBuffer());
  fs.writeFileSync(outPath, buffer);

  return { contentType, size: buffer.length };
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
    const videoPath = path.join(tmpDir, "video.mp4");

    // 1) download audio
    const audioResp = await fetch(audioUrl);
    if (!audioResp.ok) {
      return res.status(400).json({ error: `audio_download_failed_${audioResp.status}` });
    }
    const audioBuffer = Buffer.from(await audioResp.arrayBuffer());
    fs.writeFileSync(audioPath, audioBuffer);

    // 2) resolve presenter image from job -> presenter -> image_path
    const presenterImageUrl = await getPresenterImageUrl(supabase, jobId);
    const imageExt = guessImageExt("", presenterImageUrl);
    const stillPath = path.join(tmpDir, `presenter${imageExt}`);

    const imageMeta = await downloadToFile(presenterImageUrl, stillPath);

    // 3) render mp4 from presenter still image + audio
    await new Promise((resolve, reject) => {
      ffmpeg()
        .input(stillPath)
        .inputOptions(["-loop 1", "-framerate 30"])
        .input(audioPath)
        .outputOptions([
          "-c:v libx264",
          "-tune stillimage",
          "-r 30",
          "-vf scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,format=yuv420p",
          "-pix_fmt yuv420p",
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
        meta: {
          engine: "ffmpeg_presenter_still_v1",
          size: "1080x1920",
          fps: 30,
          sourceImageContentType: imageMeta.contentType || null,
          sourceImageBytes: imageMeta.size || null,
        },
      });
    } catch {}

    return res.json({
      ok: true,
      mp4Url,
      debug: {
        source: "presenter_image",
        presenterImageUrlResolved: true,
        outputPath: output.path,
      },
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e?.message || "internal_error" });
  } finally {
    try {
      if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("idive-video-worker listening on port", PORT);
});