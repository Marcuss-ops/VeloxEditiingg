package videos

import (
	"html/template"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// UploadFormPage serves a minimal HTML UI for uploading a video.
// This is intentionally backend-rendered to avoid depending on the SPA build.
// GET /youtube/upload
func (h *Handler) UploadFormPage(c *gin.Context) {
	channels := h.svc.GetChannels()

	var b strings.Builder
	b.WriteString(`<!doctype html><html lang="it"><head><meta charset="utf-8" />`)
	b.WriteString(`<meta name="viewport" content="width=device-width, initial-scale=1" />`)
	b.WriteString(`<title>YouTube Upload</title>`)
	b.WriteString(`<style>
body{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial;max-width:900px;margin:32px auto;padding:0 16px;background:#0b1220;color:#e5e7eb}
h1{font-size:22px;margin:0 0 8px 0}
.muted{color:#9ca3af;font-size:13px}
form{margin-top:18px;background:#0f172a;border:1px solid rgba(255,255,255,.10);border-radius:14px;padding:16px}
label{display:block;margin:10px 0 6px 0;color:#cbd5e1;font-size:13px}
input,select,textarea{width:100%;box-sizing:border-box;background:#0b1220;color:#e5e7eb;border:1px solid rgba(255,255,255,.12);border-radius:10px;padding:10px 12px}
textarea{min-height:110px;resize:vertical}
.row{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.btn{margin-top:14px;display:inline-flex;align-items:center;gap:8px;background:#ef4444;color:#111827;border:0;border-radius:12px;padding:10px 14px;font-weight:700;cursor:pointer}
.btn:disabled{opacity:.55;cursor:not-allowed}
pre{white-space:pre-wrap;word-break:break-word;background:#0b1220;border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:12px;margin-top:12px}
a{color:#93c5fd}
</style></head><body>`)

	b.WriteString(`<h1>Upload Video su YouTube</h1>`)
	b.WriteString(`<div class="muted">Questa pagina invia il file a <code>/api/v1/youtube/upload</code>. Se un canale fallisce, il token potrebbe essere revocato.</div>`)

	b.WriteString(`<form id="f">`)
	b.WriteString(`<div class="muted" style="margin:10px 0 0 0">
<button class="btn" id="reauth" type="button" style="background:#22c55e;color:#052e16">Ri-autorizza canale</button>
<span id="reauthStatus" style="margin-left:10px"></span>
</div>`)
	b.WriteString(`<label>Video (mp4/mov...)</label><input name="video" type="file" required />`)
	b.WriteString(`<label>Canale</label><select name="channel_id" required>`)
	b.WriteString(`<option value="" selected>Seleziona...</option>`)
	for _, ch := range channels {
		// Skip channels that don't have refresh token: uploads will fail when access token expires.
		if strings.TrimSpace(ch.RefreshToken) == "" {
			continue
		}
		id := template.HTMLEscapeString(ch.ID)
		name := ch.Title
		if strings.TrimSpace(name) == "" {
			name = ch.Name
		}
		if strings.TrimSpace(name) == "" {
			name = ch.ID
		}
		label := template.HTMLEscapeString(name)
		b.WriteString(`<option value="` + id + `">` + label + ` (` + id + `)</option>`)
	}
	b.WriteString(`</select>`)

	b.WriteString(`<div class="row">`)
	b.WriteString(`<div><label>Titolo</label><input name="title" type="text" placeholder="Titolo video" /></div>`)
	b.WriteString(`<div><label>Privacy</label><select name="privacy"><option value="private" selected>private</option><option value="unlisted">unlisted</option><option value="public">public</option></select></div>`)
	b.WriteString(`</div>`)

	b.WriteString(`<label>Descrizione</label><textarea name="description" placeholder="Descrizione..."></textarea>`)
	b.WriteString(`<label>Tags (separate da virgola)</label><input name="tags" type="text" placeholder="tag1,tag2,tag3" />`)

	b.WriteString(`<button class="btn" id="btn" type="submit">Carica</button>`)
	b.WriteString(`<pre id="out" style="display:none"></pre>`)
	b.WriteString(`</form>`)

	b.WriteString(`<script>
const f = document.getElementById('f');
const btn = document.getElementById('btn');
const out = document.getElementById('out');
const reauth = document.getElementById('reauth');
const reauthStatus = document.getElementById('reauthStatus');

function randName() {
  return 'ch_' + Math.random().toString(36).slice(2, 10);
}

reauth.addEventListener('click', async () => {
  reauthStatus.textContent = 'Apro OAuth...';
  reauth.disabled = true;
  try {
    const r = await fetch('/api/v1/youtube/oauth/start?channel_name=' + encodeURIComponent(randName()));
    const data = await r.json();
    if (!r.ok || !data || !data.auth_url) {
      reauthStatus.textContent = 'Errore OAuth start';
      return;
    }
    reauthStatus.textContent = 'Completa il login nella finestra popup...';
    window.open(data.auth_url, 'yt_oauth', 'popup=yes,width=520,height=720');
  } catch (e) {
    reauthStatus.textContent = String(e);
  } finally {
    reauth.disabled = false;
  }
});

window.addEventListener('message', (ev) => {
  const msg = ev && ev.data;
  if (!msg || msg.type !== 'youtube_auth_success') return;
  reauthStatus.textContent = 'Autenticazione OK. Ricarico la pagina...';
  setTimeout(() => location.reload(), 600);
});

f.addEventListener('submit', async (e) => {
  e.preventDefault();
  out.style.display = 'block';
  out.textContent = 'Uploading...';
  btn.disabled = true;
  try {
    const fd = new FormData(f);
    const r = await fetch('/api/v1/youtube/upload', { method: 'POST', body: fd });
    const txt = await r.text();
    let data = null;
    try { data = JSON.parse(txt); } catch {}
    if (!r.ok) {
      out.textContent = 'ERROR ' + r.status + '\\n' + (data ? JSON.stringify(data, null, 2) : txt) + '\\n\\nSuggerimento: prova "Ri-autorizza canale" se il token e scaduto.';
      return;
    }
    out.textContent = data ? JSON.stringify(data, null, 2) : txt;
    const url = data && data.result && (data.result.youtube_url || data.result.YouTubeURL);
    if (url) out.innerHTML += '\\n\\nLink: <a target=\\"_blank\\" rel=\\"noreferrer\\" href=\\"' + url + '\\">' + url + '</a>';
  } catch (err) {
    out.textContent = String(err);
  } finally {
    btn.disabled = false;
  }
});
</script>`)

	b.WriteString(`</body></html>`)
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(b.String()))
}
