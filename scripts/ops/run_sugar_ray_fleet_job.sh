#!/usr/bin/env bash
set -u

MASTER=${VELOX_MASTER_URL:-}
[ -n "$MASTER" ] || MASTER=http://127.0.0.1:8000
MASTER=$(printf '%s' "$MASTER" | sed 's:/*$::')
ADMIN_TOKEN=${VELOX_ADMIN_TOKEN:-}
[ -n "$ADMIN_TOKEN" ] || ADMIN_TOKEN=$(sudo sed -n 's/^VELOX_ADMIN_TOKEN=//p' /etc/velox-server.env | head -1)

DESTINATION_ID=${SUGAR_RAY_DESTINATION_ID:-}
if [[ -z "$DESTINATION_ID" || "$DESTINATION_ID" != instaedit_* ]]; then
  echo "FATAL: SUGAR_RAY_DESTINATION_ID must be an Instaedit destination (instaedit_<external_id>)" >&2
  exit 2
fi

SCENE0="Le luci ad anello illuminano una figura che si muove con una grazia quasi impossibile, mostrando un pugile il cui atletismo era leggendario. Lo vediamo impegnato in sparring intenso, i suoi movimenti fluidi e potenti mentre scambia colpi con un avversario. Il focus è chiaramente sul puro dinamismo del suo stile di combattimento; ogni jab, gancio e aggiustamento dei piedi appare calcolato ma senza sforzo. Dimostra una padronanza della distanza, sembrando controllare il ritmo dello scambio semplicemente spostando il peso o angolando il corpo abbastanza da evitare l'impatto. Questo segmento cattura non solo la lotta, ma anche l'arte dietro l'aggressività, suggerendo anni di rigoroso allenamento che culminano in questa esplosiva dimostrazione di abilità sotto luci intense. La telecamera lo segue attraverso un periodo di intensa attività, catturando primi piani che enfatizzano sia il tributo fisico che lo spirito duraturo dell'atleta. Il sudore luccica sulla sua pelle mentre continua a lavorare sull'avversario, mantenendo un alto livello di output anche quando visibilmente stanco. Ci sono momenti in cui sembra fare una pausa, quasi osservando la scena prima di esplodere nuovamente in azione, suggerendo una mente tattica al lavoro dietro i pugni. L'interazione è incessante; è una testimonianza visiva del picco delle condizioni umane che incontra il fuoco competitivo, non lasciando dubbi sul livello di talento mostrato in questo angolo del ring. Infine, le riprese cambiano leggermente, mostrando forse lui che interagisce con i preparatori o semplicemente camminando nella struttura dopo che l'azione principale è sbollentata. Mentre l'adrenalina immediata svanisce, la sua presenza rimane imponente. Lo si vede scambiare parole e gesti che suggeriscono profonda cameratismo con chi lo circonda, un contrasto con la violenza cruda dei momenti precedenti. L'impressione generale lasciata da questi scatti conclusivi è quella di un'eredità duratura; la prodezza fisica mostrata nella lotta lascia spazio a un'aura di professionalità vissuta e fiducia tranquilla, segnando la fine di una performance ma accennando a una carriera costruita su eccellenza sostenuta."
SCENE1="Questo segmento cattura non solo la lotta, ma anche l'arte dietro l'aggressività, suggerendo anni di rigoroso allenamento che culminano in questa esplosiva dimostrazione di abilità sotto luci intense. La telecamera lo segue attraverso un periodo di intensa attività, catturando primi piani che enfatizzano sia il tributo fisico che lo spirito duraturo dell'atleta. Il sudore luccica sulla sua pelle mentre continua a lavorare sull'avversario, mantenendo un alto livello di output anche quando visibilmente stanco. Ci sono momenti in cui sembra fare una pausa, quasi osservando la scena prima di esplodere nuovamente in azione, suggerendo una mente tattica al lavoro dietro i pugni."
SCENE2="L'interazione è incessante; è una testimonianza visiva del picco delle condizioni umane che incontra il fuoco competitivo, non lasciando dubbi sul livello di talento mostrato in questo angolo del ring. Infine, le riprese cambiano leggermente, mostrando forse lui che interagisce con i preparatori o semplicemente camminando nella struttura dopo che l'azione principale è sbollentata. Mentre l'adrenalina immediata svanisce, la sua presenza rimane imponente. Lo si vede scambiare parole e gesti che suggeriscono profonda cameratismo con chi lo circonda, un contrasto con la violenza cruda dei momenti precedenti. L'impressione generale lasciata da questi scatti conclusivi è quella di un'eredità duratura; la prodezza fisica mostrata nella lotta lascia spazio a un'aura di professionalità vissuta e fiducia tranquilla, segnando la fine di una performance ma accennando a una carriera costruita su eccellenza sostenuta."

C0=1TwVU-11JCggSBuHtavhKMevMZna-xr51
C1=1g9Ir3FrzdH1u2DmOrzSKVhTm15Mw5Zqn
C2=1S5HWNgy6QYoFQhFdj91Z0XGqC5-szhVG
V0=1dTk02g8X8tkrnGJfTLIB7rjZGvwIwUiG
V1=1OpcibcdSPWUbGafj5AixRZ78g1PBh9nH
V2=1rrGxb1qYwdx4f3szlH2Bbl9hHMQ34j3B
CLIP0="velox-asset://$C0"
CLIP1="velox-asset://$C1"
CLIP2="velox-asset://$C2"
VO0="velox-asset://$V0"
VO1="velox-asset://$V1"
VO2="velox-asset://$V2"

build_payload() {
  idem=$1
  worker=$2
  jq -n \
    --arg idem "$idem" --arg worker "$worker" --arg destination "$DESTINATION_ID" \
    --arg s0 "$SCENE0" --arg s1 "$SCENE1" --arg s2 "$SCENE2" \
    --arg c0 "$C0" --arg c1 "$C1" --arg c2 "$C2" \
    --arg clip0 "$CLIP0" --arg clip1 "$CLIP1" --arg clip2 "$CLIP2" \
    --arg v0 "$V0" --arg v1 "$V1" --arg v2 "$V2" \
    --arg vo0 "$VO0" --arg vo1 "$VO1" --arg vo2 "$VO2" '
{
  job_type: "clip.stock.v1",
  template_id: "sugar-ray-robinson",
  template_version: 1,
  idempotency_key: $idem,
  video_name: "Sugar Ray Robinson: carriera e patrimonio",
  script_text: "Sugar Ray Robinson: carriera e patrimonio",
  placement_pin_worker_id: $worker,
  publications: [{
    publication_id: $idem,
    output_ref: {artifact_role: "final_video"},
    destinations: [{destination_id: $destination, priority: 1, retry_budget: 1}]
  }],
  spec: {
    video_name: "Sugar Ray Robinson: carriera e patrimonio",
    script_text: "Sugar Ray Robinson: carriera e patrimonio",
    delivery_plan: [{destination_id: $destination, priority: 1, retry_budget: 1,
                     metadata:{publication_id:$idem}}],
    scenes: [
      {id:"scene-0", index:0, kind:"clip", text:$s0, duration_seconds:5,
       clip:{asset_id:$c0,url:$clip0,duration_ms:5000},
       voiceover:{asset_id:$v0,url:$vo0,language:"it-IT",duration_ms:5000},
       stock:[{asset_id:$c0,url:$clip0,end_ms:5000,duration_ms:5000}],
       stock_fallback:false},
      {id:"scene-1", index:1, kind:"clip", text:$s1, duration_seconds:5,
       clip:{asset_id:$c1,url:$clip1,duration_ms:5000},
       voiceover:{asset_id:$v1,url:$vo1,language:"it-IT",duration_ms:5000},
       stock:[{asset_id:$c1,url:$clip1,end_ms:5000,duration_ms:5000}],
       stock_fallback:false},
      {id:"scene-2", index:2, kind:"clip", text:$s2, duration_seconds:5,
       clip:{asset_id:$c2,url:$clip2,duration_ms:5000},
       voiceover:{asset_id:$v2,url:$vo2,language:"it-IT",duration_ms:5000},
       stock:[{asset_id:$c2,url:$clip2,end_ms:5000,duration_ms:5000}],
       stock_fallback:false}
    ]
  }
}'
}

CLIENT_ID="sugar-ray-job-$(date +%s)-$$"
ISSUE=$(curl -sS -m 20 -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H "Content-Type: application/json" \
  --data "{\"client_id\":\"$CLIENT_ID\",\"description\":\"Sugar Ray Robinson fleet validation\",\"scopes\":[\"jobs.submit\"],\"rate_limit_rps\":5,\"rate_limit_burst\":10,\"quota_max_scenes\":10,\"quota_max_total_secs\":120}" \
  -w '\n%{http_code}' "$MASTER/api/v1/admin/m2m/keys") || exit 3
ISSUE_STATUS=$(printf '%s\n' "$ISSUE" | tail -n1)
ISSUE_BODY=$(printf '%s\n' "$ISSUE" | sed '$d')
[ "$ISSUE_STATUS" = 201 ] || { printf '%s\n' "$ISSUE_BODY"; exit 4; }
M2M=$(printf '%s\n' "$ISSUE_BODY" | jq -er '.plaintext_secret') || exit 5
CLIENT_ID=$(printf '%s\n' "$ISSUE_BODY" | jq -er '.client_id')
echo "M2M client provisioned: $CLIENT_ID"

poll_job() {
  job_id=$1
  worker=$2
  started=$(date +%s)
  last=
  while :; do
    response=$(curl -sS -m 20 -H "Authorization: Bearer $M2M" \
      -w '\n%{http_code}' "$MASTER/api/v1/jobs/$job_id") || return 3
    http=$(printf '%s\n' "$response" | tail -n1)
    body=$(printf '%s\n' "$response" | sed '$d')
    state=$(printf '%s\n' "$body" | jq -r '.status // empty' 2>/dev/null)
    now=$(date +%s)
    shown=$state
    [ -n "$shown" ] || shown=unknown
    if [ "$state" != "$last" ]; then
      echo "[$worker] t=$((now-started))s status=$shown http=$http"
      last=$state
    fi
    case "$state" in
      SUCCEEDED|FAILED|CANCELLED)
        printf '%s\n' "$body" | jq '{status,job_id,worker_id,assigned_to,lease_worker_id,created,started_at,completed_at,render_time_ms,artifact_url,artifact_path,output_path,error}'
        [ "$state" = SUCCEEDED ]
        return
        ;;
    esac
    [ $((now-started)) -lt 900 ] || { echo "[$worker] timeout"; return 7; }
    sleep 5
  done
}

for WORKER in velox-worker-13197 velox-worker-523925eb host_57_129_132_133 host_57_131_20_173; do
  IDEM="sugar-ray-robinson-$(printf '%s' "$WORKER" | tr '_' '-')-$(date +%s%N)"
  echo
  echo "===== submit worker=$WORKER ====="
  PAYLOAD=$(build_payload "$IDEM" "$WORKER")
  RESPONSE=$(curl -sS -m 30 -X POST \
    -H "Authorization: Bearer $M2M" -H "Content-Type: application/json" \
    -H "X-Request-ID: $IDEM" --data-raw "$PAYLOAD" \
    -w '\n%{http_code}' "$MASTER/api/v1/jobs") || {
      echo "[$WORKER] submit network failure"
      continue
    }
  HTTP=$(printf '%s\n' "$RESPONSE" | tail -n1)
  BODY=$(printf '%s\n' "$RESPONSE" | sed '$d')
  if [ "$HTTP" != 202 ]; then
    echo "[$WORKER] submit HTTP $HTTP"
    printf '%s\n' "$BODY" | jq . 2>/dev/null || printf '%s\n' "$BODY"
    continue
  fi
  JOB_ID=$(printf '%s\n' "$BODY" | jq -er '.job_id') || continue
  echo "[$WORKER] accepted job_id=$JOB_ID"
  poll_job "$JOB_ID" "$WORKER" || true
done

curl -sS -m 10 -X DELETE -H "Authorization: Bearer $ADMIN_TOKEN" \
  "$MASTER/api/v1/admin/m2m/keys/$CLIENT_ID" >/dev/null 2>&1 || true
echo "fleet job validation complete"
