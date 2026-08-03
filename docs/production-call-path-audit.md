# Production call-path audit

Audit eseguito il 2026-08-02 su `main` a `4e807174`.

Questo documento è il Commit 1 della chiusura del percorso produttivo. Non
introduce funzionalità: registra soltanto i percorsi verificati, i bypass e il
punto in cui la state machine deve entrare.

## Percorso verificato

```text
POST /api/v1/jobs
  → pipeline.SubmitJob
  → ResolveRenderManifestRef (se presente)
  → NormalizeExternalJobSubmission
  → creatorflow resolver
  → enqueue.Enqueuer.Enqueue
  → AssetService rewrite parziale
  → AtomicJobTaskCreator: job + delivery plan + publication_states + task
  → worker render
  → artifact upload / FinalizeVerified
  → job_deliveries PENDING
  → DeliveryRunner claim
  → provider.Deliver[WithCredential]
  → job_deliveries result
```

Il percorso di acquisizione asset è realmente collegato soltanto per le
riscritture che `Enqueuer.resolveEnqueueAssets` invoca: voiceover, immagini
scena e segmenti clip temporizzati. `FinalizeVerified` crea le delivery in una
transazione, mentre il `DeliveryRunner` è avviato dal bootstrap e viene
registrato nel supervisor.

## Matrice degli input

| Input | Entrypoint / resolver | Policy verificata | Stato |
|---|---|---|---|
| voiceover top-level e alias legacy | `SubmitJob` → `Enqueuer` → `RewriteVoiceoverPayload` → `ResolveAndRegister` | `inputsecurity.Fetcher.Fetch` / `ValidateFile` / quarantine | PARZIALE: voiceover annidato in `scenes[].voiceover.url` non è raccolto dal rewrite |
| immagini scena | `SubmitJob` → `RewriteSceneImagePayload` → `ResolveAndRegister` | Fetch bounded, MIME sniffing, validazione e quarantine | PARZIALE: copre i campi raccolti, non ogni forma legacy proiettata |
| clip | `clip_segments.source_path` → `RewriteVideoClipSegments` | ffprobe del trimmer e registrazione asset | GAP: un path locale già esistente salta allowlist e `ValidateFile`; temp e copy non sono sotto la policy comune |
| clip URL senza trimming | DTO / normalizzazione → worker payload | nessuna chiamata a `AssetService` individuata | GAP produttivo |
| audio track | `audio_tracks[].source_url` → worker payload | validazione di schema/URL soltanto | GAP produttivo: nessun `ResolveAndRegister` |
| sottotitoli | `scenes[].subtitles` e `subtitle_tracks` → worker payload | validazione di schema/URL soltanto | GAP produttivo: nessun resolver comune |
| font | `layers[].font` / subtitle font → worker payload | nessuna acquisizione centralizzata individuata | GAP produttivo |
| thumbnail | ruolo `assets.RoleThumbnail` e capability publication | nessun entrypoint SubmitJob/asset acquisition produttivo individuato | NON COLLEGATO |
| render manifest | `ResolveRenderManifestRef` → `inputsecurity.Fetcher.Fetch(KindManifest)` | byte limit, hash del manifest, MIME/JSON policy | PARZIALE: il documento è protetto, ma gli asset annidati vengono poi proiettati senza un passaggio unico nel resolver |
| multipart upload | `CreatorAssetUpload` → temp controllata → `ResolveAndRegister` | byte limit reale, directory controllata, validazione, quarantine | VERIFICATO per questo entrypoint |
| `velox-asset://` | `veloxAssetResolver.Open` | `ValidateFile`, quarantine, path derivato dall'asset ID | VERIFICATO quando il resolver viene chiamato; non garantisce i campi che lo bypassano |
| Drive file | `driveResolver.Open` → `DownloadFileWithLimit` quando disponibile | servizio Drive bounded; validazione successiva | GAP di contratto: il fallback `DriveDownloader.DownloadFile` non obbliga il limite configurato |
| URL provider esterni | `socialclient` / Drive API | URL configurati dal server, non asset URL del payload | FUORI DAL RESOLVER ASSET; va mantenuto config-only e validato al bootstrap |

## Bypass diretti rilevati

1. `DataServer/internal/assets/video_segments.go` controlla prima `os.Stat` e,
   se il path esiste, lo considera attendibile senza `allowedLocalPath` né
   `inputsecurity.ValidateFile`. Lo stesso percorso usa `os.CreateTemp("")` e
   `io.Copy` senza limite esplicito.
2. `DataServer/internal/jobs/enqueue/enqueue_phases.go` non riscrive tramite
   `AssetService` clip non temporizzati, audio track, sottotitoli, font o
   thumbnail. La validazione HTTP di schema/SSRF non equivale ad acquisizione,
   MIME sniffing e quarantine.
3. `DataServer/internal/assets/resolvers.go` accetta un'interfaccia Drive che
   può eseguire `DownloadFile` senza il limite `Store.maxBytes`; il servizio
   Drive concreto oggi delega comunque a una variante bounded, ma il contratto
   permette un adapter non bounded.
4. `DataServer/internal/handlers/web/proxy/drive.go` contiene un proxy HTTP
   diretto con `http.Client.Do` e body non bounded. Non è raggiunto dal call
   path SubmitJob individuato; resta candidato a rimozione o a un audit separato
   se una route lo rende pubblico.

## Credential lease

Nel bootstrap sono registrati `drive` e `social_gateway`; entrambi implementano
`CredentialLeaseProvider` e `CredentialScopeProvider`. Il runner emette una
lease prima della consegna e, quando il provider implementa il contratto,
chiama `DeliverWithCredential`.

La chiusura non è completa: `DeliveryRunner` conserva il fallback a
`Provider.Deliver`, il bootstrap non rifiuta un provider sensibile privo di
lease e sia `DriveProvider.Deliver` sia `SocialGatewayProvider.Deliver` restano
superfici monolitiche alternative. `S3Provider` e `LocalExportProvider` hanno
solo `Deliver` e non sono registrati nel bootstrap corrente.

## Punto d’integrazione della publication state machine

`AtomicJobTaskCreator.insertPublicationStatesTx` crea correttamente lo
snapshot iniziale. Tuttavia, una scansione delle chiamate produttive mostra
che `TransitionPublicationState`, `TransitionPublicationPartial`,
`BeginPublicationPhaseEffect` e `CompletePublicationPhaseEffect` sono usati
solo nelle definizioni/store e nei test: nessuno è chiamato da
`DeliveryRunner`.

Il punto corretto di ingresso è `DeliveryRunner.processLease`, dopo:

```text
claim delivery
→ hydrate destination/artifact
→ load publication snapshot
→ issue credential lease
```

e prima della chiamata al provider. Da lì il runner deve determinare la prima
fase non completata, eseguire una sola fase, persistere la transizione e solo
dopo passare alla fase successiva. Il `remote_video_id` deve essere salvato
nella stessa unità di avanzamento subito dopo `UPLOAD_MEDIA`, prima di
metadata/localizzazioni/verifica. L’attuale `Deliver` monolitico non consente
questo resume.

## Componenti scollegati dal percorso produttivo

La ricerca dei call site produttivi ha evidenziato:

- `internal/admission`: `NewController` e `NewFairQueue` non sono chiamati dal
  bootstrap o dall'enqueue reale;
- `internal/quality`: `NextPhase` e `GoldenCases` non sono integrati in un
  render/rollout produttivo;
- osservability `phases/trends` e `regressions`: endpoint registrati, ma non
  necessari al percorso di consegna e da congelare per il blocco corrente;
- backup: primitive di backup/restore presenti, senza scheduler o restore
  operativo collegato;
- `audittrail.AppendAuditEvent`: schema e repository presenti, ma nessun
  chiamante produttivo per gli eventi di job/delivery/publication richiesti.

## Audit rimozione endpoint legacy InstaEdit — 2026-08-03

La verifica richiesta dopo la certificazione canonica ha controllato la route
`POST /api/v1/velox/jobs` e i simboli associati (`SubmitLegacy`,
adaptLegacyRequest, validateLegacyRequest, `SubmissionResult.Legacy`). Nella
working tree di `main` corrente non è presente alcuna di queste route o
implementazioni: il percorso montato è quello canonico sotto
`/api/v1/instaedit/jobs`.

È stata aggiunta una regressione in
`DataServer/cmd/server/router_instaedit_failfast_test.go` che verifica, quando
il gruppo InstaEdit è configurato, la presenza della route canonica e il
mancato montaggio della vecchia POST `/api/v1/velox/jobs`.

La metrica richiesta `legacy_job_endpoint_usage_total` non è presente nel
repository e non è disponibile alcun endpoint Prometheus configurato nella
working tree locale. Di conseguenza non è possibile certificare un valore
`accepted = 0`, né ricostruire traffico storico o dichiarare assenza di
client esterni. Questo è un limite dell'evidenza disponibile, non una
misurazione di traffico zero; l'eventuale verifica residua richiede accesso
alla sorgente Prometheus/telemetria dell'ambiente operativo.

## Sequenza autorizzata dopo questo audit

1. chiudere soltanto i bypass reali della matrice;
2. imporre la lease ai provider sensibili;
3. far caricare a `DeliveryRunner` snapshot, retry checkpoint e side effects;
4. aggiungere un solo registry per `UPLOAD_MEDIA`, `APPLY_METADATA`, `VERIFY`;
5. persistere `VIDEO_CREATED` e il remote ID;
6. aggiungere il test crash/resume che dimostra zero re-upload;
7. soltanto dopo aggiungere localizzazioni e audit transazionale minimo;
8. rimuovere o congelare lo scaffolding scollegato.

Il test di accettazione ancora mancante è: upload riuscito, metadata fallita,
restart del master, resume da metadata e un solo video remoto.
