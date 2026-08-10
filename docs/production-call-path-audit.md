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
4. `DataServer/internal/handlers/web/proxy/drive.go` era un proxy HTTP
   diretto con `http.Client.Do` e body non bounded. Il call-site inventory del
   2026-08-10 ha confermato zero caller/import e nessuna route che lo montasse:
   le route `/api/drive/*` attive appartengono al modulo canonico
   `internal/handlers/server/drive`. Il proxy è stato quindi rimosso insieme
   al setting inutilizzato `VELOX_JOB_MASTER_URL`; nessun percorso Drive
   canonico è stato modificato.

## Credential lease

Nel bootstrap sono registrati `drive` e `social_gateway`; entrambi implementano
`CredentialLeaseProvider` e `CredentialScopeProvider`. Il runner emette una
lease prima della consegna e, quando il provider implementa il contratto,
chiama `DeliverWithCredential`.

La chiusura non è completa: `DeliveryRunner` conserva il
fallback a
`Provider.Deliver`, il bootstrap non rifiuta un provider sensibile privo di
lease e sia `DriveProvider.Deliver` sia `SocialGatewayProvider.Deliver` restano
superfici monolitiche alternative. `LocalExportProvider` ha solo `Deliver` e
non è registrato nel bootstrap corrente; l'implementazione S3 non fa parte del
runtime né del contratto attuale.

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
- backup: il call-site proof del 2026-08-10 ha trovato solo primitive isolate
  in `DataServer/internal/backup`, senza import, caller, scheduler, bootstrap,
  CLI o restore operativo collegato. Il package e i test sono stati rimossi
  come scaffolding irraggiungibile; il requisito residuo appartiene a
  platform/operations e resta tracciato in `FUTURE.md` e nel runbook backup;
- `audittrail.AppendAuditEvent`: schema e repository presenti, ma nessun
  chiamante produttivo per gli eventi di job/delivery/publication richiesti.

## Audit rimozione `internal/backup` — 2026-08-10

Il proof completo è stato eseguito sul modulo `DataServer` prima della
rimozione:

- `git grep` non ha trovato import di `internal/backup` né chiamate a
  `BackupSQLite`, `RestoreSQLite`, `VerifySQLite`, `BackupEncryptedSQLite`,
  `RestoreEncryptedSQLite` o `RestoreTest` fuori dal package;
- `go list ./...` ha mostrato il package come nodo senza reverse importer;
- non esistono route, bootstrap registration, supervisor runner, scheduler,
  comando CLI, script operativo o configurazione che lo raggiungano;
- i soli test erano test white-box del package e passavano esclusivamente con
  `go test ./internal/backup`;
- `scripts/ci/sql-baseline.txt` conteneva soltanto le due righe LOC del
  production code e non rappresentava un consumer.

Decisione: RIMOSSO. Il package era scaffolding non raggiungibile, non un
contratto cross-package attivo. Il package, i test e il baseline LOC sono stati
rimossi insieme nel commit atomico della pulizia. Il requisito di
backup/restore non viene considerato implementato: ownership e criteri futuri
sono documentati in
`docs/operations/backup-and-restore.md` sotto platform/operations.

La verifica completa è stata eseguita con `scripts/ci/pre-removal-verify.sh`
sia su un worktree pulito baseline sia sul worktree con la rimozione. In
entrambi i casi `go build ./...` passa; `go vet ./...` e `go test -count=1
./...` riportano lo stesso failure preesistente in
`internal/store/store_publication_state_async_test.go:101,104`
(`undefined: errors`), senza alcun riferimento a `internal/backup` e senza
variazioni introdotte dalla patch. Il failure è registrato come follow-up
separato; la rimozione è stata accettata sulla prova di non-regressione e sui
test mirati del package prima della cancellazione.

## Audit rimozione endpoint legacy InstaEdit — 2026-08-03

La rimozione è stata autorizzata dopo la conferma operativa di traffico zero
per l'outcome `accepted` negli ultimi sette giorni. Il percorso precedente:

```text
POST /api/v1/velox/jobs → createJob → SubmitLegacy
```

è stato eliminato da `InstaeditLogin`. Il percorso supportato resta:

```text
POST /api/v1/jobs → createCanonicalJob → SubmitCanonical
```

Rimossi dal repository InstaEdit:

```text
POST /api/v1/velox/jobs
createJob
SubmitLegacy
adaptLegacyRequest
validateLegacyRequest
SubmissionResult.Legacy
legacy_job_endpoint_usage_total
```

Il GET `/api/v1/velox/jobs` e le operazioni di lettura/cancellazione per ID
non erano parte della rimozione e restano disponibili.

La suite aggiornata verifica sia l'assenza del metodo POST legacy
(`405 Method Not Allowed` sul path condiviso con il GET), sia il percorso BFF
reale verso il client Velox con i campi canonici:
`job_type`, `template_id`, `template_version`, `video_name`, `spec` e `output`.

### Evidenza e limite dell'audit

Nel precedente audit locale la sorgente Prometheus non era raggiungibile, per
cui il repository non contiene un campione storico allegato. La decisione di
rimozione è stata presa sulla conferma operativa esplicita del traffico zero,
non sull'interpretazione di una serie assente come valore zero. La metrica e
il suo test sono stati rimossi insieme alla superficie che misuravano; eventuali
snapshot Prometheus storici restano responsabilità dell'osservability store.

### Stato Git della rimozione

La rimozione è stata eseguita su `main`. Le modifiche applicative locali
preesistenti negli altri file dei repository erano già presenti e non sono
state sovrascritte né incluse automaticamente nel commit della rimozione.


## Audit migrazione contratto delivery — 2026-08-03

La verifica dei client Velox ha individuato un solo producer del DTO di
consegna `DeliverArtifactRequest`: `SocialGatewayProvider.buildRequest`, ora
vincolato alla costante `socialclient.ContractVersionDelivery`, pari a
`velox.delivery.v1`. Il client `socialclient` rifiuta prima dell’invio HTTP
la versione vuota, `velox-instaedit.v1`, `velox.job.v1`, versioni obsolete o
valori sconosciuti. I test wire verificano inoltre che il producer invii
sempre la versione canonica.

Nel tree di `main` il ramo server legacy e i simboli richiesti
(`VeloxDeliverContractRequest`, `validateContractRequest`,
`synthesizeContractDeliveryID`, `synthesizeContractDestinationID`,
`isContractPath`) risultavano già assenti; non è stata quindi inventata una
rimozione di codice non presente. La modifica chiude il lato Velox con una
sola DTO, una validazione e una persistence path verso
`POST /internal/v1/deliveries`.

I riferimenti `velox.instaedit.publish.v1` nei job metadata e
`velox.delivery.event.v1` nei callback restano invariati: sono contratti
separati e non rappresentano la versione dell’envelope delivery.

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
