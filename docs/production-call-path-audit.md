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

## Audit utilizzo endpoint legacy InstaEdit — 2026-08-03

La route è ancora montata su `main`:

```text
POST /api/v1/velox/jobs → velox.createJob → SubmitLegacy
```

La route canonica è separata:

```text
POST /api/v1/jobs → velox.createCanonicalJob → SubmitCanonical
```

La verifica è stata quindi mantenuta non distruttiva: **la route legacy non è
stata rimossa** e nessun codice di compatibilità è stato modificato.

### Metrica individuata

La metrica richiesta esiste nel repository InstaEdit:

```text
InstaeditLogin/pkg/metrics/legacy_jobs.go
legacy_job_endpoint_usage_total{endpoint, outcome}
```

Il call site è in:

```text
InstaeditLogin/pkg/api/velox/jobs_handlers.go:createJob
```

La richiesta viene registrata sempre; gli outcome ammessi includono:
`accepted`, `auth_error`, `bad_request`, `validation_error`,
`upstream_error` e `workspace_mismatch`. Il test locale
`TestCreateJob_UsesSubmissionAdapterAndRecordsUsage` verifica l'incremento
della serie `endpoint="/api/v1/velox/jobs", outcome="accepted"`.

### Verifica degli ultimi 7 giorni

**Esito: NON VERIFICABILE in questa sessione.** Non è stata fornita una URL
Prometheus operativa né credenziali per il metrics endpoint di InstaEdit; le
probe locali non hanno trovato un Prometheus su `127.0.0.1:9090` o
`127.0.0.1:9091`. Il codice configura l'esposizione InstaEdit tramite:

```text
GET /api/v1/metrics        (Basic Auth)
METRICS_PORT > 0           → listener /metrics separato, default 127.0.0.1
```

La query obbligatoria da eseguire sulla sorgente Prometheus di produzione è:

```promql
sum by (outcome) (
  increase(
    legacy_job_endpoint_usage_total{
      endpoint="/api/v1/velox/jobs"
    }[7d]
  )
)
```

La condizione per procedere in sicurezza è che la riga `outcome="accepted"`
abbia valore `0`; le altre righe servono a identificare richieste fallite o
client ancora attivi. Per interrogare direttamente soltanto l'outcome
rilevante si può usare:

```promql
sum(
  increase(
    legacy_job_endpoint_usage_total{
      endpoint="/api/v1/velox/jobs",
      outcome="accepted"
    }[7d]
  )
)
```

Un risultato vuoto non equivale automaticamente a `0`: va verificata anche
l'esistenza della serie e la sua data di raccolta, ad esempio con:

```promql
count(
  legacy_job_endpoint_usage_total{
    endpoint="/api/v1/velox/jobs",
    outcome="accepted"
  }
)
```

Non è corretto trasformare l'assenza di accesso a Prometheus in
`accepted = 0`: l'esito documentato è un **blocco operativo per mancanza di
dati storici**, non traffico zero. Servono l'URL Prometheus, l'intervallo
UTC della query e l'output JSON/CSV della risposta per chiudere questa
verifica.

### Stato Git della verifica

La verifica è stata eseguita su `main` allineato a `origin/main`, con working
tree già sporca in entrambi i repository. Le modifiche preesistenti non sono
state sovrascritte; questa sezione documenta soltanto l'audit e non include
la route né i file applicativi nel perimetro di rimozione.


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
