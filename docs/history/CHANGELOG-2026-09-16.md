# Changelog operativo — 2026-09-16

## fMP4 packet-copy: publish queue eliminata

Abbiamo corretto il percorso di early upload del worker. Prima il worker
aspettava fino a due secondi che l’early upload terminasse **prima** di inviare
`TaskOutputDeclared`; questo trasformava la pubblicazione in:

```text
render → attesa → declare → upload
```

Ora il worker:

1. riceve l’`EarlyUploadID` senza attendere il completamento;
2. dichiara subito l’output e lega la sessione al Master;
3. lascia terminare l’upload in parallelo;
4. riusa il risultato early senza trasferire nuovamente il file.

Commit: `b93492e5` — `fix(worker): declare artifacts before early upload completion`.

File principali:

- `RemoteCodex/native/worker-agent-go/internal/worker/active_task_publish.go`
- `RemoteCodex/native/worker-agent-go/internal/worker/active_task_early_upload.go`

## Verifica codice e rilascio

- `go test ./...` verde nell’intero modulo `worker-agent-go`.
- Worker image release completata e certificata dal workflow GitHub.
- Digest rilasciato: `sha256:82ccdfdc331ea58b0b3e27a0dd3fd1d95f9a21fee1e185b90f3b37db0eceb884`.
- Tutti i quattro worker sono aggiornati, `HEALTHY`, versione `v1.4.33`.
- Repository sincronizzata: `HEAD == origin/main == b93492e5`.

Gate flotta:

```text
ready=4
disabled=0
misconfigured=0
transport_failures=0
```

## Job reale producer → Master → worker remoto

Job: `job_fcaf8aba3dfe35bb`

- Worker: `host_57_131_20_173`
- Piano: `output.profile_id=velox-h264-fmp4-stream-v1`
- Asset scaricati a runtime: `13`
- Byte scaricati a runtime: `26.821.233`
- Wall time completo: `1.090 ms`
- Native pipeline/render: `180 ms`
- `publish_queue_wait`: **`11 ms`**; prima era circa `2.100 ms`
- Publish: `609 ms`
- Upload/finalizzazione remota: circa `549/56 ms`
- `concat_mode=packet_copy`
- `packet_copy_ratio=100`
- `encode_passes=0`
- `segments_total=12`
- `segments_packet_copy=12`
- `segments_reencoded=0`
- `audio_packet_copy=1`
- Output: `26.825.233` byte
- SHA256: `66f6b34f1238f1c21ad8b0ffbf78c36f1d324a36e23ab30b23ef5fdd47db6799`
- Box `moof`: `43`
- `safe_offset_bytes > 0` osservato prima di `finalized=true`
- Nessun evento di backward seek osservato nel log worker
- Disk read: `0`; disk write: circa `26,85 MB`
- CPU time: `616 ms`

Il piano ha riusato la stessa sessione early (`upload_id` identico) e il log
mostra `early upload reusable at publish`: non è stato eseguito un secondo
upload completo.

Evidenza locale non sensibile:

- `.velox/fmp4-job-admin-b93492e5-20260916.json`
- `.velox/fmp4-worker-job-b93492e5-20260916.log`
- `.velox/fmp4-final-artifact-b93492e5-20260916.mp4`

## Cosa non è ancora chiuso

L’early upload è ora realmente avviato e riusato, ma l’overlap misurato è
ancora:

```text
progressive_overlap_ms=0
progressive_parts_before_render=0
```

Il mux produce i primi watermark (`28`, `16.798.560`, `26.825.233` byte) troppo
vicino alla fine del render perché una parte upload termini prima della
finalizzazione. La coda di publish è stata risolta; il prossimo problema è la
granularità temporale con cui il native sink rende disponibili i fragment.

## Prossimi punti di battaglia

### 1. Overlap effettivo fragment → upload

- pubblicare `safe_offset` a ogni fragment `moof` realmente completato;
- ridurre la latenza tra `moof/mdat` e `UploadPart`;
- portare `progressive_parts_before_render` e `progressive_overlap_ms` sopra zero;
- mantenere il fallback corretto quando il piano early non arriva.

### 2. Benchmark di concorrenza

Misurare per worker `1`, `4`, `8` e `16` job concorrenti, registrando:

- video/minuto e p50/p95;
- banda input/output;
- CPU, RSS, filesystem e connessioni;
- publisher queue wait;
- errori, retry e checksum mismatch.

Non assumere throughput lineare: il limite può spostarsi su rete, Master,
storage remoto o cache asset.

### 3. Job lunghi reali

Eseguire benchmark con video da `20 minuti`, `1 ora` e `3 ore`, sia warm sia
cold. Il percorso packet-copy dovrebbe scalare soprattutto con byte, numero di
packet e fragment, non con frame decode/encode.

### 4. Eliminazione dello scratch dal percorso caldo

Evolvere il sink da:

```text
libav → .partial → uploader
```

a:

```text
libav/AVIO → bounded native buffer → uploader
```

Prima con buffer bounded e recovery; poi valutare buffer pool nativo,
multipart pipelining e connessioni persistenti.

### 5. Ottimizzazioni native avanzate solo dopo il profiling

`io_uring`, `splice/vmsplice`, buffer pinned e zero-copy hanno senso solo dopo
aver dimostrato che il limite è ancora CPU/syscall/memcpy. Il native mux attuale
è già nell’ordine dei millisecondi; il prossimo guadagno principale è nel
movimento dei byte e nella concorrenza.

## Stato finale della giornata

Il motore packet-copy è passato da una pipeline con circa due secondi di
attesa artificiale a una pubblicazione con queue wait nell’ordine dei
millisecondi. Il prossimo obiettivo non è più “rendere veloce il renderer”, ma
portare il sistema verso:

```text
tempo totale ≈ max(download, upload) + jitter
```

con throughput sostenibile sotto carico e metriche live verificabili.
