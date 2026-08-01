# Threat model: input remoti

## Perimetro

Il master tratta come non fidati tutti i byte e gli URL che possono provenire da
un job, da un manifest, da un provider esterno o da un asset già registrato.
Il perimetro comprende clip, immagini, audio, voiceover, sottotitoli, manifest,
font e thumbnail, oltre agli endpoint HTTP dei provider.

Il renderer riceve soltanto asset locali verificati o riferimenti
`velox-asset://`. Non deve risolvere direttamente URL forniti dal client.

## Attori e beni da proteggere

| Attore | Capacità | Bene a rischio |
| --- | --- | --- |
| Client/job malevolo | controlla URL, manifest e metadati | rete interna, filesystem, master |
| Server remoto ostile | controlla redirect, DNS, header e body | worker, renderer, storage temporaneo |
| Provider compromesso | restituisce byte o MIME falsi | pipeline asset e pubblicazione |
| DNS rebinding | cambia la risposta tra validazione e connessione | reti private e metadata service |

## Controlli obbligatori

Il registry centrale è `DataServer/internal/inputsecurity`: `Policy` contiene
limiti, timeout, resolver, directory controllate e quarantine; `Fetcher` è
l’unico percorso per acquisire URL HTTP(S); `ValidateFile` è il confine prima
della registrazione o del renderer.

- `http` e `https` soltanto; niente userinfo, `file://` o schemi arbitrari.
- DNS e dial verificano loopback, privati, link-local, multicast,
  unspecified, CGNAT, benchmarking e metadata cloud.
- Ogni redirect viene rivalidato e il numero è limitato.
- Il body viene letto con `io.LimitReader(MaxBytes+1)`: `Content-Length` non è
  una prova di dimensione.
- DNS, connessione, header, trasferimento e ffprobe hanno timeout distinti.
- MIME viene sniffato dai byte e confrontato con il ruolo richiesto; HTML e
  contenuti corrotti sono rifiutati.
- clip, audio e voiceover passano `ffprobe` senza shell, con protocollo `file`
  soltanto, path locale generato e timeout.
- temporary file, staging e quarantine usano directory e nomi generati dal
  sistema; i path client non sono accettati come staging.
- input sospetti vengono rimossi o spostati in quarantine con metadati limitati
  e senza URL originale.
- i rifiuti usano `SecurityError.Code`; le metriche hanno cardinalità limitata
  a `kind` e `error_code`.

## Matrice di attacco e test

| Attacco | Codice canonico | Copertura |
| --- | --- | --- |
| loopback/private/metadata | `INPUT_SSRF_BLOCKED` | `TestFetchBlocksPrivateAndMetadataNetworks` |
| DNS rebinding dopo redirect | `INPUT_DNS_REBINDING_BLOCKED` | `TestFetchRejectsDNSRebindingAfterRedirect` |
| DNS lento | `INPUT_DOWNLOAD_TIMEOUT` | `TestFetchDNSLookupTimeoutIsCanonical` |
| body oltre limite con Content-Length falso | `INPUT_DOWNLOAD_TOO_LARGE` | `TestFetchEnforcesStreamingLimitWhenContentLengthLies` |
| catena di redirect eccessiva | `INPUT_REDIRECT_LIMIT` | `TestFetchRedirectLimitIsCanonical` |
| MIME HTML/corrotto | `INPUT_HTML_PAYLOAD`, `INPUT_FFPROBE_FAILED` | `TestValidateRejectsHTMLMIMEAndCorruptMedia` |
| path locale fuori allowlist | `INPUT_PATH_VIOLATION` | `TestValidateRejectsClientPathAndQuarantinesWithoutURL` |
| policy senza temp dir esplicita | — | `TestFetchUsesSystemTempWhenPolicyOmitsTempDir` |

## Invarianti di rilascio

1. Nessun URL non fidato raggiunge una rete privata o il metadata service.
2. Nessun file supera `MaxBytes` contando i byte effettivamente letti.
3. Un redirect o un cambio DNS non evita la policy.
4. Un file non verificato non viene promosso a asset `READY` né consegnato al
   renderer.
5. Ogni rifiuto osservabile espone `error_code`; log e metriche non contengono
   URL sensibili, token o header.

La policy di test può abilitare reti private solo in fixture ermetiche. La
composizione di produzione non abilita quel bypass.
