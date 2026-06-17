# 13 — mTLS for Control Plane Authentication

## Stato attuale

L'autenticazione attuale è basata su:
- **Bearer token** ottenuto durante la registrazione (durata 1 ora)
- **Bypass per IP locali** (`AuthorizeWorkerToken` in `auth.go:36` accetta IP di loopback senza token)
- **Nessun TLS** a livello di control plane

Il `TokenManager` è ora SQLite-backed (grazie a migration 020 e `store_worker_control.go`),
ma il token è ancora un bearer effimero.

## Stato target

mTLS (mutual TLS) per il control plane gRPC:

```
Worker                                    Master
  │                                          │
  │ ──── client cert ────────────────────→ │  (prova identità worker)
  │  ←─── server cert ──────────────────── │  (prova identità master)
  │                                          │
  │ ──── gRPC stream over mTLS ──────────→ │  (sessione autenticata)
```

Principi:

1. **Certificato client = identità worker**: il `worker_id` è derivato dal certificato client
2. **Il `worker_id` nel messaggio è informazione, non prova di identità**: il server estrae
   l'identità dal certificato, non dal campo `worker_id` del messaggio
3. **Stream TLS = sessione autenticata**: non servono token bearer dopo l'handshake
4. **Credential hash in SQLite** (task 01) come fallback per validazione offline

### Gestione certificati

- **CA interna** (o cert-manager su Kubernetes) per firmare i certificati
- Il worker riceve il certificato client all'installazione (es. via Ansible, file su disco)
- Il master ha il certificato server e la CA per validare i client
- Rotazione certificati: il worker si ri-registra con il nuovo certificato

### Integrazione con credential SQLite

Il certificato client può essere validato in due modi:
1. **Chain of trust**: il master verifica che il certificato sia firmato dalla CA interna
2. **Pin del fingerprint**: il master confronta il fingerprint del certificato con
   `worker_credentials.credential_hash` in SQLite (task 01)

## File coinvolti

| File | Azione |
|---|---|
| `DataServer/internal/transport/grpc_server.go` | Modificare: configurare TLS, estrarre worker ID dal cert |
| `DataServer/internal/transport/tls_config.go` | Nuovo: helper per configurazione TLS |
| `DataServer/cmd/server/bootstrap.go` | Modificare: caricare certificati server e CA |
| `DataServer/internal/config/config.go` | Modificare: `TLSCertFile`, `TLSKeyFile`, `TLSClientCAFile` |
| `RemoteCodex/native/worker-agent-go/internal/transport/grpc_transport.go` | Modificare: configurare client TLS |
| `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | Modificare: `TLSClientCertFile`, `TLSClientKeyFile`, `TLSCAFile` |

## Definition of Done

### Infrastruttura
- [ ] Script per generare CA root e certificati worker (`scripts/gen-worker-certs.sh`)
- [ ] Certificato server per il master
- [ ] Certificato client per ogni worker (con `CN = worker_id`)

### Lato master
- [ ] `GRPCControlServer` configurato con TLS:
  ```go
  tls.Config{
      ClientAuth: tls.RequireAndVerifyClientCert,
      ClientCAs:  caCertPool,
  }
  ```
- [ ] Interceptor gRPC estrae `workerID` dal `CommonName` del certificato client
- [ ] Il `workerID` estratto dal cert viene iniettato nel context
- [ ] Validazione aggiuntiva opzionale: fingerprint del cert matcha `worker_credentials.credential_hash`
- [ ] Config `control_grpc_tls_cert`, `control_grpc_tls_key`, `control_grpc_tls_ca`

### Lato worker
- [ ] `GRPCStreamTransport` carica certificato client e CA:
  ```go
  tls.Config{
      Certificates: []tls.Certificate{clientCert},
      RootCAs:      caCertPool,
  }
  ```
- [ ] Config `control_grpc_tls_client_cert`, `control_grpc_tls_client_key`, `control_grpc_tls_ca`
- [ ] Fallback: se TLS non configurato, usa connessione insecure (sviluppo locale)

### Test
- [ ] Test: connessione con certificato valido → autorizzato
- [ ] Test: connessione senza certificato → rifiutata
- [ ] Test: connessione con certificato non firmato dalla CA → rifiutata
- [ ] Test: `workerID` nel messaggio diverso da `CN` del cert → warning loggato, usa `CN`

## Criteri di test

```bash
# Unit test
cd refactored/DataServer && go test ./internal/transport/... -v -run TestTLS
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/transport/... -v -run TestTLS

# Integration: handshake mTLS
cd refactored && go test ./... -tags=integration -v -run TestMTLSHandshake
```

## Dipendenze

- **08** (GRPCStreamTransport) — prerequisito (TLS è configurato su gRPC)
- **01** (worker credentials) — per validazione fingerprint del cert
