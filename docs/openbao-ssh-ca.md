# Velox — SSH Certificate Authority con OpenBao

> **Stato:** operativo (fase 7 della migrazione secrets — `docs/secrets-audit.md`).
> **Obiettivo:** eliminare le password SSH statiche e le `authorized_keys` per
> operatore: i nodi fidano della **CA SSH di OpenBao** (`TrustedUserCAKeys`) e gli
> operatori accedono con **certificati firmati a TTL breve** (default 30 min) e
> principals limitati a `velox-admin` / `velox-deploy`. Certificati scaduti = accesso
> negato — nessuna revoca manuale su 5 VPS.

---

## 1. Architettura

```text
operatore (laptop)                        worker / master (VPS)
  ~/.ssh/velox (privata, MAI esce)          /etc/ssh/trusted-user-ca-keys.pem
  ~/.ssh/velox-cert.pub  ◄──┐                └─ TrustedUserCAKeys (sshd)
                            │ firma TTL 30m
                     OPENBAO (secrets engine ssh)
                       ssh/config/ca  → CA key pair (privata SOLO in OpenBao)
                       ssh/roles/velox-operator
                         key_type=ca, allowed_users=velox-admin,velox-deploy
```

- La **chiave privata della CA** nasce dentro OpenBao (`generate_signing_key`) e
  non viene MAI esportata. Rigenerarla invaliderebbe tutti i certificati emessi:
  `provision-ssh-ca.sh` è idempotente e **rifiuta di sovrascrivere** una CA esistente.
- La **chiave pubblica** della CA (non segreta) viene esportata da
  `provision-ssh-ca.sh` in `$STATE_DIR/ssh-ca.pub` (0644) e distribuita ai nodi
  dall'operatore (il playbook `bootstrap-ssh.yml` è stato ritirato) come
  `/etc/ssh/trusted-user-ca-keys.pem` (0600), dichiarata con `TrustedUserCAKeys`.
- L'operatore firma la SUA public key per 30 minuti; OpenSSH la usa
  automaticamente come `~/.ssh/<chiave>-cert.pub`.

## 2. Bootstrap della CA (una tantum)

Prerequisiti: OpenBao up e unsealed (`deploy/openbao/` §4-7), `bao` CLI o REST,
root token (o token con policy `admin`) nello state dir.

```bash
cd deploy/openbao
./scripts/provision-ssh-ca.sh            # enable engine + CA + role + export pubkey
./scripts/verify-ssh-ca.sh               # 11 check: engine, CA, role, firma di prova, negativo
```

Cosa crea:
- secrets engine `ssh` (mount `ssh/`);
- CA signing key in `ssh/config/ca` (idempotente — seconda run: `SKIP`);
- role `velox-operator` (`key_type=ca`, `allow_user_certificates=true`,
  `allowed_users=velox-admin,velox-deploy`, `default_user=velox-deploy`,
  `ttl=30m`, `max_ttl=24h`, `allowed_user_key_configs` ssh-rsa 2048/4096 + ed25519);
- `$STATE_DIR/ssh-ca.pub` (0644) — la chiave PUBBLICA per `TrustedUserCAKeys`.

Verifica live (`verify-ssh-ca.sh`): engine attivo, CA configurata, role corretto,
**firma di prova** di una chiave ed25519 temporanea validata con `ssh-keygen -L`
(principal + finestra di validità), e check **negativo fail-closed**: la firma con
un principal non consentito (es. `root`) viene rifiutata (HTTP 400/403).

## 3. Firma del certificato operatore (uso quotidiano)

```bash
cd deploy/openbao
./scripts/sign-operator-ssh.sh --pubkey-file ~/.ssh/velox.pub
# → cert su stdout; installa con:  ~/.ssh/velox-cert.pub
./scripts/sign-operator-ssh.sh --pubkey-file ~/.ssh/velox.pub \
    --principals velox-admin --ttl 2h --out ~/.ssh/velox-cert.pub
```

- TTL **breve** (default `30m`; il role limita il max); principals ammessi:
  `velox-admin`, `velox-deploy` (qualsiasi altro → rifiutato dal server).
- Il cert va salvato come `<nome-chiave>-cert.pub` accanto alla chiave privata
  (convenzione OpenSSH: usato automaticamente).
- Il nome del file di output NON deve contenere la chiave privata: il cert è
  pubblico (firmato), la chiave privata non lascia mai la workstation.
- Auth per la firma: esclusivamente `BAO_TOKEN` ottenuto tramite login
  AppRole `ssh-operator` (vedi §5). Il root token di bootstrap non è accettato
  dallo script operativo.

## 4. TrustedUserCAKeys sui nodi

La distribuzione della CA pubblica ai nodi è un passo **operativo** (il playbook
Ansible `bootstrap-ssh.yml` è stato ritirato insieme al bridge Ansible):
`provision-ssh-ca.sh` esporta la chiave PUBBLICA in `$STATE_DIR/ssh-ca.pub`
(0644) e l'operatore la installa su ogni nodo (master/worker) come
`/etc/ssh/trusted-user-ca-keys.pem` (0600, root) con il drop-in hardening:

```bash
# sul nodo target (CA pub esportata da provision-ssh-ca.sh):
scp .velox/openbao/ssh-ca.pub velox-deploy@HOST:/tmp/ssh-ca.pub
ssh velox-deploy@HOST '
  sudo install -m 0600 -o root -g root /tmp/ssh-ca.pub /etc/ssh/trusted-user-ca-keys.pem &&
  sudo rm -f /tmp/ssh-ca.pub &&
  printf "%s\n" \
    "PubkeyAuthentication yes" "PasswordAuthentication no" \
    "KbdInteractiveAuthentication no" "PermitRootLogin no" \
    "TrustedUserCAKeys /etc/ssh/trusted-user-ca-keys.pem" \
    | sudo tee /etc/ssh/sshd_config.d/00-velox-hardening.conf >/dev/null &&
  sudo sshd -t && sudo systemctl reload ssh
'
```

> **Fail-safe**: `sshd -t` valida la config PRIMA del reload; `TrustedUserCAKeys`
> punta a un file 0600 root. Se la CA non è ancora distribuita, la direttiva
> non viene scritta — il nodo continua ad accettare solo chiavi.

## 5. Identità di firma least-privilege (AppRole `ssh-operator`)

Policy `deploy/openbao/policies/ssh-operator.hcl`:
`update` su `ssh/sign/*`, `read` su `ssh/config/ca` + `ssh/roles/*`, `read` su
`sys/health`. Nessun accesso al KV `velox/*`.

```bash
cd deploy/openbao
./scripts/provision-policies.sh                                  # admin(ssh/*) + ssh-operator
./scripts/provision-approle.sh --principal ssh-operator          # materiale AppRole
./scripts/verify-approle.sh --principal ssh-operator             # 6 check fail-closed
```

L'AppRole `ssh-operator` è l'unico percorso operativo per la firma:

```bash
cd deploy/openbao
export BAO_CACERT="${OPENBAO_CA_FILE:-../../.velox/openbao/tls/server.crt}"
[[ -s "$BAO_CACERT" ]] || { echo "OpenBao CA certificate missing: $BAO_CACERT" >&2; exit 1; }
BAO_TOKEN=$(bao write -field=token auth/approle/login \
  role_id="$(cat ../../.velox/openbao/approle/ssh-operator/role-id)" \
  secret_id="$(cat ../../.velox/openbao/approle/ssh-operator/secret-id)") \
  ./scripts/sign-operator-ssh.sh --pubkey-file ~/.ssh/velox.pub
```

`sign-operator-ssh.sh` rifiuta l'esecuzione se `BAO_TOKEN` non è presente,
verifica che il token abbia la policy `ssh-operator` e non legge più alcun root
token locale. Un token admin/root passato manualmente viene quindi rifiutato.

## 6. Dismissione password / chiavi statiche

Sequenza consigliata (niente blocchi fuori da tutte le macchine):

1. **CA attiva ovunque**: distribuire `ssh-ca.pub` su tutti i nodi
   (`/etc/ssh/trusted-user-ca-keys.pem` + `TrustedUserCAKeys`, §4).
2. **Operatori su cert**: ogni operatore firma e usa solo certificati per un
   periodo di osservazione (es. 1 settimana).
3. **Rimozione `authorized_keys`**: rimuovere manualmente
   `/home/velox-deploy/.ssh/authorized_keys` sui nodi (la CA non la usa più).
4. **Password SSH**: già disabilitate da `PasswordAuthentication no` (fase
   precedente); sudo passwordless `velox-deploy`.
5. **Rotazione CA** (rara): richiede re-bootstrap `TrustedUserCAKeys` + ri-firma
   di tutti gli operatori — da fare solo in caso di compromissione della CA privata.

## 7. Variabili

| Variabile | Dove | Uso |
|---|---|---|
| `ssh-ca.pub` | `$STATE_DIR/ssh-ca.pub` — esportata da `provision-ssh-ca.sh` (0644) | CA pubblica → `/etc/ssh/trusted-user-ca-keys.pem` (0600) sui nodi |
| `TrustedUserCAKeys` | `/etc/ssh/sshd_config.d/00-velox-hardening.conf` | path `/etc/ssh/trusted-user-ca-keys.pem` |
| `OPENBAO_SSH_ROLE/TTL/MAX_TTL/ALLOWED_USERS/PRINCIPALS/CA_OUT` | env di `provision-ssh-ca.sh` | parametri role/CA |

## 8. Sicurezza e limiti noti

- **TTL breve** = finestra d'attacco ridotta; `not_before_duration=30`s (di default
  OpenBao) evita problemi di clock skew.
- **Niente revoche**: un cert scade da solo; per escludere subito un operatore
  compromesso, ruotare il `secret-id` dell'AppRole `ssh-operator` e/o revocare i
  token (`bao token revoke <accessor>`).
- Il check negativo (`root` rifiutato) è **fail-closed** in `verify-ssh-ca.sh`:
  un errore di verifica fa fallire, mai passare.
- La chiave privata della CA NON è esportabile da OpenBao: il backup della CA è
  il backup del volume raft di OpenBao (§13 di `deploy/openbao/README.md`).

## 9. Riferimenti

- Script: `deploy/openbao/scripts/{provision-ssh-ca,sign-operator-ssh,verify-ssh-ca}.sh`
- Policy: `deploy/openbao/policies/{admin,ssh-operator}.hcl`
- OpenBao SSH engine: https://openbao.org/docs/secrets/ssh/
- OpenSSH CA: `man ssh-keygen` (`-s`, `-I`, `-n`, `-V`) e `man sshd_config` (`TrustedUserCAKeys`)
