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
- La **chiave pubblica** della CA (non segreta) viene distribuita ai nodi da
  `deploy/playbooks/bootstrap-ssh.yml` → `/etc/ssh/trusted-user-ca-keys.pem` (0600)
  e dichiarata con `TrustedUserCAKeys`.
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
- Auth per la firma: `BAO_TOKEN` (es. token AppRole `ssh-operator`, vedi §5) o
  root token dallo state dir (fallback bootstrap).

## 4. TrustedUserCAKeys sui nodi (bootstrap-ssh.yml)

`deploy/playbooks/bootstrap-ssh.yml` (idempotente, esegue `sshd -t` prima del
reload) ora:

1. crea l'utente `velox-deploy` (no password, PAM locked);
2. installa la pubkey operatore in `authorized_keys` (**fallback di transizione**);
3. installa la CA pubblica OpenBao in `/etc/ssh/trusted-user-ca-keys.pem` (0600)
   quando `velox_ssh_ca_pubkey` / `vault_velox_ssh_ca_pubkey` è valorizzata;
4. drop-in hardening `/etc/ssh/sshd_config.d/00-velox-hardening.conf`:
   `PubkeyAuthentication yes`, `PasswordAuthentication no`,
   `KbdInteractiveAuthentication no`, `PermitRootLogin no` e
   `TrustedUserCAKeys /etc/ssh/trusted-user-ca-keys.pem` (solo se la CA è presente).

```bash
# con la CA pub passata inline (o via vault, vedi §7):
ansible-playbook -i deploy/ansible/inventory.ini \
  --extra-vars "velox_ssh_ca_pubkey=$(cat .velox/openbao/ssh-ca.pub)" \
  deploy/playbooks/bootstrap-ssh.yml
```

> **Fail-safe**: `sshd -t` valida la config PRIMA del reload; `TrustedUserCAKeys`
> punta a un file 0600 root. Se la CA non è ancora distribuita, la direttiva non
> viene scritta (condizione Jinja) — il nodo continua ad accettare solo chiavi.

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

L'AppRole `ssh-operator` consente la firma SENZA il root token:
`BAO_TOKEN=$(bao write -field=token auth/approle/login role_id=... secret_id=...) \
 ./scripts/sign-operator-ssh.sh --pubkey-file ~/.ssh/velox.pub`

## 6. Dismissione password / chiavi statiche

Sequenza consigliata (niente blocchi fuori da tutte le macchine):

1. **CA attiva ovunque**: `bootstrap-ssh.yml` su tutti i nodi (CA pub presente).
2. **Operatori su cert**: ogni operatore firma e usa solo certificati per un
   periodo di osservazione (es. 1 settimana).
3. **Rimozione `authorized_keys`**: togliere il task "Install operator SSH public
   key" da `bootstrap-ssh.yml` (o svuotare `vault_velox_operator_pubkey`) e
   pulire `/home/velox-deploy/.ssh/authorized_keys` sui nodi.
4. **Password SSH**: già disabilitate da `PasswordAuthentication no` (fase
   precedente); `vault_velox_sudo_password` rimosso; sudo passwordless
   `velox-deploy`.
5. **Rotazione CA** (rara): richiede re-bootstrap `TrustedUserCAKeys` + ri-firma
   di tutti gli operatori — da fare solo in caso di compromissione della CA privata.

## 7. Variabili

| Variabile | Dove | Uso |
|---|---|---|
| `vault_velox_ssh_ca_pubkey` | `deploy/group_vars/vault.yml.example` | CA pubblica per `bootstrap-ssh.yml` |
| `velox_ssh_ca_pubkey` | extra-var inline (vince su vault) | idem |
| `sshd_ca_keys_file` | `bootstrap-ssh.yml` (default `/etc/ssh/trusted-user-ca-keys.pem`) | path TrustedUserCAKeys |
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
  il backup del volume raft di OpenBao (§11 di `deploy/openbao/README.md`).

## 9. Riferimenti

- Script: `deploy/openbao/scripts/{provision-ssh-ca,sign-operator-ssh,verify-ssh-ca}.sh`
- Policy: `deploy/openbao/policies/{admin,ssh-operator}.hcl`
- Playbook: `deploy/playbooks/bootstrap-ssh.yml`
- OpenBao SSH engine: https://openbao.org/docs/secrets/ssh/
- OpenSSH CA: `man ssh-keygen` (`-s`, `-I`, `-n`, `-V`) e `man sshd_config` (`TrustedUserCAKeys`)
