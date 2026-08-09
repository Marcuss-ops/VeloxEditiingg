# RW-PROD-017 — Rollout, promotion e rollback worker

**Priorità:** P0
**Dipendenze:** RW-PROD-015, RW-PROD-016
**Stato attuale:** Implementato. `fleetctl`/FleetController/UpdateExecutor è il
solo percorso di rollout; Ansible deployment actions e il vecchio bump script
sono fail-closed. Il doctor production, il digest ledger e il rollback riusano
la stessa immagine immutabile.

---

## 1. Pain points

1. **Gate production:** `doctor --production` è fail-closed; il rollout non
   certifica un worker senza evidenza positiva.
2. **Anti-rebuild enforced.** Compose/activation accettano solo il digest GHCR
   completo; il ledger confronta desired e running digest.
3. **Rollback playbook** esiste ma non è testato integration.
4. **Rollout mixed (vecchia/nuova immagine) live** non documentato con rischi.

---

## 2. Soluzione

1. **Gate obbligatorio `doctor`:** `velox-worker-agent doctor --production
   --json` deve uscire 0; WARN e prove mancanti sono FAIL.

2. **Image digest versioning:** `fleetctl` accepts only the full GHCR
   `@sha256:` reference; the Master deployment ledger stores target and
   previous digests and the WorkerCard exposes desired/running state.

3. **Promotion:** FleetController owns drain, exact-digest activation,
   readiness, smoke/Drive checks and ledger transition. Failure reuses the
   previous digest; no playbook or rebuild is involved.

4. **Rollback testable:** the same activation primitive restores the previous
   immutable digest and verifies the canonical container/readiness path.

5. **Vietare rebuild:** production playbooks and compatibility endpoints are
   fail-closed; only CI builds the signed image. `BUNDLE_HASH.txt` is
   diagnostic metadata and never selects a release.

---

## 3. Azioni concrete

| # | File:line | Stato |
|---|-----------|--------|
| A1 | `velox-worker-agent doctor --production` | Implementato, fail-closed. |
| A2 | `fleetctl update/status --production` | Implementato, digest/ledger aware. |
| A3 | `FleetController/UpdateExecutor` | Unico owner di rollout e rollback. |
| A4 | `velox-worker-activate-image` | Pull/activate/verify exact digest; nessun rebuild. |
| A5 | Ansible rollout API/playbooks | Ritirati o fail-closed; nessun caller production. |

---

## 4. Criteri di accettazione

- [x] Rollout e rollback riusano digest immutabili senza rebuild.
- [x] Desired/running digest e stato ledger sono confrontabili e fail-closed.
- [x] Production doctor rifiuta WARN o evidenza mancante.

---

## 5. Test obbligatori

- `TestPromote_DependsOnDoctorPass`.
- `TestRollback_RestoresToLastDigest`.
- `TestAntiRebuild_DifferentDigestBetweenStages_Fails`.
- `TestRolloutMixed_OldAndNewLive_NoJobLoss`.
- `TestCanaryFailure_AutoRollback`.

---

## 6. Evidenze

- `rollout-${ID}-${TS}.json` con digest, commit, canary metrics, fail rate, durations.
- `worker_deploys` DB rows populate on every promotion.
- HTTP endpoint `GET /api/v1/rollouts` consultabile.
- Dashboard `rollouts.json` con timeline + status.
