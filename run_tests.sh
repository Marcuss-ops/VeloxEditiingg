cd DataServer
go test -count=1 -v -timeout 60s ./internal/outbox/... 2>&1 | tee /tmp/ds_outbox_v3.log | tail -200
echo "=== DS_OUTBOX_TEST=$? ==="
echo "=== PASS_COUNTS ==="
grep -cE "^--- PASS" /tmp/ds_outbox_v3.log
echo "=== FAIL_LINES ==="
grep -E "^(--- FAIL|FAIL\t|panic:|test timed out)" /tmp/ds_outbox_v3.log || echo "(none)"
echo "=== FAIL_NAMES ==="
grep -E "^--- FAIL" /tmp/ds_outbox_v3.log | awk '{print $3}'
