# ============================================================
# Common DoD Framework — colors, counters, logging & summary
# ============================================================
# Source this file at the top of any DoD check script:
#   source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
# ============================================================

# ── Bootstrap paths ──────────────────────────────────────────────
_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$_SCRIPT_DIR/../.." && pwd)"  # scripts/lib/ → project root
APP="$REPO_ROOT"
unset _SCRIPT_DIR

# ── Colors ──────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

# ── Counters & state ────────────────────────────────────────────
FAIL=0; WARN=0; SKIP=0; PASS=0; VERBOSE=false; WORKER=""

# ── Logging functions ───────────────────────────────────────────
log()    { echo -e "${BLUE}[DoD]${NC} $*"; }
ok()     { echo -e "  ${GREEN}✅ PASS${NC}  $*"; PASS=$((PASS + 1)); }
warn()   { echo -e "  ${YELLOW}⚠️  WARN${NC}  $*"; WARN=$((WARN + 1)); }
fail()   { echo -e "  ${RED}❌ FAIL${NC}  $*"; FAIL=$((FAIL + 1)); }
skip()   { echo -e "  ${YELLOW}⏭  SKIP${NC}  $*"; SKIP=$((SKIP + 1)); }
header() { echo -e "\n${BLUE}━━━ $* ━━━${NC}"; }

# ── Argument parsing ────────────────────────────────────────────
# Consumes --worker and --verbose from $@; hand them before sourcing
# if your script needs to add custom flags.
while [[ $# -gt 0 ]]; do
    case $1 in
        --worker)  WORKER="$2"; shift 2 ;;
        --verbose) VERBOSE=true; shift ;;
        --)        shift; break ;;
        *)         break ;;  # stop at first unknown arg (let caller handle)
    esac
done

# ── Summary & exit ──────────────────────────────────────────────
summary() {
    echo ""
    echo -e "  ${GREEN}PASS: $PASS${NC}  ${RED}FAIL: $FAIL${NC}  ${YELLOW}WARN: $WARN${NC}  ${YELLOW}SKIP: $SKIP${NC}"
    echo ""
    if [ $FAIL -gt 0 ]; then
        echo -e "${RED}DoD NOT MET — $FAIL gate(s) failed${NC}"
        exit 1
    elif [ $WARN -gt 0 ] || [ $SKIP -gt 0 ]; then
        echo -e "${YELLOW}RESULT: INCOMPLETE (WARN=$WARN, SKIP=$SKIP)${NC}"
        exit 2
    else
        echo -e "${GREEN}All gates PASS${NC}"
        exit 0
    fi
}
