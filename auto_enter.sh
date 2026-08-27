#!/bin/bash

# Range casuale in secondi: 5 minuti (300s) -> 15 minuti (900s)
MIN_WAIT=300
MAX_WAIT=900

get_freebuff_sessions() {
    tmux list-sessions -F "#{session_name}" 2>/dev/null | grep "freebuff"
}

case "$1" in
    start)
        if pgrep -f "auto_enter.sh loop" > /dev/null; then
            echo "[!] Auto-enter è già in esecuzione."
            exit 1
        fi

        SESSIONS=$(get_freebuff_sessions)
        if [ -z "$SESSIONS" ]; then
            echo "[x] Nessuna sessione tmux contenente 'freebuff' trovata attiva!"
            exit 1
        fi

        echo "[+] Sessioni rilevate:"
        echo "$SESSIONS"

        # Invio immediato di test a tutte le sessioni
        echo "[+] Invio segnale Enter immediato di prova..."
        for s in $SESSIONS; do
            tmux send-keys -t "$s" Enter
        done

        # Avvio del loop random in background
        nohup "$0" loop > /dev/null 2>&1 &
        echo "[✓] Auto-Enter ATTIVATO con intervallo random tra $((MIN_WAIT / 60)) e $((MAX_WAIT / 60)) minuti."
        ;;

    stop)
        pkill -f "auto_enter.sh loop"
        echo "[-] Auto-Enter DISATTIVATO."
        ;;

    status)
        if pgrep -f "auto_enter.sh loop" > /dev/null; then
            echo "[✓] Auto-Enter è ATTIVO in background."
            echo "[+] Sessioni attualmente monitorate:"
            get_freebuff_sessions
        else
            echo "[x] Auto-Enter è SPENTO."
        fi
        ;;

    loop)
        while true; do
            # Calcola un'attesa casuale tra MIN_WAIT e MAX_WAIT
            SLEEP_TIME=$(( RANDOM % (MAX_WAIT - MIN_WAIT + 1) + MIN_WAIT ))
            sleep "$SLEEP_TIME"

            # Invia Invio a tutte le sessioni con "freebuff" nel nome
            SESSIONS=$(get_freebuff_sessions)
            for s in $SESSIONS; do
                tmux send-keys -t "$s" Enter 2>/dev/null
            done
        done
        ;;

    *)
        echo "Uso: $0 {start|stop|status}"
        exit 1
        ;;
esac
