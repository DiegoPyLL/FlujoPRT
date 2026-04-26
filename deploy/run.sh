#!/bin/bash

# Script para ejecutar los procesos de FlujoPRT en background.
#
# Captura CCTV:
#   ./run.sh start|stop|restart|status|logs
#
# Deteccion vehicular (proceso paralelo):
#   ./run.sh deteccion-start|deteccion-stop|deteccion-restart|deteccion-status|deteccion-logs

PROYECTO_DIR="/home/ubuntu/FlujoPRT"

# Captura
CAPTURA_SCRIPT="$PROYECTO_DIR/src/imageRecopilator/Cloud/ImageRecompilerCloud.py"
CAPTURA_LOG="$PROYECTO_DIR/captura.log"
CAPTURA_PID="$PROYECTO_DIR/captura.pid"

# Deteccion vehicular
DETECCION_SCRIPT="$PROYECTO_DIR/scripts/VehicleRecognition/worker_deteccion.py"
DETECCION_LOG="$PROYECTO_DIR/scripts/VehicleRecognition/logs/acciones_deteccion.log"
DETECCION_PID="$PROYECTO_DIR/deteccion.pid"

_iniciar() {
    local nombre="$1" script="$2" log="$3" pid_file="$4"
    if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "$nombre ya esta corriendo (PID: $(cat "$pid_file"))"
        exit 1
    fi
    mkdir -p "$(dirname "$log")"
    echo "Iniciando $nombre..."
    nohup python3 "$script" >> "$log" 2>&1 &
    echo $! > "$pid_file"
    echo "$nombre iniciado (PID: $(cat "$pid_file"))"
    echo "Ver logs: tail -f $log"
}

_detener() {
    local nombre="$1" pid_file="$2"
    if [ ! -f "$pid_file" ]; then
        echo "No hay proceso $nombre corriendo"
        exit 1
    fi
    local pid
    pid=$(cat "$pid_file")
    echo "Deteniendo $nombre (PID: $pid)..."
    kill "$pid"
    rm -f "$pid_file"
    echo "$nombre detenido"
}

_estado() {
    local nombre="$1" pid_file="$2"
    if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
        echo "$nombre corriendo (PID: $(cat "$pid_file"))"
    else
        echo "$nombre detenido"
        [ -f "$pid_file" ] && rm -f "$pid_file"
    fi
}

case "$1" in
    # ── Captura CCTV ──────────────────────────────────────────────────────────
    start)
        _iniciar "Captura CCTV" "$CAPTURA_SCRIPT" "$CAPTURA_LOG" "$CAPTURA_PID"
        ;;
    stop)
        _detener "Captura CCTV" "$CAPTURA_PID"
        ;;
    restart)
        $0 stop; sleep 2; $0 start
        ;;
    status)
        _estado "Captura CCTV" "$CAPTURA_PID"
        ;;
    logs)
        tail -f "$CAPTURA_LOG"
        ;;

    # ── Deteccion vehicular ───────────────────────────────────────────────────
    deteccion-start)
        _iniciar "Deteccion vehicular" "$DETECCION_SCRIPT" "$DETECCION_LOG" "$DETECCION_PID"
        ;;
    deteccion-stop)
        _detener "Deteccion vehicular" "$DETECCION_PID"
        ;;
    deteccion-restart)
        $0 deteccion-stop; sleep 2; $0 deteccion-start
        ;;
    deteccion-status)
        _estado "Deteccion vehicular" "$DETECCION_PID"
        ;;
    deteccion-logs)
        tail -f "$DETECCION_LOG"
        ;;

    *)
        echo "Uso: $0 {start|stop|restart|status|logs}"
        echo "       $0 {deteccion-start|deteccion-stop|deteccion-restart|deteccion-status|deteccion-logs}"
        exit 1
        ;;
esac