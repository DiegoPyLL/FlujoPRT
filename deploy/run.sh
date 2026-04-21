#!/bin/bash

# Script para ejecutar ImageRecompilerCloud en background

SCRIPT_PATH="/home/ubuntu/FlujoPRT/src/imageRecopilator/Cloud/ImageRecompilerCloud.py"
LOG_FILE="/home/ubuntu/captura.log"
PID_FILE="/home/ubuntu/captura.pid"

case "$1" in
    start)
        if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
            echo "El proceso ya está corriendo (PID: $(cat $PID_FILE))"
            exit 1
        fi
        
        echo "Iniciando captura en background..."
        nohup python3 "$SCRIPT_PATH" > "$LOG_FILE" 2>&1 &
        echo $! > "$PID_FILE"
        echo "Proceso iniciado (PID: $(cat $PID_FILE))"
        echo "Ver logs: tail -f $LOG_FILE"
        ;;
    
    stop)
        if [ ! -f "$PID_FILE" ]; then
            echo "No hay proceso corriendo"
            exit 1
        fi
        
        PID=$(cat "$PID_FILE")
        echo "Deteniendo proceso (PID: $PID)..."
        kill $PID
        rm -f "$PID_FILE"
        echo "Proceso detenido"
        ;;
    
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    
    status)
        if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
            echo "Proceso corriendo (PID: $(cat $PID_FILE))"
        else
            echo "Proceso detenido"
            [ -f "$PID_FILE" ] && rm -f "$PID_FILE"
        fi
        ;;
    
    logs)
        tail -f "$LOG_FILE"
        ;;
    
    *)
        echo "Uso: $0 {start|stop|restart|status|logs}"
        exit 1
        ;;
esac