// Sistema de Heartbeat para monitoreo de actividad
(function() {
    const INTERVALO_HEARTBEAT = 2 * 60 * 1000; // 2 minutos
    let heartbeatInterval = null;
    
    function enviarHeartbeat() {
        fetch('/api/actividad/heartbeat', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        })
        .then(res => res.json())
        .then(data => {
            // console.log('💓 Heartbeat enviado');
        })
        .catch(err => {
            console.error('Error enviando heartbeat:', err);
        });
    }
    
    function iniciarHeartbeat() {
        // Enviar heartbeat inmediatamente
        enviarHeartbeat();
        
        // Luego cada 2 minutos
        heartbeatInterval = setInterval(enviarHeartbeat, INTERVALO_HEARTBEAT);
        
        console.log('💓 Sistema de heartbeat iniciado (cada 2 minutos)');
    }
    
    function detenerHeartbeat() {
        if (heartbeatInterval) {
            clearInterval(heartbeatInterval);
            heartbeatInterval = null;
            console.log('💓 Sistema de heartbeat detenido');
        }
    }
    
    // Iniciar cuando la página carga
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', iniciarHeartbeat);
    } else {
        iniciarHeartbeat();
    }
    
    // Detener cuando la página se cierra
    window.addEventListener('beforeunload', detenerHeartbeat);
    
    // Pausar cuando la pestaña no está visible (opcional pero recomendado)
    document.addEventListener('visibilitychange', function() {
        if (document.hidden) {
            detenerHeartbeat();
        } else {
            iniciarHeartbeat();
        }
    });
})();
