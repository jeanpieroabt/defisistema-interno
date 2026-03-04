// version-check.js - Detecta nuevas versiones y muestra banner de actualización
(function() {
    const API = window.API_BASE_URL || 'https://defisistema-interno.onrender.com';
    const CHECK_INTERVAL = 60000; // Verificar cada 60 segundos
    let currentVersion = null;
    let bannerShown = false;

    async function checkVersion() {
        try {
            const r = await fetch(`${API}/api/cliente/version`, { cache: 'no-store' });
            if (!r.ok) return;
            const data = await r.json();
            if (!currentVersion) {
                currentVersion = data.version;
                return;
            }
            if (data.version !== currentVersion && !bannerShown) {
                showUpdateBanner();
                bannerShown = true;
            }
        } catch (e) { /* silencioso */ }
    }

    function showUpdateBanner() {
        const banner = document.createElement('div');
        banner.id = 'update-banner';
        banner.innerHTML = `
            <div style="
                position: fixed; bottom: 0; left: 0; right: 0; z-index: 99999;
                background: linear-gradient(135deg, #1a73e8, #0d47a1);
                color: white; padding: 14px 20px;
                display: flex; align-items: center; justify-content: space-between;
                box-shadow: 0 -2px 12px rgba(0,0,0,0.3);
                font-family: 'Inter', sans-serif;
                animation: slideUp 0.3s ease-out;
            ">
                <div style="display:flex; align-items:center; gap:10px; flex:1;">
                    <span style="font-size:20px;">🔄</span>
                    <span style="font-size:14px; font-weight:500;">Nueva actualización disponible</span>
                </div>
                <button onclick="location.reload(true)" style="
                    background: white; color: #1a73e8; border: none;
                    padding: 8px 18px; border-radius: 20px;
                    font-weight: 700; font-size: 13px; cursor: pointer;
                    white-space: nowrap;
                ">Actualizar</button>
            </div>
        `;
        const style = document.createElement('style');
        style.textContent = '@keyframes slideUp { from { transform: translateY(100%); } to { transform: translateY(0); } }';
        document.head.appendChild(style);
        document.body.appendChild(banner);
    }

    // Primera verificación al cargar, luego periódicamente
    checkVersion();
    setInterval(checkVersion, CHECK_INTERVAL);
})();
