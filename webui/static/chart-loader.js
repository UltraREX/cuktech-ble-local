// Notify the page when the bundled Chart.js finishes loading.
// Runs synchronously in <head>, no IIFE needed
if (typeof Chart === 'undefined') {
    var localScript = document.querySelector('script[src="/static/chart.umd.min.js"]');
    if (localScript) {
        localScript.onload = function() {
            window._chartLoaded = true;
            if (typeof window.onChartReady === 'function') {
                window.onChartReady();
            }
        };
        localScript.onerror = function() {
            console.error('Failed to load bundled Chart.js');
        };
    }
}
