// Backend base URL. Empty = same origin (the Thymeleaf-served page). Set window.AMS_BACKEND
// (e.g. 'http://host:9000') before this script to point a separately-hosted frontend at the backend.
const BACKEND = (window.AMS_BACKEND || '').replace(/\/$/, '');
const WS_URL = (BACKEND ? BACKEND.replace(/^http/, 'ws') : 'ws://' + window.location.host) + '/discover';
const counts = { total: 0, CAR_ACCIDENT: 0, FIRE_ACCIDENT: 0, CRIMINAL: 0, OTHER_ACCIDENT: 0 };

const stompClient = new StompJs.Client({ brokerURL: WS_URL, reconnectDelay: 3000 });

stompClient.onConnect = () => {
    setStatus(true);
    stompClient.subscribe('/topic/events', (msg) => onEvent(JSON.parse(msg.body)));
    stompClient.subscribe('/topic/send-status', (msg) => onBatch(JSON.parse(msg.body)));
    stompClient.subscribe('/topic/service-discovery', (msg) => onServices(JSON.parse(msg.body)));
    stompClient.subscribe('/topic/analytics', (msg) => onAnalytics(JSON.parse(msg.body)));
    fetch(BACKEND + '/api/v1/messages/batches').then((r) => r.json()).then((list) => list.forEach(onBatch)).catch(() => {});
    fetch(BACKEND + '/api/v1/register').then((r) => r.json()).then(onServices).catch(() => {});
};
stompClient.onWebSocketClose = () => setStatus(false);
stompClient.onStompError = (frame) => console.error('Broker error: ', frame.body);

function setStatus(connected) {
    const el = document.getElementById('status');
    el.textContent = connected ? 'connected' : 'disconnected';
    el.className = 'badge ' + (connected ? 'bg-success' : 'bg-danger');
}

function onEvent(e) {
    counts.total++;
    counts[e.type] = (counts[e.type] || 0) + 1;
    document.getElementById('count-total').textContent = counts.total;
    const typeEl = document.getElementById('count-' + e.type);
    if (typeEl) typeEl.textContent = counts[e.type];
    prependRow(e);
}

function prependRow(e) {
    const time = new Date(e.receivedAt || Date.now()).toLocaleTimeString();
    const row = '<tr>'
        + '<td>' + time + '</td>'
        + '<td>' + typeBadge(e.type) + '</td>'
        + '<td>' + escapeHtml(e.address) + '</td>'
        + '<td>' + escapeHtml(e.latitude) + '</td>'
        + '<td>' + escapeHtml(e.longitude) + '</td>'
        + '</tr>';
    const feed = document.getElementById('feed');
    feed.insertAdjacentHTML('afterbegin', row);
    while (feed.rows.length > 50) {
        feed.deleteRow(feed.rows.length - 1);
    }
}

const batches = {};

function onBatch(b) {
    batches[b.id] = b;
    renderBatches();
}

function renderBatches() {
    const list = Object.values(batches).sort((a, b) => b.startedAt - a.startedAt).slice(0, 10);
    const tbody = document.getElementById('batches');
    if (!list.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-muted small">No batches yet.</td></tr>';
        return;
    }
    tbody.innerHTML = list.map(batchRow).join('');
}

function batchRow(b) {
    const done = b.produced + b.failed;
    const pct = b.total ? Math.round(done * 100 / b.total) : 0;
    const inProgress = b.status === 'IN_PROGRESS';
    const color = b.status === 'COMPLETED' ? 'success' : (b.status === 'COMPLETED_WITH_ERRORS' ? 'warning' : 'info');
    const bar = '<div class="progress" style="height:18px;">'
        + '<div class="progress-bar bg-' + color + (inProgress ? ' progress-bar-striped progress-bar-animated' : '')
        + '" style="width:' + pct + '%">' + pct + '%</div></div>';
    return '<tr>'
        + '<td><code>' + b.id.slice(0, 8) + '</code></td>'
        + '<td>' + b.total + '</td>'
        + '<td>' + b.produced + '</td>'
        + '<td>' + (b.failed ? '<span class="text-danger">' + b.failed + '</span>' : '0') + '</td>'
        + '<td><span class="badge text-bg-' + color + '">' + b.status + '</span></td>'
        + '<td>' + bar + '</td>'
        + '</tr>';
}

function onServices(list) {
    const tbody = document.getElementById('services');
    if (!Array.isArray(list) || !list.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-muted small">No services.</td></tr>';
        return;
    }
    tbody.innerHTML = list.map(serviceRow).join('');
}

function serviceRow(s) {
    const color = { UP: 'success', DEGRADED: 'warning', DOWN: 'danger', UNKNOWN: 'secondary' }[s.status] || 'secondary';
    const checked = s.lastChecked ? new Date(s.lastChecked).toLocaleTimeString() : '—';
    const detail = s.detail ? ' <span class="text-muted small">' + escapeHtml(s.detail) + '</span>' : '';
    const latency = (s.status === 'DOWN' && s.httpStatus == null) ? '—' : s.latencyMs + ' ms';
    return '<tr>'
        + '<td>' + escapeHtml(s.name) + '<div class="text-muted small">' + escapeHtml(s.url) + '</div></td>'
        + '<td><span class="badge text-bg-' + color + '">' + s.status + '</span></td>'
        + '<td>' + latency + '</td>'
        + '<td>' + (s.httpStatus != null ? s.httpStatus : '—') + '</td>'
        + '<td>' + checked + detail + '</td>'
        + '</tr>';
}

let typeChart = null;
let rateChart = null;

function initCharts() {
    const typeCanvas = document.getElementById('chart-types');
    const rateCanvas = document.getElementById('chart-rate');
    if (!typeCanvas || !rateCanvas || typeof Chart === 'undefined') return;
    typeChart = new Chart(typeCanvas, {
        type: 'doughnut',
        data: { labels: [], datasets: [{ data: [], backgroundColor: ['#0d6efd', '#dc3545', '#ffc107', '#6c757d', '#198754', '#0dcaf0'] }] },
        options: { plugins: { legend: { position: 'bottom' } }, animation: false }
    });
    rateChart = new Chart(rateCanvas, {
        type: 'line',
        data: { labels: [], datasets: [{ label: 'events/sec', data: [], borderColor: '#0d6efd', backgroundColor: 'rgba(13,110,253,0.1)', fill: true, tension: 0.3, pointRadius: 0 }] },
        options: { scales: { y: { beginAtZero: true } }, plugins: { legend: { display: false } }, animation: false }
    });
}

function onAnalytics(a) {
    setText('a-rate', Math.round(a.eventsPerSec));
    setText('a-total', a.totalEvents);
    setText('a-sensitive', a.sensitiveCount);
    setText('a-fraud', a.fraudCount);
    if (typeChart) {
        const types = a.byType || {};
        typeChart.data.labels = Object.keys(types);
        typeChart.data.datasets[0].data = Object.values(types);
        typeChart.update('none');
    }
    if (rateChart) {
        rateChart.data.labels.push(new Date(a.timestamp).toLocaleTimeString());
        rateChart.data.datasets[0].data.push(Math.round(a.eventsPerSec));
        if (rateChart.data.labels.length > 60) { rateChart.data.labels.shift(); rateChart.data.datasets[0].data.shift(); }
        rateChart.update('none');
    }
    const ul = document.getElementById('a-locations');
    if (ul) {
        ul.innerHTML = (a.topLocations || []).map((l) =>
            '<li class="list-group-item d-flex justify-content-between px-0">'
            + '<span class="text-truncate" style="max-width:140px">' + escapeHtml(l.address) + '</span>'
            + '<span class="badge text-bg-secondary">' + l.count + '</span></li>').join('');
    }
}

function setText(id, val) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
}

function typeBadge(type) {
    const colors = { CAR_ACCIDENT: 'primary', FIRE_ACCIDENT: 'danger', CRIMINAL: 'warning', OTHER_ACCIDENT: 'secondary' };
    return '<span class="badge text-bg-' + (colors[type] || 'secondary') + '">' + type + '</span>';
}

function escapeHtml(s) {
    return (s || '').replace(/[&<>"']/g, (c) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    document.getElementById('generate').addEventListener('click', () => {
        const total = document.getElementById('total').value;
        const result = document.getElementById('generate-result');
        result.textContent = 'sending…';
        fetch(BACKEND + '/api/v1/messages', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: 'total=' + encodeURIComponent(total)
        })
            .then((r) => r.json())
            .then((b) => { onBatch(b); result.textContent = 'batch ' + b.id.slice(0, 8) + ' queued (' + b.total + ' events)'; })
            .catch((err) => { result.textContent = 'Error: ' + err; });
    });

    stompClient.activate();
});
