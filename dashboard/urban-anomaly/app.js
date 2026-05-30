const apiBase = new URLSearchParams(window.location.search).get("api");
const citySelect = document.querySelector("#city");
const refreshButton = document.querySelector("#refresh");
const alertsEl = document.querySelector("#alerts");
const mapEl = document.querySelector("#map");

function setText(id, value) {
  document.querySelector(`#${id}`).textContent = value;
}

function normalizePoint(alert, bounds) {
  const latRange = bounds.maxLat - bounds.minLat || 1;
  const lonRange = bounds.maxLon - bounds.minLon || 1;
  return {
    x: ((alert.lon - bounds.minLon) / lonRange) * 84 + 8,
    y: (1 - (alert.lat - bounds.minLat) / latRange) * 84 + 8
  };
}

function render(data) {
  const summary = data.summary || {};
  const alerts = data.alerts || [];
  setText("total", summary.total_alerts ?? alerts.length);
  setText("critical", summary.critical ?? 0);
  setText("high", summary.high ?? 0);
  setText("medium", summary.medium ?? 0);
  document.querySelector("#city-label").textContent = citySelect.value;

  alertsEl.innerHTML = "";
  mapEl.innerHTML = "";

  const bounds = alerts.reduce((acc, alert) => ({
    minLat: Math.min(acc.minLat, Number(alert.lat)),
    maxLat: Math.max(acc.maxLat, Number(alert.lat)),
    minLon: Math.min(acc.minLon, Number(alert.lon)),
    maxLon: Math.max(acc.maxLon, Number(alert.lon))
  }), { minLat: 90, maxLat: -90, minLon: 180, maxLon: -180 });

  alerts.forEach((alert) => {
    const marker = document.createElement("span");
    marker.className = `marker ${alert.severity}`;
    const point = normalizePoint(alert, bounds);
    marker.style.left = `${point.x}%`;
    marker.style.top = `${point.y}%`;
    marker.title = `${alert.track_id} ${alert.score}`;
    mapEl.appendChild(marker);

    const row = document.createElement("article");
    row.className = "alert";
    row.innerHTML = `
      <div class="alert-title">
        <span>${alert.track_id || alert.alert_id}</span>
        <span class="badge ${alert.severity}">${alert.severity}</span>
      </div>
      <p class="alert-meta">${alert.sensor_type} | score ${alert.score} | ${alert.observed_at}</p>
      <p class="reasons">${(alert.reason_codes || []).join(", ")}</p>
      <p class="review-note">Status: ${alert.status || "needs_human_review"}</p>
    `;
    alertsEl.appendChild(row);
  });
}

async function loadAlerts() {
  if (!apiBase) {
    render(window.SAMPLE_ALERTS);
    return;
  }
  const url = `${apiBase}?city=${encodeURIComponent(citySelect.value)}&limit=50`;
  const response = await fetch(url);
  render(await response.json());
}

refreshButton.addEventListener("click", loadAlerts);
citySelect.addEventListener("change", loadAlerts);
loadAlerts();
