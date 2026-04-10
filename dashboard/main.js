const DATA_DIR = "/output";
const cityState = { page: 1, pageSize: 25, filterText: "", rows: [] };
const clubsState = { page: 1, pageSize: 25, rows: [] };
const changesState = { page: 1, pageSize: 25, activeTab: "new", data: {} };
const STATUS_POLL_MS = 5000;
const QUAL_LABELS = {
  A: "Basic",
  B: "Morse (5 wpm)",
  C: "Morse (12 wpm)",
  D: "Advanced",
  E: "Basic with Honours",
};

/** Colours for the qualification doughnut and custom legend (cycles if needed). */
const QUAL_MIX_COLORS = [
  "#5BC0EB",
  "#F25F5C",
  "#F4A261",
  "#E9C46A",
  "#2A9D8F",
  "#9B5DE5",
  "#94A3B8",
];

function qualLetterKeyHtml() {
  return Object.entries(QUAL_LABELS)
    .map(
      ([letter, text]) =>
        `<span class="qual-legend__letter-pair"><strong>${letter}</strong> ${escapeHtml(text)}</span>`,
    )
    .join("");
}

function describeQualificationCombo(combo) {
  if (!combo || combo === "(none)") return "No listed qualification";
  if (combo === "OTHER") return "Other qualification combinations";
  const labels = combo
    .split("")
    .map((letter) => QUAL_LABELS[letter] || letter)
    .join(" + ");
  return `${combo} - ${labels}`;
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let i = 0;
  let inQuotes = false;

  while (i < text.length) {
    const ch = text[i];

    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') {
        cell += '"';
        i += 2;
        continue;
      }
      inQuotes = !inQuotes;
      i += 1;
      continue;
    }

    if (ch === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      i += 1;
      continue;
    }

    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && text[i + 1] === "\n") {
        i += 1;
      }
      row.push(cell);
      if (!(row.length === 1 && row[0] === "")) {
        rows.push(row);
      }
      row = [];
      cell = "";
      i += 1;
      continue;
    }

    cell += ch;
    i += 1;
  }

  if (cell.length > 0 || row.length > 0) {
    row.push(cell);
    rows.push(row);
  }

  if (rows.length === 0) return [];
  const headers = rows[0];
  return rows.slice(1).map((r) => {
    const obj = {};
    headers.forEach((h, idx) => {
      obj[h] = (r[idx] ?? "").trim();
    });
    return obj;
  });
}

async function loadCsv(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Failed to load ${path}: ${response.status}`);
  }
  const text = await response.text();
  return parseCsv(text);
}

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function fmt(n) {
  return new Intl.NumberFormat("en-CA").format(n);
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (ch) => {
    const entities = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    };
    return entities[ch] || ch;
  });
}

function ensureStatusPanel() {
  let panel = document.querySelector("#status-panel");
  if (panel) return panel;

  panel = document.createElement("section");
  panel.id = "status-panel";
  panel.className = "panel panel--status";
  document.querySelector("main.container").prepend(panel);
  return panel;
}

function clearStatusPanel() {
  const panel = document.querySelector("#status-panel");
  if (panel) panel.remove();
}

function renderBootstrapState(status) {
  const panel = ensureStatusPanel();
  const startedAt = status.last_started_at
    ? status.last_started_at.replace("T", " ").replace("+00:00", " UTC")
    : null;

  panel.classList.toggle("error", Boolean(status.last_error));
  panel.innerHTML = `
    <h2>Preparing dashboard data</h2>
    <p>${
      status.analysis_running
        ? "Initial analysis is running in the background. A fresh Railway deploy can take a minute or two before the CSV and JSON outputs are ready."
        : "The web service is up, but the generated dashboard files are not ready yet."
    }</p>
    ${startedAt ? `<p class="status-meta">Last analysis start: ${escapeHtml(startedAt)}</p>` : ""}
    ${status.last_error ? `<p class="status-meta">Last error: ${escapeHtml(status.last_error)}</p>` : ""}
  `;
}

function fillKpis(provinceRows, qualRows, canonicalRowCount) {
  const summedProvinces = provinceRows.reduce((sum, r) => sum + num(r.records), 0);
  const n = canonicalRowCount != null ? Number(canonicalRowCount) : NaN;
  const totalRecords =
    Number.isFinite(n) && n > 0 ? Math.trunc(n) : summedProvinces;
  const provinceCount = provinceRows.length;
  const topProvince = provinceRows[0] || {};
  const topQual = qualRows[0] || {};

  document.querySelector("#kpi-total-records").textContent = fmt(totalRecords);
  document.querySelector("#kpi-province-count").textContent = fmt(provinceCount);
  document.querySelector("#kpi-top-province").textContent =
    topProvince.province && topProvince.records
      ? `${topProvince.province} (${fmt(num(topProvince.records))})`
      : "-";
  document.querySelector("#kpi-top-qual").textContent =
    topQual.qualification_combo && topQual.records
      ? `${describeQualificationCombo(topQual.qualification_combo)} (${fmt(num(topQual.records))})`
      : "-";
}

function fillChangeKpis(changes) {
  document.querySelector("#kpi-new-callsigns").textContent = fmt((changes.new || []).length);
  document.querySelector("#kpi-removed-callsigns").textContent = fmt((changes.removed || []).length);
  document.querySelector("#kpi-upgrades").textContent = fmt((changes.qual_upgrade || []).length);
}

function renderProvinceChart(rows) {
  const top = rows.slice(0, 10);
  const ctx = document.querySelector("#province-chart");
  const tickColor = "#9fb1d0";
  const gridColor = "rgba(159, 177, 208, 0.12)";

  new Chart(ctx, {
    type: "bar",
    data: {
      labels: top.map((r) => r.province),
      datasets: [
        {
          label: "Records",
          data: top.map((r) => num(r.records)),
          backgroundColor: "rgba(99, 164, 255, 0.72)",
          borderColor: "rgba(255, 255, 255, 0.35)",
          borderWidth: 1,
          borderRadius: 4,
          barPercentage: 0.72,
          categoryPercentage: 0.82,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      layout: {
        padding: { left: 0, right: 4, top: 6, bottom: 2 },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (c) => `Records: ${fmt(num(c.parsed.y))}`,
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            color: tickColor,
            maxRotation: 0,
            minRotation: 0,
            autoSkip: false,
            font: { size: 11 },
          },
          border: { display: false },
        },
        y: {
          beginAtZero: true,
          grid: { color: gridColor },
          border: { display: false },
          ticks: {
            color: tickColor,
            maxTicksLimit: 7,
            font: { size: 11 },
            callback: (v) => fmt(v),
          },
        },
      },
    },
  });
}

function renderQualMixLegend(labels, colors) {
  const legend = document.querySelector("#qual-legend");
  const segments = labels
    .map(
      (lab, i) => `
    <div class="qual-legend__item">
      <span class="qual-legend__swatch" style="background:${colors[i]}"></span>
      <span class="qual-legend__label">${escapeHtml(lab)}</span>
    </div>`,
    )
    .join("");
  legend.innerHTML = `
    <div class="qual-legend__segments">${segments}</div>
    <div class="qual-legend__letters" aria-label="Qualification letter key">${qualLetterKeyHtml()}</div>
  `;
}

function renderQualChart(rows) {
  const top = rows.slice(0, 6);
  const others = rows.slice(6).reduce((sum, r) => sum + num(r.records), 0);
  const labels = top.map((r) => describeQualificationCombo(r.qualification_combo));
  const values = top.map((r) => num(r.records));
  if (others > 0) {
    labels.push(describeQualificationCombo("OTHER"));
    values.push(others);
  }

  const colors = labels.map((_, i) => QUAL_MIX_COLORS[i % QUAL_MIX_COLORS.length]);

  const ctx = document.querySelector("#qual-chart");
  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [
        {
          data: values,
          backgroundColor: colors,
          // Match Chart.js default doughnut styling (same visual as Basic/Honours chart)
          borderColor: "rgba(255, 255, 255, 0.92)",
          borderWidth: 2,
          hoverBorderColor: "#ffffff",
        },
      ],
    },
    options: {
      responsive: true,
      cutout: "52%",
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (context) => `${context.label}: ${fmt(num(context.parsed))}`,
          },
        },
      },
    },
  });

  renderQualMixLegend(labels, colors);
}

function renderLevelSplitChart(rows) {
  let basicHonours = 0;
  let advancedAndOther = 0;

  for (const row of rows) {
    const combo = row.qualification_combo || "";
    const count = num(row.records);
    if (combo.includes("D")) {
      advancedAndOther += count;
    } else if (combo.includes("A") || combo.includes("E")) {
      basicHonours += count;
    } else {
      advancedAndOther += count;
    }
  }

  const ctx = document.querySelector("#level-chart");
  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: ["Basic / Basic with Honours", "Advanced (and advanced+other combinations)"],
      datasets: [
        {
          data: [basicHonours, advancedAndOther],
          backgroundColor: ["#5BC0EB", "#F25F5C"],
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        tooltip: {
          callbacks: {
            label: (context) => `${context.label}: ${fmt(num(context.parsed))}`,
          },
        },
      },
    },
  });
}

function renderQualityTable(rows) {
  const body = document.querySelector("#quality-body");
  body.innerHTML = rows
    .map(
      (r) => `
      <tr>
        <td>${escapeHtml(r.metric || "")}</td>
        <td>${fmt(num(r.count))}</td>
        <td>${num(r.share_pct).toFixed(3)}</td>
      </tr>
    `
    )
    .join("");
}

const CLUB_SUMMARY_LABELS = {
  total_records: "Total records (source rows)",
  records_with_club_name: "Records listing a club name",
  club_record_share_pct: "Share of records with a club name",
  distinct_club_names: "Distinct club names",
  distinct_fsa: "Distinct postal FSAs (first 3 characters)",
};

function clubSummaryMetricLabel(metric) {
  return CLUB_SUMMARY_LABELS[metric] || metric.replace(/_/g, " ");
}

function renderClubSummaryTable(rows) {
  const body = document.querySelector("#club-summary-body");
  body.innerHTML = rows
    .map((r) => {
      const metric = r.metric || "";
      const label = clubSummaryMetricLabel(metric);
      const raw = num(r.value);
      let display;
      if (metric === "club_record_share_pct") {
        display = `${raw.toFixed(3)}%`;
      } else {
        display = fmt(Math.round(raw));
      }
      return `<tr>
        <td>${escapeHtml(label)}</td>
        <td class="num">${display}</td>
      </tr>`;
    })
    .join("");
}

function renderQualByProvinceTable(rows) {
  const letters = ["A", "B", "C", "D", "E"];
  const body = document.querySelector("#qual-by-prov-body");
  body.innerHTML = rows
    .map((r) => {
      const qualCells = letters
        .map((L) => {
          const c = num(r[`qual_${L}_count`]);
          const p = num(r[`qual_${L}_pct`]);
          return `<td class="num">${fmt(c)}<span class="pct"> (${p.toFixed(1)}%)</span></td>`;
        })
        .join("");
      return `<tr>
        <td>${escapeHtml(r.province || "")}</td>
        <td class="num">${fmt(num(r.records))}</td>
        ${qualCells}
      </tr>`;
    })
    .join("");
  const keyEl = document.querySelector("#qual-by-prov-letters");
  if (keyEl) keyEl.innerHTML = qualLetterKeyHtml();
}

function renderCityTable() {
  const needle = cityState.filterText.trim().toUpperCase();
  const filtered = cityState.rows
    .map((r, idx) => ({ ...r, rank: idx + 1 }))
    .filter((r) => {
      const hay = `${r.province} ${r.city}`.toUpperCase();
      return !needle || hay.includes(needle);
    });
  const totalPages = Math.max(1, Math.ceil(filtered.length / cityState.pageSize));
  cityState.page = Math.min(cityState.page, totalPages);
  const start = (cityState.page - 1) * cityState.pageSize;
  const visibleRows = filtered.slice(start, start + cityState.pageSize);

  const body = document.querySelector("#city-body");
  body.innerHTML = visibleRows
    .map(
      (r) => `
      <tr>
        <td>${r.rank}</td>
        <td>${escapeHtml(r.province || "")}</td>
        <td>${escapeHtml(r.city || "")}</td>
        <td>${fmt(num(r.records))}</td>
      </tr>
    `
    )
    .join("");

  document.querySelector("#city-page-label").textContent = `Page ${cityState.page} of ${totalPages}`;
  document.querySelector("#city-prev").disabled = cityState.page <= 1;
  document.querySelector("#city-next").disabled = cityState.page >= totalPages;
}

function renderClubTable() {
  const totalPages = Math.max(1, Math.ceil(clubsState.rows.length / clubsState.pageSize));
  clubsState.page = Math.min(clubsState.page, totalPages);
  const start = (clubsState.page - 1) * clubsState.pageSize;
  const visibleRows = clubsState.rows.slice(start, start + clubsState.pageSize);

  const body = document.querySelector("#clubs-body");
  body.innerHTML = visibleRows
    .map(
      (r, idx) => `
      <tr>
        <td>${start + idx + 1}</td>
        <td>${escapeHtml(r.club_name || "")}</td>
        <td>${fmt(num(r.records))}</td>
      </tr>
    `
    )
    .join("");

  document.querySelector("#clubs-page-label").textContent = `Page ${clubsState.page} of ${totalPages}`;
  document.querySelector("#clubs-prev").disabled = clubsState.page <= 1;
  document.querySelector("#clubs-next").disabled = clubsState.page >= totalPages;
}

function renderTrendChart(history) {
  if (!history || history.length === 0) return;
  const ctx = document.querySelector("#trend-chart");
  new Chart(ctx, {
    type: "line",
    data: {
      labels: history.map((r) => r.taken_at.slice(0, 10)),
      datasets: [
        {
          label: "Total Licensees",
          data: history.map((r) => r.row_count),
          tension: 0.3,
          fill: true,
          pointRadius: history.length < 30 ? 4 : 2,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        y: {
          beginAtZero: false,
          ticks: { callback: (v) => fmt(v) },
        },
      },
    },
  });
}

function renderChangesTable() {
  const rows = changesState.data[changesState.activeTab] || [];
  const totalPages = Math.max(1, Math.ceil(rows.length / changesState.pageSize));
  changesState.page = Math.min(changesState.page, totalPages);
  const start = (changesState.page - 1) * changesState.pageSize;
  const visible = rows.slice(start, start + changesState.pageSize);
  const isQual = changesState.activeTab.startsWith("qual");

  const thead = document.querySelector("#changes-thead");
  thead.innerHTML = isQual
    ? "<tr><th>Callsign</th><th>Province</th><th>Before</th><th>After</th><th>Detected</th></tr>"
    : "<tr><th>Callsign</th><th>Province</th><th>Detected</th></tr>";

  const body = document.querySelector("#changes-body");
  if (visible.length === 0) {
    body.innerHTML = `<tr><td colspan="${isQual ? 5 : 3}" style="color:var(--muted);font-style:italic">No entries</td></tr>`;
  } else {
    body.innerHTML = visible
      .map((r) => {
        const date = escapeHtml((r.detected_at || "").slice(0, 10));
        const callsign = escapeHtml(r.callsign || "");
        const province = escapeHtml(r.prov_cd || "");
        if (isQual) {
          const oldQuals = escapeHtml(r.old_quals || "");
          const newQuals = escapeHtml(r.new_quals || "");
          return `<tr>
            <td><code>${callsign}</code></td>
            <td>${province}</td>
            <td>${oldQuals}</td>
            <td>${newQuals}</td>
            <td>${date}</td>
          </tr>`;
        }
        return `<tr>
          <td><code>${callsign}</code></td>
          <td>${province}</td>
          <td>${date}</td>
        </tr>`;
      })
      .join("");
  }

  document.querySelector("#changes-page-label").textContent = `Page ${changesState.page} of ${totalPages}`;
  document.querySelector("#changes-prev").disabled = changesState.page <= 1;
  document.querySelector("#changes-next").disabled = changesState.page >= totalPages;
}

function initChangesPanel(changes) {
  changesState.data = changes;

  const days = changes.days || 30;
  document.querySelector("#changes-date-range").textContent = `— last ${days} days`;

  document.querySelectorAll(".tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("tab-btn--active"));
      btn.classList.add("tab-btn--active");
      changesState.activeTab = btn.dataset.tab;
      changesState.page = 1;
      renderChangesTable();
    });
  });

  document.querySelector("#changes-prev").addEventListener("click", () => {
    if (changesState.page > 1) changesState.page -= 1;
    renderChangesTable();
  });
  document.querySelector("#changes-next").addEventListener("click", () => {
    changesState.page += 1;
    renderChangesTable();
  });

  renderChangesTable();
}

function showError(message) {
  const panel = ensureStatusPanel();
  panel.className = "panel panel--status error";
  panel.innerHTML = `<h2>Failed to load dashboard data</h2><p>${escapeHtml(message)}</p>`;
}

async function loadJson(path) {
  const response = await fetch(path);
  if (!response.ok) throw new Error(`Failed to load ${path}: ${response.status}`);
  return response.json();
}

async function waitForDashboardData() {
  while (true) {
    const status = await loadJson("/status");
    if (status.ready) {
      clearStatusPanel();
      return status;
    }

    renderBootstrapState(status);
    if (status.last_error && !status.analysis_running) {
      throw new Error(`Initial analysis failed: ${status.last_error}`);
    }

    await new Promise((resolve) => window.setTimeout(resolve, STATUS_POLL_MS));
  }
}

async function boot() {
  try {
    const status = await waitForDashboardData();

    const [
      provinceRows,
      qualRows,
      qualByProvRows,
      qualityRows,
      cityRows,
      clubsRows,
      clubSummaryRows,
      snapshotHistory,
      recentChanges,
    ] = await Promise.all([
      loadCsv(`${DATA_DIR}/province_summary.csv`),
      loadCsv(`${DATA_DIR}/qualification_combo_summary.csv`),
      loadCsv(`${DATA_DIR}/qualification_by_province.csv`),
      loadCsv(`${DATA_DIR}/data_quality_summary.csv`),
      loadCsv(`${DATA_DIR}/city_summary.csv`),
      loadCsv(`${DATA_DIR}/top_clubs.csv`),
      loadCsv(`${DATA_DIR}/club_summary.csv`),
      loadJson(`${DATA_DIR}/snapshot_history.json`).catch(() => []),
      loadJson(`${DATA_DIR}/recent_changes.json?nocache=${Date.now()}`).catch(() => ({})),
    ]);

    fillKpis(provinceRows, qualRows, status.row_count);
    fillChangeKpis(recentChanges);
    renderTrendChart(snapshotHistory);
    renderProvinceChart(provinceRows);
    renderQualChart(qualRows);
    renderLevelSplitChart(qualRows);
    initChangesPanel(recentChanges);
    renderQualityTable(qualityRows);
    renderQualByProvinceTable(qualByProvRows);
    cityState.rows = cityRows;
    clubsState.rows = clubsRows;
    renderCityTable();
    renderClubTable();
    renderClubSummaryTable(clubSummaryRows);

    const cityFilter = document.querySelector("#city-filter");
    cityFilter.addEventListener("input", () => {
      cityState.filterText = cityFilter.value;
      cityState.page = 1;
      renderCityTable();
    });

    document.querySelector("#city-page-size").addEventListener("change", (e) => {
      cityState.pageSize = num(e.target.value) || 25;
      cityState.page = 1;
      renderCityTable();
    });
    document.querySelector("#city-prev").addEventListener("click", () => {
      if (cityState.page > 1) cityState.page -= 1;
      renderCityTable();
    });
    document.querySelector("#city-next").addEventListener("click", () => {
      cityState.page += 1;
      renderCityTable();
    });

    document.querySelector("#clubs-page-size").addEventListener("change", (e) => {
      clubsState.pageSize = num(e.target.value) || 25;
      clubsState.page = 1;
      renderClubTable();
    });
    document.querySelector("#clubs-prev").addEventListener("click", () => {
      if (clubsState.page > 1) clubsState.page -= 1;
      renderClubTable();
    });
    document.querySelector("#clubs-next").addEventListener("click", () => {
      clubsState.page += 1;
      renderClubTable();
    });
  } catch (err) {
    showError(err instanceof Error ? err.message : String(err));
  }
}

boot();
