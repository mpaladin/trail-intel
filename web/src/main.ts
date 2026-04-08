import "./styles.css";
import "leaflet/dist/leaflet.css";

import { buildForecastReport } from "./lib/forecast/engine";
import {
  formatDateTime,
  formatDistanceKm,
  formatDuration,
  formatFullDateTime,
  formatNullableNumber,
  formatNumber,
  formatPercent,
  safeFileStem,
} from "./lib/forecast/format";
import { ForecastInputError, WeatherApiError } from "./lib/forecast/errors";
import type { BuildForecastResult, ForecastKeyMoment, SampleForecast } from "./lib/forecast/types";
import { renderReportImageBlob } from "./lib/export/reportImage";
import { renderPrecipitationChart, renderTemperatureChart, renderWindChart } from "./ui/charts";
import { mountForecastMap, renderForecastMapCard, teardownForecastMap } from "./ui/forecastMap";

interface BeforeInstallPromptEvent extends Event {
  prompt: () => Promise<void>;
  userChoice: Promise<{ outcome: "accepted" | "dismissed"; platform: string }>;
}

interface AppState {
  fileName: string;
  gpxText: string;
  reportResult: BuildForecastResult | null;
  loading: boolean;
  exporting: boolean;
  error: string;
  exportError: string;
  installPrompt: BeforeInstallPromptEvent | null;
  installDismissed: boolean;
}

const appRoot = document.querySelector<HTMLDivElement>("#app");
if (!appRoot) {
  throw new Error("Could not find app container.");
}
const root = appRoot;

const state: AppState = {
  fileName: "",
  gpxText: "",
  reportResult: null,
  loading: false,
  exporting: false,
  error: "",
  exportError: "",
  installPrompt: null,
  installDismissed: false,
};

renderApp();
registerServiceWorker();

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.installPrompt = event as BeforeInstallPromptEvent;
  renderInstallCard();
});

window.addEventListener("appinstalled", () => {
  state.installPrompt = null;
  state.installDismissed = true;
  renderInstallCard();
});

function renderApp(): void {
  root.innerHTML = `
    <main class="page-shell">
      <section class="hero-panel">
        <div class="hero-copy">
          <p class="eyebrow">TrailIntel Forecast PWA</p>
          <h1>Build a route weather report directly on your phone.</h1>
          <p class="hero-lead">
            Import a GPX file, choose a start time, and TrailIntel will sample the route,
            fetch weather live from Open-Meteo, and render a mobile-friendly forecast without
            sending your GPX to a backend.
          </p>
          <div class="hero-pills">
            <span class="pill">GitHub Pages ready</span>
            <span class="pill">Open-Meteo live data</span>
            <span class="pill">Installable PWA</span>
          </div>
        </div>
        <div class="hero-aside" id="install-card"></div>
      </section>

      <section class="panel form-panel">
        <div class="panel-head">
          <div>
            <p class="section-tag">Generate</p>
            <h2>Create a forecast</h2>
          </div>
          <p class="section-copy">Forecasts stay on-device except for the weather API requests.</p>
        </div>
        <form id="forecast-form" class="forecast-form">
          <label class="field field-file">
            <span>GPX file</span>
            <input id="gpx-input" name="gpx" type="file" accept=".gpx,application/gpx+xml,application/xml,text/xml" required />
          </label>
          <label class="field">
            <span>Route title</span>
            <input id="title-input" name="title" type="text" placeholder="Optional display title" />
          </label>
          <label class="field">
            <span>Start time</span>
            <input id="start-input" name="start" type="datetime-local" required />
          </label>
          <label class="field">
            <span>Timezone</span>
            <input id="timezone-input" name="timezone" type="text" list="timezone-options" required />
            <datalist id="timezone-options"></datalist>
          </label>
          <label class="field">
            <span>Duration</span>
            <input id="duration-input" name="duration" type="text" value="03:30" inputmode="numeric" pattern="[0-9]{1,2}:[0-9]{2}(:[0-9]{2})?" required />
          </label>
          <label class="field">
            <span>Sample minutes</span>
            <input id="sample-minutes-input" name="sampleMinutes" type="number" min="1" max="60" value="10" required />
          </label>
          <div class="form-actions">
            <button id="generate-button" class="primary-button" type="submit">Generate forecast</button>
            <p class="form-hint">The app fetches weather directly from Open-Meteo and keeps the forecast interactive.</p>
          </div>
        </form>
      </section>

      <section id="status-panel" class="status-panel" aria-live="polite"></section>
      <section id="report-root"></section>
    </main>
  `;

  setupForm();
  renderInstallCard();
  renderStatus();
  renderReport();
}

function setupForm(): void {
  const form = document.querySelector<HTMLFormElement>("#forecast-form");
  const titleInput = document.querySelector<HTMLInputElement>("#title-input");
  const fileInput = document.querySelector<HTMLInputElement>("#gpx-input");
  const startInput = document.querySelector<HTMLInputElement>("#start-input");
  const timezoneInput = document.querySelector<HTMLInputElement>("#timezone-input");
  const timezoneOptions = document.querySelector<HTMLDataListElement>("#timezone-options");
  if (!form || !titleInput || !fileInput || !startInput || !timezoneInput || !timezoneOptions) {
    throw new Error("Could not initialize forecast form.");
  }

  startInput.value = defaultStartTimeInput();
  timezoneInput.value = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  timezoneOptions.innerHTML = listTimezones()
    .map((timezoneName) => `<option value="${timezoneName}"></option>`)
    .join("");

  fileInput.addEventListener("change", async () => {
    const file = fileInput.files?.[0];
    if (!file) {
      state.gpxText = "";
      state.fileName = "";
      return;
    }

    state.fileName = file.name;
    state.gpxText = await file.text();
    if (!titleInput.value.trim()) {
      titleInput.value = safeFileStem(file.name);
    }
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!state.gpxText.trim()) {
      state.error = "Choose a GPX file before generating a forecast.";
      state.reportResult = null;
      renderStatus();
      renderReport();
      return;
    }

    state.loading = true;
    state.error = "";
    state.exportError = "";
    renderStatus();
    updateButton();

    const formData = new FormData(form);
    try {
      state.reportResult = await buildForecastReport({
        gpxText: state.gpxText,
        title: String(formData.get("title") || ""),
        start: String(formData.get("start") || ""),
        duration: String(formData.get("duration") || ""),
        timezoneName: String(formData.get("timezone") || ""),
        sampleMinutes: Number(formData.get("sampleMinutes") || 10),
      });
    } catch (error) {
      state.reportResult = null;
      state.error = formatError(error);
    } finally {
      state.loading = false;
      state.exporting = false;
      renderStatus();
      renderReport();
      updateButton();
    }
  });

  updateButton();
}

function renderInstallCard(): void {
  const installCard = document.querySelector<HTMLDivElement>("#install-card");
  if (!installCard) {
    return;
  }

  const isInstalled =
    window.matchMedia("(display-mode: standalone)").matches || (navigator as Navigator & { standalone?: boolean }).standalone === true;
  const isiOS = /iphone|ipad|ipod/i.test(navigator.userAgent);

  if (isInstalled || state.installDismissed) {
    installCard.innerHTML = `
      <div class="install-card success-card">
        <p class="install-tag">Ready</p>
        <strong>Use it like an app.</strong>
        <p>The forecast shell is installable and keeps its UI available offline.</p>
      </div>
    `;
    return;
  }

  if (state.installPrompt) {
    installCard.innerHTML = `
      <div class="install-card">
        <p class="install-tag">Install</p>
        <strong>Add TrailIntel to your home screen.</strong>
        <p>Keep the shell handy on your phone and launch forecasts like a native app.</p>
        <div class="install-actions">
          <button id="install-button" class="secondary-button" type="button">Install app</button>
          <button id="dismiss-install-button" class="ghost-button" type="button">Not now</button>
        </div>
      </div>
    `;
    document.querySelector<HTMLButtonElement>("#install-button")?.addEventListener("click", async () => {
      const prompt = state.installPrompt;
      if (!prompt) {
        return;
      }
      await prompt.prompt();
      await prompt.userChoice;
      state.installPrompt = null;
      renderInstallCard();
    });
    document.querySelector<HTMLButtonElement>("#dismiss-install-button")?.addEventListener("click", () => {
      state.installDismissed = true;
      renderInstallCard();
    });
    return;
  }

  if (isiOS) {
    installCard.innerHTML = `
      <div class="install-card">
        <p class="install-tag">iPhone tip</p>
        <strong>Add it from Safari.</strong>
        <p>Open the share sheet in Safari and choose <span class="inline-pill">Add to Home Screen</span>.</p>
      </div>
    `;
    return;
  }

  installCard.innerHTML = `
    <div class="install-card">
      <p class="install-tag">PWA shell</p>
      <strong>Install support is enabled.</strong>
      <p>When your browser offers installation, TrailIntel can live on your home screen.</p>
    </div>
  `;
}

function renderStatus(): void {
  const statusPanel = document.querySelector<HTMLDivElement>("#status-panel");
  if (!statusPanel) {
    return;
  }

  if (state.loading) {
    statusPanel.innerHTML = `
      <div class="status-card loading-card">
        <span class="status-dot"></span>
        Sampling the route and fetching weather from Open-Meteo...
      </div>
    `;
    return;
  }

  if (state.error) {
    statusPanel.innerHTML = `<div class="status-card error-card">${escapeHtml(state.error)}</div>`;
    return;
  }

  if (state.reportResult) {
    statusPanel.innerHTML = `
      <div class="status-card success-card">
        Forecast ready. ${escapeHtml(state.reportResult.report.samples.length.toString())} route samples processed from ${escapeHtml(state.reportResult.report.sourceLabel)}.
      </div>
    `;
    return;
  }

  statusPanel.innerHTML = "";
}

function renderReport(): void {
  const reportRoot = document.querySelector<HTMLDivElement>("#report-root");
  if (!reportRoot) {
    return;
  }

  teardownForecastMap();

  if (!state.reportResult) {
    reportRoot.innerHTML = `
      <section class="panel empty-panel">
        <div class="empty-illustration"></div>
        <div>
          <p class="section-tag">Forecast</p>
          <h2>Your interactive route report will appear here.</h2>
          <p class="section-copy">
            Once you import a GPX and generate a run, the app will show route metrics, key moments,
            live weather charts, and a sample-by-sample route breakdown.
          </p>
        </div>
      </section>
    `;
    return;
  }

  const { report, summary, keyMoments } = state.reportResult;
  reportRoot.innerHTML = `
    <section class="panel report-hero">
      <div class="panel-head report-head">
        <div class="report-head-copy">
          <p class="section-tag">Forecast Report</p>
          <h2>${escapeHtml(report.title)}</h2>
          <p class="section-copy">
            ${escapeHtml(formatFullDateTime(report.startTimeMs, report.timezoneName))} to
            ${escapeHtml(formatFullDateTime(report.endTimeMs, report.timezoneName))}.
          </p>
        </div>
        <div class="report-head-side">
          <div class="meta-pill-row">
            <span class="pill">${escapeHtml(report.sourceLabel)}</span>
            <span class="pill">${escapeHtml(report.providerId)}</span>
            <span class="pill">${escapeHtml(report.timezoneName)}</span>
          </div>
          <div class="report-actions">
            <button id="download-report-button" class="secondary-button" type="button">
              Download report PNG
            </button>
            <p id="export-note" class="export-note">
              Downloads a square PNG with the map, charts, and route summary.
            </p>
          </div>
        </div>
      </div>
      <div class="metric-grid">
        ${metricCard("Distance", formatDistanceKm(report.route.totalDistanceM), "Total sampled route length")}
        ${metricCard("Ascent", `${formatNumber(report.route.totalAscentM, 0)} m`, "Cumulative positive elevation")}
        ${metricCard("Duration", formatDuration(report.durationMs), "User-entered ride duration")}
        ${metricCard("Samples", report.samples.length.toString(), "Generated route checkpoints")}
        ${metricCard("Temp range", `${formatNumber(summary.temperatureMinC)} to ${formatNumber(summary.temperatureMaxC)} C`, "Across all sampled route points")}
        ${metricCard("Max wind", `${formatNumber(summary.windMaxKph)} km/h`, "Peak sustained wind")}
      </div>
    </section>

    <div class="two-up-grid">
      ${renderForecastMapCard(report)}
      <section class="panel">
        <div class="panel-head">
          <div>
            <p class="section-tag">Moments</p>
            <h2>Key spots</h2>
          </div>
          <p class="section-copy">The start, finish, and the most consequential weather points.</p>
        </div>
        <div class="moment-grid">
          ${keyMoments.map((moment) => keyMomentCard(moment, report.timezoneName)).join("")}
        </div>
      </section>
    </div>

    <section class="chart-grid">
      ${renderTemperatureChart(report)}
      ${renderWindChart(report)}
      ${renderPrecipitationChart(report)}
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <p class="section-tag">Samples</p>
          <h2>Route-by-route detail</h2>
        </div>
        <p class="section-copy">Every sampled point shows the time, location along the route, and weather conditions.</p>
      </div>
      <div class="sample-grid">
        ${report.samples.map((sample) => sampleCard(sample, report.timezoneName)).join("")}
      </div>
    </section>
  `;

  const exportButton = document.querySelector<HTMLButtonElement>("#download-report-button");
  exportButton?.addEventListener("click", () => {
    void handleExport();
  });
  renderExportState();
  void mountForecastMap(report);
}

function metricCard(label: string, value: string, detail: string): string {
  return `
    <article class="metric-card">
      <p>${escapeHtml(label)}</p>
      <strong>${escapeHtml(value)}</strong>
      <span>${escapeHtml(detail)}</span>
    </article>
  `;
}

function keyMomentCard(moment: ForecastKeyMoment, timezoneName: string): string {
  const { sample } = moment;
  return `
    <article class="moment-card">
      <p class="moment-label">${escapeHtml(moment.label)}</p>
      <strong>${escapeHtml(formatDateTime(sample.sample.timestampMs, timezoneName))}</strong>
      <span>${escapeHtml(formatDistanceKm(sample.sample.distanceM))}</span>
      <span>${escapeHtml(formatNumber(sample.temperatureC))} C</span>
      <span>${escapeHtml(formatNumber(sample.windKph))} km/h wind</span>
      <span>${escapeHtml(formatNumber(sample.precipitationMm, 2))} mm rain</span>
    </article>
  `;
}

function sampleCard(sample: SampleForecast, timezoneName: string): string {
  const items = [
    ["Time", formatDateTime(sample.sample.timestampMs, timezoneName)],
    ["Distance", formatDistanceKm(sample.sample.distanceM)],
    ["Elevation", sample.sample.elevationM === null ? "-" : `${formatNumber(sample.sample.elevationM, 0)} m`],
    ["Temp", `${formatNumber(sample.temperatureC)} C`],
    ["Feels like", `${formatNullableNumber(sample.apparentTemperatureC)} C`],
    ["Wind", `${formatNumber(sample.windKph)} km/h`],
    ["Gust", `${formatNullableNumber(sample.windGustKph)} km/h`],
    ["Direction", `${formatNumber(sample.windDirectionDeg, 0)} deg`],
    ["Clouds", `${formatNumber(sample.cloudCoverPct, 0)}%`],
    ["Rain", `${formatNumber(sample.precipitationMm, 2)} mm`],
    ["Rain chance", formatPercent(sample.precipitationProbability)],
  ];

  return `
    <article class="sample-card">
      <div class="sample-head">
        <strong>Sample ${sample.sample.index + 1}</strong>
        <span>${escapeHtml(formatDuration(sample.sample.elapsedMs))}</span>
      </div>
      <dl class="sample-list">
        ${items
          .map(
            ([label, value]) => `
              <div>
                <dt>${escapeHtml(label)}</dt>
                <dd>${escapeHtml(value)}</dd>
              </div>
            `,
          )
          .join("")}
      </dl>
    </article>
  `;
}

function updateButton(): void {
  const button = document.querySelector<HTMLButtonElement>("#generate-button");
  if (!button) {
    return;
  }
  button.disabled = state.loading;
  button.textContent = state.loading ? "Generating..." : "Generate forecast";
}

function renderExportState(): void {
  const button = document.querySelector<HTMLButtonElement>("#download-report-button");
  const note = document.querySelector<HTMLParagraphElement>("#export-note");
  if (!button || !note || !state.reportResult) {
    return;
  }

  button.disabled = state.exporting;
  button.textContent = state.exporting ? "Rendering PNG..." : "Download report PNG";
  note.classList.toggle("export-note-error", Boolean(state.exportError));
  if (state.exporting) {
    note.textContent = "Fetching map tiles and encoding the report image...";
    return;
  }
  if (state.exportError) {
    note.textContent = state.exportError;
    return;
  }
  note.textContent = "Downloads a square PNG with the map, charts, and route summary.";
}

async function handleExport(): Promise<void> {
  if (!state.reportResult || state.exporting) {
    return;
  }

  state.exporting = true;
  state.exportError = "";
  renderExportState();

  try {
    const blob = await renderReportImageBlob(state.reportResult);
    downloadBlob(blob, buildExportFileName(state.reportResult));
  } catch (error) {
    state.exportError = formatError(error);
  } finally {
    state.exporting = false;
    renderExportState();
  }
}

function listTimezones(): string[] {
  const supportedValuesOf = (
    Intl as typeof globalThis.Intl & {
      supportedValuesOf?: (key: string) => string[];
    }
  ).supportedValuesOf;
  if (typeof supportedValuesOf === "function") {
    return supportedValuesOf("timeZone");
  }
  return ["UTC", "Europe/Rome", "Europe/Zurich", "Europe/Paris", "America/Los_Angeles"];
}

function defaultStartTimeInput(): string {
  const nextHour = new Date();
  nextHour.setMinutes(0, 0, 0);
  nextHour.setHours(nextHour.getHours() + 1);
  const year = nextHour.getFullYear();
  const month = `${nextHour.getMonth() + 1}`.padStart(2, "0");
  const day = `${nextHour.getDate()}`.padStart(2, "0");
  const hours = `${nextHour.getHours()}`.padStart(2, "0");
  const minutes = `${nextHour.getMinutes()}`.padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function formatError(error: unknown): string {
  if (error instanceof ForecastInputError || error instanceof WeatherApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return "Something went wrong while generating the forecast.";
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function buildExportFileName(result: BuildForecastResult): string {
  const title = safeFileStem(result.report.title)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  const date = new Date(result.report.startTimeMs).toISOString().slice(0, 10);
  return `${title || "forecast-report"}-${date}.png`;
}

function downloadBlob(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.append(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 0);
}

async function registerServiceWorker(): Promise<void> {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  try {
    await navigator.serviceWorker.register("./service-worker.js");
  } catch {
    // Ignore registration failures in unsupported hosting environments.
  }
}
