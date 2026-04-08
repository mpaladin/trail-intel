export function formatDateTime(
  timestampMs: number,
  timezoneName: string,
  options: Intl.DateTimeFormatOptions = {},
): string {
  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: timezoneName,
    ...options,
  }).format(new Date(timestampMs));
}

export function formatFullDateTime(timestampMs: number, timezoneName: string): string {
  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
    timeZone: timezoneName,
  }).format(new Date(timestampMs));
}

export function formatDuration(durationMs: number): string {
  const totalSeconds = Math.round(durationMs / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts: string[] = [];
  if (hours) {
    parts.push(`${hours}h`);
  }
  if (minutes || !parts.length) {
    parts.push(`${String(minutes).padStart(2, "0")}m`);
  }
  if (seconds) {
    parts.push(`${String(seconds).padStart(2, "0")}s`);
  }
  return parts.join(" ");
}

export function formatNumber(value: number, digits = 1): string {
  return value.toFixed(digits);
}

export function formatNullableNumber(value: number | null, digits = 1): string {
  return value === null ? "-" : value.toFixed(digits);
}

export function formatDistanceKm(distanceM: number): string {
  return `${(distanceM / 1000).toFixed(2)} km`;
}

export function formatPercent(value: number | null): string {
  return value === null ? "-" : `${Math.round(value)}%`;
}

export function safeFileStem(fileName: string): string {
  return fileName.replace(/\.[^./]+$/, "").replace(/[-_]+/g, " ").trim();
}
