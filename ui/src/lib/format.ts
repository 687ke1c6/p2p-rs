/** Bytes as the CLI shows them (`peers::bytes`): `512 B`, `1.5 MiB`. */
export function bytes(n: number): string {
  const units = ["B", "KiB", "MiB", "GiB"];
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0 ? `${n} B` : `${value.toFixed(1)} ${units[unit]}`;
}

/** Seconds since a unix time, by this machine's clock. */
export function secondsSince(unix: number): number {
  return Math.max(0, Math.round(Date.now() / 1000 - unix));
}
