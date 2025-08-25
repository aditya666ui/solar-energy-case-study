// simple CSV download helper
function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const s = String(value);
  // wrap in quotes if needed; escape double quotes
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

export function downloadCsv(filename, headers, rows) {
  // headers: array of column names
  // rows: array of arrays (each inner array = row values)
  const lines = [];
  if (headers?.length) lines.push(headers.map(csvEscape).join(","));
  for (const row of rows) lines.push(row.map(csvEscape).join(","));

  const csv = "\uFEFF" + lines.join("\n"); // BOM so Excel opens UTF-8 correctly
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}