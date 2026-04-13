export function toPostgresISO(
  value: any,
  options?: { raw?: boolean; onlyDate?: boolean }
): string | null {

  if (options?.raw) {
    return value ?? null;
  }

  // limpieza fuerte
  if (
    value === null ||
    value === undefined ||
    value === '' ||
    value === '[null]' ||
    value === 'NULL'
  ) {
    return null;
  }

  // SI YA ES ISO → NO TOCAR (pero sí aplicar onlyDate si viene)
  if (typeof value === 'string' && value.includes('T') && value.includes('Z')) {
    return options?.onlyDate
      ? value.split('T')[0]
      : value;
  }

  let date: Date;

  // Número (Excel serial)
  if (typeof value === 'number') {
    const excelEpoch = Date.UTC(1899, 11, 30);
    date = new Date(excelEpoch + value * 86400000);
  }

  // String numérico (Excel como texto)
  else if (typeof value === 'string' && !isNaN(Number(value.trim()))) {
    const num = Number(value.trim());
    const excelEpoch = Date.UTC(1899, 11, 30);
    date = new Date(excelEpoch + num * 86400000);
  }

  // Strings
  else if (typeof value === 'string') {
    const str = value.trim();
    const strLower = str.toLowerCase();

    if (
      strLower.includes('no resuelta') ||
      strLower.includes('no aplica') ||
      strLower.includes('n/a')
    ) {
      return null;
    }

    if (/^\d{2}\/\d{2}\/\d{4}/.test(str)) {
      const [datePart, timePart] = str.split(' ');
      const [day, month, year] = datePart.split('/').map(Number);

      let h = 0, m = 0, s = 0;

      if (timePart) {
        const t = timePart.split(':').map(Number);
        h = t[0] || 0;
        m = t[1] || 0;
        s = t[2] || 0;
      }

      date = new Date(Date.UTC(year, month - 1, day, h, m, s));
    }

    else if (typeof value === 'string') {
  const str = value.trim();
  const strLower = str.toLowerCase();

  if (
    strLower.includes('no resuelta') ||
    strLower.includes('no aplica') ||
    strLower.includes('n/a')
  ) {
    return null;
  }

  const parts = str.split(/[\/\-]/);

  if (parts.length === 3) {
    const [day, month, year] = parts.map(Number);

    if (day > 12) {
      // Seguro es DD/MM/YYYY
      date = new Date(Date.UTC(year, month - 1, day));
    } else {
      // Ambiguo → asumimos DD/MM igual (porque es tu caso)
      date = new Date(Date.UTC(year, month - 1, day));
    }
  }

  else {
    return null;
  }
}

    else {
      return null;
    }
  }

  else {
    return null;
  }

  if (!date || isNaN(date.getTime())) {
    return null;
  }

  return options?.onlyDate
    ? date.toISOString().split('T')[0]  
    : date.toISOString();               
}