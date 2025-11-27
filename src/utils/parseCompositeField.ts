// src/utils/parseCompositeField.ts

export interface CompositeFieldParseResult {
  plotCodes: string[];
  customerName?: string;
  customerPhone?: string;
}

/**
 * Parse chuỗi name kiểu:
 *  - "B3.4.4-16-11;12-TRẦN TRỌNG NGHĨA-0969446992"
 *  - "H1.9-07-16;17;18+H1.9-08-15"
 *  - "F6.4-09-06;07;08;09 F6.4-09-10"
 */
export function parseCompositeField(raw: string | null | undefined): string[] {
  if (!raw) return [];

  // Bỏ khoảng trắng
  let clean = raw.trim().replace(/\s+/g, '');

  // CẮT BỎ phần "-Tên-ĐiệnThoại" ở cuối, ví dụ:
  // "F6.3-09-16;17-TỐNGQUỐCTRỌNG-0979944914"
  // => còn lại "F6.3-09-16;17"
  //
  // Quy tắc: gặp dấu '-' mà sau đó là CHỮ (kể cả tiếng Việt) thì coi như bắt đầu phần Tên,
  // cắt từ đó trở đi.
  clean = clean.replace(/-[A-Za-zÀ-ỹ][^+]*$/u, '');

  const result: string[] = [];

  // Mỗi cụm ngăn bởi '+' (vd: H1.9-07-16;17;18+H1.9-08-15)
  for (const partRaw of clean.split('+')) {
    const part = partRaw.trim();
    if (!part) continue;

    // Dạng mở rộng: F6.3-09-16;17;18
    let m = part.match(/^([A-Z]\d(?:\.\d+)?)-(\d{2})-([\d;]+)$/i);
    if (m) {
      const block = m[1];        // F6.3
      const row   = m[2];        // 09
      const nums  = m[3];        // "16;17;18"

      for (const grave of nums.split(';')) {
        if (!grave) continue;
        const nn = grave.padStart(2, '0');
        result.push(`${block}-${row}-${nn}`); // F6.3-09-16, F6.3-09-17...
      }
      continue;
    }

    // Dạng đơn: F6.3-09-16
    m = part.match(/^([A-Z]\d(?:\.\d+)?-\d{2}-\d{2})$/i);
    if (m) {
      result.push(m[1]);
      continue;
    }

    // Các dạng khác (như NG-0979944914, hay phần tên bị sót) => bỏ qua
  }

  // Loại trùng cho chắc
  return [...new Set(result)];
}