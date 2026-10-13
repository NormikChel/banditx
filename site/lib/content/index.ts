import { ru } from "./ru";
import { en } from "./en";
import type { Locale } from "../i18n";
import type { SiteContent } from "./types";

export const content: Record<Locale, SiteContent> = { ru, en };

export function getContent(locale: Locale): SiteContent {
  return content[locale];
}

// === Проверка: одинаковое ли количество страниц/блоков/фич ===
// Выполняется при импорте, то есть на этапе сборки Next.js.
(function assertSameShape() {
  const errs: string[] = [];

  if (ru.home.features.length !== en.home.features.length) {
    errs.push(`home.features: ru=${ru.home.features.length} en=${en.home.features.length}`);
  }
  if (ru.docs.length !== en.docs.length) {
    errs.push(`docs.length: ru=${ru.docs.length} en=${en.docs.length}`);
  }

  const n = Math.min(ru.docs.length, en.docs.length);
  for (let i = 0; i < n; i++) {
    const r = ru.docs[i], e = en.docs[i];
    if (r.slug !== e.slug) {
      errs.push(`docs[${i}].slug: ru="${r.slug}" en="${e.slug}"`);
      continue;
    }
    if (r.blocks.length !== e.blocks.length) {
      errs.push(`docs["${r.slug}"].blocks.length: ru=${r.blocks.length} en=${e.blocks.length}`);
      continue;
    }
    for (let j = 0; j < r.blocks.length; j++) {
      if (r.blocks[j].type !== e.blocks[j].type) {
        errs.push(
          `docs["${r.slug}"].blocks[${j}].type: ru="${r.blocks[j].type}" en="${e.blocks[j].type}"`
        );
      }
    }
  }

  if (errs.length > 0) {
    throw new Error("i18n shape mismatch:\n" + errs.join("\n"));
  }
})();