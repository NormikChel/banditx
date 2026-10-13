"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { useEffect, useState } from "react";
import { locales, ui, type Locale } from "@/lib/i18n";

export function Header({ locale }: { locale: Locale }) {
  const t = ui[locale];
  const { theme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  const pathname = usePathname();

  useEffect(() => setMounted(true), []);

  const other = locale === "ru" ? "en" : "ru";
  const pathWithoutLocale = pathname.replace(/^\/(ru|en)/, "") || "/";

  return (
    <header
      className="sticky top-0 z-40 w-full border-b backdrop-blur"
      style={{ background: "color-mix(in srgb, var(--bg) 85%, transparent)" }}
    >
      <div className="mx-auto flex h-14 max-w-5xl items-center justify-between px-4">
        <div className="flex items-center gap-6">
          <Link href={`/${locale}`} className="text-lg font-semibold" style={{ color: "var(--fg)" }}>
            {t.brand}
          </Link>
          <nav className="flex items-center gap-4 text-sm">
            <Link href={`/${locale}`}>{t.navHome}</Link>
            <Link href={`/${locale}/docs`}>{t.navDocs}</Link>
            <a href="https://github.com/NormikChel/banditx" target="_blank" rel="noreferrer">
              {t.navGithub}
            </a>
          </nav>
        </div>

        <div className="flex items-center gap-3 text-sm">
          <Link href={`/${other}${pathWithoutLocale === "/" ? "" : pathWithoutLocale}`} className="uppercase">
            {other}
          </Link>
          {mounted && (
            <select
              value={theme}
              onChange={(e) => setTheme(e.target.value)}
              className="rounded border px-2 py-1"
              style={{ background: "var(--bg)", color: "var(--fg)" }}
            >
              <option value="light">{t.themeLight}</option>
              <option value="dark">{t.themeDark}</option>
              <option value="system">{t.themeSystem}</option>
            </select>
          )}
        </div>
      </div>
    </header>
  );
}