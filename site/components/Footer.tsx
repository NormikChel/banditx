import { ui, type Locale } from "@/lib/i18n";

export function Footer({ locale }: { locale: Locale }) {
  return (
    <footer className="mt-20 border-t">
      <div className="mx-auto max-w-5xl px-4 py-8 text-sm" style={{ color: "var(--muted)" }}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>© {new Date().getFullYear()} banditx · MIT</div>
          <div>
            <a href="https://github.com/NormikChel/banditx">github.com/NormikChel/banditx</a>
          </div>
        </div>
      </div>
    </footer>
  );
}