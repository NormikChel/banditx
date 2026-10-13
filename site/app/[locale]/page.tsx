import Link from "next/link";
import { getContent } from "@/lib/content";
import { ui, type Locale } from "@/lib/i18n";

export function generateStaticParams() {
  return [{ locale: "ru" }, { locale: "en" }];
}

export default async function Home({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  const l = locale as Locale;
  const t = ui[l];
  const c = getContent(l);

  return (
    <main className="mx-auto max-w-5xl px-4 py-16">
      <h1 className="text-5xl font-bold">{c.home.tagline}</h1>
      <p className="mt-4 text-lg" style={{ color: "var(--muted)" }}>{c.home.intro}</p>

      <div className="mt-8 flex gap-3">
        <Link href={`/${l}/docs`} className="rounded px-4 py-2 font-medium" style={{ background: "var(--accent)", color: "#fff" }}>
          {t.navDocs}
        </Link>
        <a href="https://github.com/NormikChel/banditx" className="rounded border px-4 py-2 font-medium">
          {t.navGithub}
        </a>
      </div>

      <section className="mt-16 grid gap-6 sm:grid-cols-2">
        {c.home.features.map((f) => (
          <div key={f.title} className="rounded border p-5">
            <h3 className="font-semibold">{f.title}</h3>
            <p className="mt-2 text-sm" style={{ color: "var(--muted)" }}>{f.body}</p>
          </div>
        ))}
      </section>

      <section className="mt-16">
        <pre className="rounded border p-4 overflow-x-auto text-sm">
          <code>{c.home.quickStart.code}</code>
        </pre>
      </section>
    </main>
  );
}