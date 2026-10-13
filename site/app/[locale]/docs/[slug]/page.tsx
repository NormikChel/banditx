import { notFound } from "next/navigation";
import { getContent } from "@/lib/content";
import { locales, type Locale } from "@/lib/i18n";

export function generateStaticParams() {
  const params: { locale: string; slug: string }[] = [];
  for (const locale of locales) {
    const c = getContent(locale);
    for (const page of c.docs) {
      params.push({ locale, slug: page.slug });
    }
  }
  return params;
}

export default async function DocPage({
  params,
}: {
  params: Promise<{ locale: string; slug: string }>;
}) {
  const { locale, slug } = await params;
  const l = locale as Locale;
  const c = getContent(l);
  const page = c.docs.find((p) => p.slug === slug);
  if (!page) notFound();

  return (
    <main className="mx-auto max-w-5xl px-4 py-16">
      <h1 className="text-3xl font-bold">{page.title}</h1>
      <p className="mt-2 text-lg" style={{ color: "var(--muted)" }}>{page.description}</p>

      <article className="mt-10 space-y-4 leading-relaxed">
        {page.blocks.map((b, i) => {
          if (b.type === "paragraph") return <p key={i}>{b.text}</p>;
          if (b.type === "heading") {
            const Tag = b.level === 2 ? "h2" : "h3";
            const cls = b.level === 2 ? "text-2xl font-semibold mt-10" : "text-xl font-semibold mt-6";
            return <Tag key={i} className={cls}>{b.text}</Tag>;
          }
          if (b.type === "code") {
            return (
              <pre key={i} className="rounded border p-4 overflow-x-auto text-sm">
                <code>{b.code}</code>
              </pre>
            );
          }
          if (b.type === "list") {
            return (
              <ul key={i} className="list-disc pl-6 space-y-1">
                {b.items.map((it, j) => <li key={j}>{it}</li>)}
              </ul>
            );
          }
          return null;
        })}
      </article>
    </main>
  );
}