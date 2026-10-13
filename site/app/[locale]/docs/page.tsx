import Link from "next/link";
import { getContent } from "@/lib/content";
import { ui, type Locale } from "@/lib/i18n";

export function generateStaticParams() {
  return [{ locale: "ru" }, { locale: "en" }];
}

export default async function DocsIndex({ params }: { params: Promise<{ locale: string }> }) {
  const { locale } = await params;
  const l = locale as Locale;
  const c = getContent(l);

  return (
    <main className="mx-auto max-w-5xl px-4 py-16">
      <h1 className="text-3xl font-bold">{ui[l].navDocs}</h1>
      <ul className="mt-8 space-y-4">
        {c.docs.map((page) => (
          <li key={page.slug} className="rounded border p-5">
            <Link href={`/${l}/docs/${page.slug}`} className="text-lg font-semibold">
              {page.title}
            </Link>
            <p className="mt-1 text-sm" style={{ color: "var(--muted)" }}>{page.description}</p>
          </li>
        ))}
      </ul>
    </main>
  );
}