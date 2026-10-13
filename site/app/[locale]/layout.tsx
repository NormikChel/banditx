import { notFound } from "next/navigation";
import { Providers } from "@/lib/theme";
import { Header } from "@/components/Header";
import { Footer } from "@/components/Footer";
import { locales, type Locale } from "@/lib/i18n";

export function generateStaticParams() {
  return locales.map((locale) => ({ locale }));
}

export default async function LocaleLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: Promise<{ locale: string }>;
}) {
  const { locale } = await params;
  if (!locales.includes(locale as Locale)) notFound();
  const l = locale as Locale;

  return (
    <html lang={l} suppressHydrationWarning>
      <body className="min-h-screen flex flex-col">
        <Providers>
          <Header locale={l} />
          <div className="flex-1">{children}</div>
          <Footer locale={l} />
        </Providers>
      </body>
    </html>
  );
}