import Link from "next/link";

export default function Landing() {
  return (
    <main style={{ padding: 40, fontFamily: "system-ui" }}>
      <h1>banditx</h1>
      <p>Choose language / Выберите язык:</p>
      <ul>
        <li><Link href="/ru/">Русский</Link></li>
        <li><Link href="/en/">English</Link></li>
      </ul>
    </main>
  );
}