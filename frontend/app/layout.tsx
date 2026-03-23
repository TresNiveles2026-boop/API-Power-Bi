import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AI-BI Orchestrator | Lenguaje Natural → Power BI",
  description:
    "Plataforma de inteligencia de negocio potenciada por IA. Genera visuales, DAX y filtros en Power BI usando lenguaje natural.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="es" className="dark" suppressHydrationWarning>
      <head>
        <link
          href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className="antialiased" suppressHydrationWarning>
        {children}
      </body>
    </html>
  );
}
