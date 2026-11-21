import type { Metadata } from "next";
import type { ReactNode } from "react";
import { headers } from "next/headers";

import { SessionProvider } from "@/lib/auth/SessionProvider";

import "./globals.css";

export const metadata: Metadata = {
  title: "Enterprise MAS Dashboard",
  description: "Control plane for agents, executions, workflows and the WhatsApp plane.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  const nonce = headers().get("x-nonce") ?? undefined;
  return (
    <html lang="en" className="dark">
      <body data-nonce={nonce}>
        <SessionProvider>{children}</SessionProvider>
      </body>
    </html>
  );
}
