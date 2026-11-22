"use client";

import { useEffect, useState } from "react";

/** Ephemeral status message. Never used for error detail (use ErrorState). */
export function Toast({ message, onDone }: { message: string | null; onDone?: () => void }) {
  const [visible, setVisible] = useState(Boolean(message));
  useEffect(() => {
    setVisible(Boolean(message));
    if (!message) return;
    const timer = setTimeout(() => {
      setVisible(false);
      onDone?.();
    }, 4000);
    return () => clearTimeout(timer);
  }, [message, onDone]);
  if (!visible || !message) return null;
  return <div role="status" className="rounded border border-line bg-panel px-3 py-2 text-sm">{message}</div>;
}
