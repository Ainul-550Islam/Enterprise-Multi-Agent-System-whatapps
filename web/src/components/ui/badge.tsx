"use client";

export type BadgeTone = "neutral" | "ok" | "warn" | "danger";

export interface BadgeProps {
  tone?: BadgeTone;
  children: string;
}

const toneClass: Record<BadgeTone, string> = {
  neutral: "mas-badge mas-badge-neutral",
  ok: "mas-badge mas-badge-ok",
  warn: "mas-badge mas-badge-warn",
  danger: "mas-badge mas-badge-danger",
};

/** Compact status pill; the tone maps to the visual class, nothing else. */
export function Badge({ tone = "neutral", children }: BadgeProps) {
  return (
    <span className={toneClass[tone]} data-tone={tone}>
      {children}
    </span>
  );
}

/** Maps a resource status string to a badge tone. */
export function toneForStatus(status: string): BadgeTone {
  switch (status) {
    case "succeeded":
    case "completed":
    case "healthy":
    case "active":
    case "published":
    case "approved":
      return "ok";
    case "running":
    case "queued":
    case "paused":
    case "pending":
      return "warn";
    case "failed":
    case "cancelled":
    case "canceled":
    case "revoked":
    case "denied":
      return "danger";
    default:
      return "neutral";
  }
}
