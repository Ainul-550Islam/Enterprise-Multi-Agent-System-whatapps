import { CSRF_COOKIE } from "@/lib/auth/cookies";

/** Reads the double-submit CSRF cookie in the browser. Empty on the server. */
export function readCsrfCookie(cookieHeader?: string): string {
  const source =
    cookieHeader ?? (typeof document === "undefined" ? "" : document.cookie);
  const parts = source.split(";").map((part) => part.trim());
  for (const part of parts) {
    if (part.startsWith(`${CSRF_COOKIE}=`)) {
      return decodeURIComponent(part.slice(CSRF_COOKIE.length + 1));
    }
  }
  return "";
}
