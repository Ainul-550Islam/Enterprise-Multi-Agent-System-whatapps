/** Cookie names used by the BFF. The session cookie is httpOnly; CSRF is not. */
export const SESSION_COOKIE = "mas_session";
export const CSRF_COOKIE = "mas_csrf";
export const OIDC_STATE_COOKIE = "mas_oidc";

export const SESSION_MAX_AGE_SECONDS = 8 * 60 * 60;
