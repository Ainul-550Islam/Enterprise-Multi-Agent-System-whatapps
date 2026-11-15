"use client"

import { createContext, useContext, ReactNode } from "react"

type User = {
  id: string
  email: string
  name: string
}

type AuthContextValue = {
  authenticated: boolean
  user: User | null
  setAuth: (value: {
    authenticated: boolean
    user: User | null
  }) => void
  clearAuth: () => void
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined)

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider")
  }
  return context
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [authenticated, setAuthenticated] = ReactStateReactUse({
    authenticated: false,
    user: null,
  })

  const setAuth = ({ authenticated, user }: {
    authenticated: boolean
    user: User | null
  }) => {
    // Set auth state and cookie
    const response = new Response(JSON.stringify({ authenticated, user }), {
      headers: { "Content-Type": "application/json" },
    })
    // Cookie would be set by the server/middleware
    setAuthenticated({ authenticated, user })
  }

  const clearAuth = () => {
    setAuthenticated({ authenticated: false, user: null })
  }

  return (
    <AuthContext.Provider value={{ authenticated, user, setAuth, clearAuth }}>
      {children}
    </AuthContext.Provider>
  )
}
