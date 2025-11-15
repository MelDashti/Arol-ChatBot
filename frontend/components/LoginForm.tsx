'use client'

import { useState } from 'react'
import { login, getCurrentUser } from '@/lib/api'
import { useAuthStore } from '@/lib/store'
import { Loader2 } from 'lucide-react'
import toast from 'react-hot-toast'

export function LoginForm() {
  const [isLogin, setIsLogin] = useState(true)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const { setUser } = useAuthStore()

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)

    try {
      const tokenData = await login({ username, password })
      localStorage.setItem('access_token', tokenData.access_token)
      localStorage.setItem('refresh_token', tokenData.refresh_token)

      const userData = await getCurrentUser()
      setUser(userData)
      toast.success('Logged in successfully!')
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Login failed')
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-primary/10 via-background to-purple-600/10">
      <div className="w-full max-w-md p-8 bg-background rounded-2xl shadow-2xl border border-border">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-purple-600 bg-clip-text text-transparent">
            Arol AI Chatbot
          </h1>
          <p className="text-muted-foreground mt-2">
            Your intelligent conversational assistant
          </p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="username" className="block text-sm font-medium mb-2">
              Username
            </label>
            <input
              id="username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-4 py-2 rounded-lg border border-input bg-background focus:outline-none focus:ring-2 focus:ring-ring"
              required
              disabled={isLoading}
            />
          </div>

          <div>
            <label htmlFor="password" className="block text-sm font-medium mb-2">
              Password
            </label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-4 py-2 rounded-lg border border-input bg-background focus:outline-none focus:ring-2 focus:ring-ring"
              required
              disabled={isLoading}
            />
          </div>

          <button
            type="submit"
            disabled={isLoading}
            className="w-full py-3 bg-primary text-primary-foreground rounded-lg font-medium hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-all flex items-center justify-center gap-2"
          >
            {isLoading && <Loader2 className="w-5 h-5 animate-spin" />}
            {isLogin ? 'Sign In' : 'Sign Up'}
          </button>
        </form>

        <p className="text-center mt-6 text-sm text-muted-foreground">
          Demo: username: <code className="bg-muted px-2 py-1 rounded">demo</code> / password: <code className="bg-muted px-2 py-1 rounded">demo123</code>
        </p>
      </div>
    </div>
  )
}
