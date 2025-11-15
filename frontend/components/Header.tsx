'use client'

import { Menu, Moon, Sun, LogOut, User as UserIcon, Settings, BarChart3 } from 'lucide-react'
import { useTheme } from 'next-themes'
import { useAuthStore } from '@/lib/store'
import { logout } from '@/lib/api'
import toast from 'react-hot-toast'

interface HeaderProps {
  onMenuClick: () => void
}

export function Header({ onMenuClick }: HeaderProps) {
  const { theme, setTheme } = useTheme()
  const { user, logout: logoutStore } = useAuthStore()

  const handleLogout = async () => {
    try {
      await logout()
      logoutStore()
      toast.success('Logged out successfully')
    } catch (error) {
      console.error('Logout error:', error)
    }
  }

  return (
    <header className="border-b border-border bg-background">
      <div className="flex items-center justify-between px-4 py-3">
        {/* Left */}
        <div className="flex items-center gap-3">
          <button
            onClick={onMenuClick}
            className="p-2 hover:bg-muted rounded-lg lg:hidden"
            aria-label="Toggle menu"
          >
            <Menu className="w-5 h-5" />
          </button>

          <h1 className="text-xl font-bold bg-gradient-to-r from-primary to-purple-600 bg-clip-text text-transparent">
            Arol AI Assistant
          </h1>
        </div>

        {/* Right */}
        <div className="flex items-center gap-2">
          <button
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
            className="p-2 hover:bg-muted rounded-lg"
            aria-label="Toggle theme"
          >
            {theme === 'dark' ? (
              <Sun className="w-5 h-5" />
            ) : (
              <Moon className="w-5 h-5" />
            )}
          </button>

          <button
            className="p-2 hover:bg-muted rounded-lg"
            aria-label="Analytics"
            title="Analytics"
          >
            <BarChart3 className="w-5 h-5" />
          </button>

          <button
            className="p-2 hover:bg-muted rounded-lg"
            aria-label="Settings"
            title="Settings"
          >
            <Settings className="w-5 h-5" />
          </button>

          <div className="h-6 w-px bg-border" />

          <div className="flex items-center gap-2 px-2">
            <div className="w-8 h-8 rounded-full bg-primary flex items-center justify-center">
              <UserIcon className="w-5 h-5 text-primary-foreground" />
            </div>
            <span className="text-sm font-medium hidden sm:inline">
              {user?.username || 'User'}
            </span>
          </div>

          <button
            onClick={handleLogout}
            className="p-2 hover:bg-muted rounded-lg text-destructive"
            aria-label="Logout"
            title="Logout"
          >
            <LogOut className="w-5 h-5" />
          </button>
        </div>
      </div>
    </header>
  )
}
