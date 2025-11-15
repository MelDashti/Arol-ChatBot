import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type { User, Conversation, Message } from './types'

interface AuthState {
  user: User | null
  isAuthenticated: boolean
  setUser: (user: User | null) => void
  logout: () => void
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      user: null,
      isAuthenticated: false,
      setUser: (user) => set({ user, isAuthenticated: !!user }),
      logout: () => {
        localStorage.removeItem('access_token')
        localStorage.removeItem('refresh_token')
        set({ user: null, isAuthenticated: false })
      },
    }),
    {
      name: 'auth-storage',
    }
  )
)

interface ChatState {
  conversations: Conversation[]
  currentConversation: Conversation | null
  messages: Message[]
  setConversations: (conversations: Conversation[]) => void
  setCurrentConversation: (conversation: Conversation | null) => void
  setMessages: (messages: Message[]) => void
  addMessage: (message: Message) => void
  clearMessages: () => void
}

export const useChatStore = create<ChatState>((set) => ({
  conversations: [],
  currentConversation: null,
  messages: [],
  setConversations: (conversations) => set({ conversations }),
  setCurrentConversation: (conversation) => set({ currentConversation: conversation, messages: [] }),
  setMessages: (messages) => set({ messages }),
  addMessage: (message) => set((state) => ({ messages: [...state.messages, message] })),
  clearMessages: () => set({ messages: [] }),
}))
