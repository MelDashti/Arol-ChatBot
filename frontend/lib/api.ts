import axios from 'axios'
import type { ChatRequest, ChatResponse, Conversation, LoginRequest, Token, User } from './types'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor to add auth token
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// Response interceptor to handle auth errors
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    if (error.response?.status === 401) {
      // Try to refresh token
      const refreshToken = localStorage.getItem('refresh_token')
      if (refreshToken) {
        try {
          const response = await axios.post(`${API_URL}/api/auth/refresh`, {
            refresh_token: refreshToken,
          })
          const { access_token, refresh_token } = response.data
          localStorage.setItem('access_token', access_token)
          localStorage.setItem('refresh_token', refresh_token)

          // Retry original request
          error.config.headers.Authorization = `Bearer ${access_token}`
          return axios(error.config)
        } catch (refreshError) {
          // Refresh failed, logout
          localStorage.removeItem('access_token')
          localStorage.removeItem('refresh_token')
          window.location.href = '/login'
        }
      }
    }
    return Promise.reject(error)
  }
)

// Auth APIs
export const login = async (data: LoginRequest): Promise<Token> => {
  const response = await api.post('/api/auth/login', data)
  return response.data
}

export const register = async (data: any): Promise<User> => {
  const response = await api.post('/api/auth/register', data)
  return response.data
}

export const logout = async (): Promise<void> => {
  await api.post('/api/auth/logout')
}

export const getCurrentUser = async (): Promise<User> => {
  const response = await api.get('/api/auth/me')
  return response.data
}

// Chat APIs
export const sendMessage = async (data: ChatRequest): Promise<ChatResponse> => {
  const response = await api.post('/api/chat/message', data)
  return response.data
}

export const getConversations = async (): Promise<Conversation[]> => {
  const response = await api.get('/api/chat/conversations')
  return response.data
}

export const getConversation = async (id: number): Promise<any> => {
  const response = await api.get(`/api/chat/conversations/${id}`)
  return response.data
}

export const deleteConversation = async (id: number): Promise<void> => {
  await api.delete(`/api/chat/conversations/${id}`)
}

export const createConversation = async (title?: string): Promise<Conversation> => {
  const response = await api.post('/api/chat/conversations', { title })
  return response.data
}

// Analytics APIs
export const getUserStats = async (): Promise<any> => {
  const response = await api.get('/api/analytics/my-stats')
  return response.data
}

export const getActivity = async (days: number = 30): Promise<any> => {
  const response = await api.get(`/api/analytics/activity?days=${days}`)
  return response.data
}

// Health check
export const healthCheck = async (): Promise<any> => {
  const response = await api.get('/api/health')
  return response.data
}

export default api
