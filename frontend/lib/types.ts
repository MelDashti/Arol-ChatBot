export interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  processing_time_ms?: number
  sources?: any[]
  rating?: number
}

export interface Conversation {
  id: number
  user_id: number
  title: string | null
  is_archived: boolean
  created_at: string
  updated_at: string | null
  message_count?: number
}

export interface User {
  id: number
  email: string
  username: string
  full_name: string | null
  role: string
  is_active: boolean
  created_at: string
}

export interface Token {
  access_token: string
  refresh_token: string
  token_type: string
}

export interface LoginRequest {
  username: string
  password: string
}

export interface ChatRequest {
  message: string
  conversation_id?: number
  stream?: boolean
  use_cache?: boolean
}

export interface ChatResponse {
  message: string
  conversation_id: number
  sources?: any[]
  confidence_score?: number
  processing_time_ms?: number
}
