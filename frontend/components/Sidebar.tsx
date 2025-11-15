'use client'

import { Plus, MessageSquare, Trash2, X } from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getConversations, createConversation, deleteConversation } from '@/lib/api'
import { useChatStore } from '@/lib/store'
import toast from 'react-hot-toast'

interface SidebarProps {
  isOpen: boolean
  onClose: () => void
}

export function Sidebar({ isOpen, onClose }: SidebarProps) {
  const queryClient = useQueryClient()
  const { currentConversation, setCurrentConversation, clearMessages } = useChatStore()

  const { data: conversations = [] } = useQuery({
    queryKey: ['conversations'],
    queryFn: getConversations,
  })

  const createMutation = useMutation({
    mutationFn: createConversation,
    onSuccess: (newConv) => {
      queryClient.invalidateQueries({ queryKey: ['conversations'] })
      setCurrentConversation(newConv)
      clearMessages()
      toast.success('New conversation created')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: deleteConversation,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conversations'] })
      toast.success('Conversation deleted')
    },
  })

  return (
    <>
      {/* Mobile overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar */}
      <aside
        className={`fixed lg:relative inset-y-0 left-0 z-50 w-64 bg-muted/50 border-r border-border transform transition-transform duration-200 ease-in-out ${
          isOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'
        }`}
      >
        <div className="flex flex-col h-full">
          {/* Header */}
          <div className="p-4 border-b border-border flex items-center justify-between">
            <h2 className="font-semibold">Conversations</h2>
            <button
              onClick={onClose}
              className="lg:hidden p-1 hover:bg-background rounded"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* New Chat Button */}
          <div className="p-4">
            <button
              onClick={() => createMutation.mutate()}
              className="w-full flex items-center gap-2 px-4 py-2 rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 transition-colors"
            >
              <Plus className="w-5 h-5" />
              New Chat
            </button>
          </div>

          {/* Conversations List */}
          <div className="flex-1 overflow-y-auto scrollbar-hide">
            {conversations.map((conv) => (
              <div
                key={conv.id}
                className={`group mx-2 mb-1 rounded-lg transition-colors cursor-pointer ${
                  currentConversation?.id === conv.id
                    ? 'bg-background'
                    : 'hover:bg-background/50'
                }`}
                onClick={() => setCurrentConversation(conv)}
              >
                <div className="flex items-center gap-2 p-3">
                  <MessageSquare className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm truncate">
                      {conv.title || 'New conversation'}
                    </p>
                    {conv.message_count !== undefined && (
                      <p className="text-xs text-muted-foreground">
                        {conv.message_count} messages
                      </p>
                    )}
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation()
                      deleteMutation.mutate(conv.id)
                    }}
                    className="opacity-0 group-hover:opacity-100 p-1 hover:bg-destructive/20 rounded transition-all"
                  >
                    <Trash2 className="w-4 h-4 text-destructive" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </aside>
    </>
  )
}
