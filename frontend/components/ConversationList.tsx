"use client";

import { useEffect, useState } from "react";
import { formatDistanceToNow } from "date-fns";
import { es } from "date-fns/locale";
import type { Conversation } from "@/lib/types";
import { getConversations } from "@/lib/api";

interface ConversationListProps {
    currentConversationId: string | null;
    onSelectConversation: (id: string) => void;
    onNewConversation: () => void;
}

export default function ConversationList({
    currentConversationId,
    onSelectConversation,
    onNewConversation,
}: ConversationListProps) {
    const [conversations, setConversations] = useState<Conversation[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [isOpen, setIsOpen] = useState(true);

    useEffect(() => {
        loadConversations();
    }, [currentConversationId]); // Reload when conversation changes to update titles

    const loadConversations = async () => {
        setIsLoading(true);
        try {
            const data = await getConversations();
            setConversations(data);
        } catch (error) {
            console.error("Error loading conversations:", error);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div
            className={`
                flex flex-col border-r border-slate-700/50 bg-slate-900/50 backdrop-blur-md transition-all duration-300
                ${isOpen ? "w-64" : "w-12"}
            `}
        >
            {/* Header / Toggle */}
            <div className="flex items-center justify-between p-4 border-b border-slate-700/50 h-16">
                {isOpen && <span className="font-semibold text-slate-200">Historial</span>}
                <button
                    onClick={() => setIsOpen(!isOpen)}
                    className="p-1.5 rounded-lg hover:bg-slate-800 text-slate-400 hover:text-white transition-colors"
                >
                    {isOpen ? "◀" : "▶"}
                </button>
            </div>

            {/* Content */}
            {isOpen && (
                <div className="flex-1 overflow-y-auto p-2 space-y-2">
                    <button
                        onClick={onNewConversation}
                        className="w-full flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600/20 hover:bg-blue-600/30 text-blue-300 hover:text-blue-100 transition-colors border border-blue-500/30"
                    >
                        <span className="text-lg">+</span>
                        <span className="text-sm font-medium">Nueva conversación</span>
                    </button>

                    <div className="mt-4 space-y-1">
                        {isLoading && conversations.length === 0 ? (
                            <div className="text-center text-slate-500 text-sm py-4">Cargando...</div>
                        ) : conversations.map((conv) => (
                            <button
                                key={conv.id}
                                onClick={() => onSelectConversation(conv.id)}
                                className={`
                                    w-full text-left px-3 py-2.5 rounded-lg text-sm transition-all group relative
                                    ${currentConversationId === conv.id
                                        ? "bg-slate-800/80 text-white shadow-sm ring-1 ring-slate-700"
                                        : "text-slate-400 hover:bg-slate-800/40 hover:text-slate-200"
                                    }
                                `}
                            >
                                <div className="truncate pr-4 font-medium">{conv.title}</div>
                                <div className="text-xs text-slate-500 mt-0.5 font-light">
                                    {formatDistanceToNow(new Date(conv.updated_at), { addSuffix: true, locale: es })}
                                </div>
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
