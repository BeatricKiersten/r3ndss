import React from 'react';
import { CheckCircle, XCircle, AlertTriangle, Info, X } from 'lucide-react';
import { useToastStore } from '../../store/toastStore';

const iconMap = {
  success: CheckCircle,
  error: XCircle,
  warning: AlertTriangle,
  info: Info,
};

const colorMap = {
  success: {
    bg: 'bg-green-500/10',
    border: 'border-green-500/20',
    icon: 'text-green-400',
  },
  error: {
    bg: 'bg-red-500/10',
    border: 'border-red-500/20',
    icon: 'text-red-400',
  },
  warning: {
    bg: 'bg-yellow-500/10',
    border: 'border-yellow-500/20',
    icon: 'text-yellow-400',
  },
  info: {
    bg: 'bg-blue-500/10',
    border: 'border-blue-500/20',
    icon: 'text-blue-400',
  },
};

function ToastItem({ toast }) {
  const removeToast = useToastStore((state) => state.removeToast);
  const Icon = iconMap[toast.type] || Info;
  const colors = colorMap[toast.type] || colorMap.info;

  return (
    <div
      className={`${colors.bg} border ${colors.border} rounded-lg p-4 shadow-lg animate-slide-in`}
    >
      <div className="flex items-start gap-3">
        <Icon className={`w-5 h-5 ${colors.icon} flex-shrink-0 mt-0.5`} />
        <div className="flex-1 min-w-0">
          {toast.title && (
            <p className="text-sm font-medium text-white">{toast.title}</p>
          )}
          {toast.message && (
            <p className="text-xs text-[#888] mt-0.5">{toast.message}</p>
          )}
        </div>
        <button
          onClick={() => removeToast(toast.id)}
          className="p-1 hover:bg-white/10 rounded transition-colors"
        >
          <X className="w-4 h-4 text-[#666]" />
        </button>
      </div>
    </div>
  );
}

export function ToastContainer() {
  const toasts = useToastStore((state) => state.toasts);

  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 space-y-2 max-w-sm w-full">
      {toasts.map((toast) => (
        <ToastItem key={toast.id} toast={toast} />
      ))}
    </div>
  );
}
