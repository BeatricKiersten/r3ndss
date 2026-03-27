import React from 'react';
import { AlertTriangle, X } from 'lucide-react';

export function ConfirmDialog({ 
  isOpen, 
  onClose, 
  onConfirm, 
  title = 'Confirm', 
  message = 'Are you sure?',
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  variant = 'warning'
}) {
  if (!isOpen) return null;

  const handleConfirm = async () => {
    await onConfirm();
    onClose();
  };

  const iconColors = {
    warning: 'text-yellow-400',
    danger: 'text-red-400',
    info: 'text-blue-400',
  };

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4" onClick={onClose}>
      <div className="card p-6 max-w-sm w-full" onClick={e => e.stopPropagation()}>
        <div className="flex items-start gap-4">
          <div className={`w-10 h-10 rounded-full bg-[#222] flex items-center justify-center flex-shrink-0`}>
            <AlertTriangle className={`w-5 h-5 ${iconColors[variant]}`} />
          </div>
          <div className="flex-1">
            <h3 className="text-lg font-medium text-white mb-2">{title}</h3>
            <p className="text-sm text-[#888] mb-4">{message}</p>
            <div className="flex gap-2 justify-end">
              <button 
                onClick={onClose}
                className="px-4 py-2 text-sm text-[#888] hover:text-white transition-colors"
              >
                {cancelText}
              </button>
              <button 
                onClick={handleConfirm}
                className={`px-4 py-2 text-sm rounded-lg font-medium transition-colors ${
                  variant === 'danger' 
                    ? 'bg-red-500/20 text-red-400 hover:bg-red-500/30' 
                    : variant === 'warning'
                    ? 'bg-yellow-500/20 text-yellow-400 hover:bg-yellow-500/30'
                    : 'bg-blue-500/20 text-blue-400 hover:bg-blue-500/30'
                }`}
              >
                {confirmText}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
