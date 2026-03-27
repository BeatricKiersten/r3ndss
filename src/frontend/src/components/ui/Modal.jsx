import React from 'react';
import { X } from 'lucide-react';

export function Modal({ isOpen, onClose, title, children, maxWidth = 'max-w-lg' }) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50 p-4" onClick={onClose}>
      <div 
        className={`card p-5 w-full ${maxWidth} max-h-[90vh] overflow-y-auto`} 
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-medium text-white">{title}</h3>
          <button onClick={onClose} className="p-1.5 hover:bg-[#222] rounded transition-colors">
            <X className="w-5 h-5 text-[#666]" />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}
