import React from 'react';

export function Badge({ children, variant = 'default', className = '' }) {
  const variants = {
    default: 'bg-[#222] text-[#888]',
    success: 'bg-green-400/10 text-green-400',
    warning: 'bg-yellow-400/10 text-yellow-400',
    error: 'bg-red-400/10 text-red-400',
    info: 'bg-blue-400/10 text-blue-400',
  };

  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${variants[variant]} ${className}`}>
      {children}
    </span>
  );
}
