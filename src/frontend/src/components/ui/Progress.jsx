import React from 'react';

export function Progress({ value, showLabel = true, size = 'default', className = '' }) {
  const sizes = {
    sm: 'h-1',
    default: 'h-1.5',
    lg: 'h-2',
  };

  const getBarClass = () => {
    if (value >= 100) return 'complete';
    if (value > 0) return 'in-progress';
    return '';
  };

  return (
    <div className={`w-full ${className}`}>
      {showLabel && (
        <div className="flex items-center justify-between text-xs mb-1">
          <span className="text-[#666]">{Math.round(value)}%</span>
        </div>
      )}
      <div className={`progress-bar ${sizes[size]}`}>
        <div 
          className={`progress-bar-fill ${getBarClass()}`} 
          style={{ width: `${Math.min(100, Math.max(0, value))}%` }} 
        />
      </div>
    </div>
  );
}
