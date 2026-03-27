import React from 'react';

export function LoadingSkeleton({ count = 1, className = '' }) {
  return (
    <div className={`space-y-3 ${className}`}>
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="animate-pulse">
          <div className="flex items-center gap-3 p-3 rounded-lg bg-[#1a1a1a] border border-[#222]">
            <div className="w-10 h-10 rounded bg-[#222]" />
            <div className="flex-1 space-y-2">
              <div className="h-4 bg-[#222] rounded w-3/4" />
              <div className="h-3 bg-[#222] rounded w-1/2" />
            </div>
            <div className="w-16 h-6 bg-[#222] rounded" />
          </div>
        </div>
      ))}
    </div>
  );
}

export function CardSkeleton() {
  return (
    <div className="animate-pulse">
      <div className="card p-4 space-y-3">
        <div className="h-5 bg-[#222] rounded w-1/3" />
        <div className="space-y-2">
          <div className="h-4 bg-[#222] rounded" />
          <div className="h-4 bg-[#222] rounded w-5/6" />
          <div className="h-4 bg-[#222] rounded w-4/6" />
        </div>
      </div>
    </div>
  );
}
