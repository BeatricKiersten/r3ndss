import React from 'react';
import { FileQuestion, Upload, FolderOpen, CloudOff, Search } from 'lucide-react';

const iconMap = {
  file: FileQuestion,
  upload: Upload,
  folder: FolderOpen,
  cloud: CloudOff,
  search: Search,
};

export function EmptyState({ 
  icon = 'file', 
  title = 'No data', 
  description = 'Nothing to show here',
  action = null,
  className = ''
}) {
  const Icon = iconMap[icon] || iconMap.file;

  return (
    <div className={`flex flex-col items-center justify-center py-12 px-4 text-center ${className}`}>
      <div className="w-16 h-16 rounded-2xl bg-[#1a1a1a] flex items-center justify-center mb-4">
        <Icon className="w-8 h-8 text-[#555]" />
      </div>
      <h3 className="text-lg font-medium text-[#aaa] mb-1">{title}</h3>
      <p className="text-sm text-[#666] max-w-sm">{description}</p>
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
