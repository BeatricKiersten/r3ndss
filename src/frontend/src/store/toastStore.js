import { create } from 'zustand';

let toastId = 0;

export const useToastStore = create((set, get) => ({
  toasts: [],
  
  addToast: ({ type = 'info', title, message, duration = 5000 }) => {
    const id = ++toastId;
    const toast = { id, type, title, message, duration };
    
    set((state) => ({
      toasts: [...state.toasts, toast]
    }));
    
    if (duration > 0) {
      setTimeout(() => {
        get().removeToast(id);
      }, duration);
    }
    
    return id;
  },
  
  removeToast: (id) => {
    set((state) => ({
      toasts: state.toasts.filter((t) => t.id !== id)
    }));
  },
  
  success: (title, message) => get().addToast({ type: 'success', title, message }),
  error: (title, message) => get().addToast({ type: 'error', title, message }),
  warning: (title, message) => get().addToast({ type: 'warning', title, message }),
  info: (title, message) => get().addToast({ type: 'info', title, message }),
}));

export function toast(options) {
  return useToastStore.getState().addToast(options);
}

toast.success = (title, message) => useToastStore.getState().success(title, message);
toast.error = (title, message) => useToastStore.getState().error(title, message);
toast.warning = (title, message) => useToastStore.getState().warning(title, message);
toast.info = (title, message) => useToastStore.getState().info(title, message);
