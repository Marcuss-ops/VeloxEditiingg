import { useEffect } from "react";
import { X } from "lucide-react";
import { cn } from "../lib/utils";

interface Props {
  open: boolean;
  onClose: () => void;
}

export function DemoModal({ open, onClose }: Props) {
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open, onClose]);

  return (
    <div
      data-testid="demo-modal-backdrop"
      className={cn(
        "fixed inset-0 bg-black/60 flex items-center justify-center z-[100] p-6",
        open ? "flex" : "hidden"
      )}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="bg-white rounded-xl max-w-[800px] w-full overflow-hidden shadow-[0_20px_60px_rgba(0,0,0,0.3)] animate-[modalIn_0.2s_ease]">
        <div className="flex justify-between items-center py-3.5 px-[18px] border-b border-neutral-100">
          <h3 className="text-[15px] font-semibold text-black">SocialSync — Demo rapida</h3>
          <button onClick={onClose} data-testid="demo-modal-close" className="text-[22px] leading-none p-1 cursor-pointer border-0 bg-transparent" aria-label="Chiudi">
            <X size={22} />
          </button>
        </div>
        <div className="aspect-video bg-black grid place-items-center text-white relative">
          <div className="w-[72px] h-[72px] rounded-full bg-white/12 border border-white/20 grid place-items-center backdrop-blur-lg">
            <svg width="28" height="28" viewBox="0 0 24 24" fill="white">
              <path d="M8 5v14l11-7z" />
            </svg>
          </div>
        </div>
      </div>
    </div>
  );
}
