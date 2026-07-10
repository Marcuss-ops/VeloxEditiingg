import { useState } from "react";
import { PlatformPills } from "./PlatformPills";
import { DemoModal } from "./DemoModal";

export function Hero() {
  const [demoOpen, setDemoOpen] = useState(false);

  return (
    <section className="pt-24 pb-10 text-center">
      <div className="max-w-[1100px] mx-auto px-6">
        <h1 className="text-[clamp(44px,8vw,76px)] font-extrabold tracking-[-0.03em] leading-[0.95] mb-6 text-black">
          Log in{" "}
          <span className="text-[#0A84FF]">once.</span>{" "}
          Post{" "}
          <span className="text-[#0A84FF]">everywhere.</span>
        </h1>

        <p className="text-[clamp(16px,2.1vw,19px)] text-neutral-700 max-w-[740px] mx-auto mb-8 leading-relaxed">
          Ti connetti{" "}
          <span className="text-[#0A84FF] font-semibold">una volta</span>{" "}
          con OAuth sicuro a Instagram, Facebook, TikTok, YouTube e X — poi
          carichi una volta sola e SocialSync pubblica{" "}
          <span className="text-[#0A84FF] font-semibold">ovunque</span>.
        </p>

        <div className="flex gap-3 justify-center flex-wrap mb-[72px]">
          <a
            href="#"
            className="inline-flex items-center gap-2 px-[18px] py-[10px] rounded-xl text-sm font-semibold bg-black text-white no-underline hover:-translate-y-[1px] hover:bg-neutral-900 transition-all"
          >
            Inizia gratis
          </a>
          <button
            onClick={() => setDemoOpen(true)}
            className="inline-flex items-center gap-2 px-[18px] py-[10px] rounded-xl text-sm font-semibold bg-white text-black border border-black no-underline hover:-translate-y-[1px] hover:bg-neutral-50 transition-all cursor-pointer"
          >
            Guarda demo
          </button>
        </div>

        {/* Upload mockup */}
        <div className="max-w-[920px] mx-auto">
          <div className="max-w-[420px] mx-auto bg-white border border-neutral-100 rounded-xl shadow-[0_10px_40px_rgba(0,0,0,0.06)] overflow-hidden">
            <div className="flex items-center gap-2.5 py-3 px-3.5 bg-neutral-50 border-b border-neutral-100">
              <div className="flex gap-[5px]">
                <span className="w-2 h-2 rounded-full bg-neutral-300" />
                <span className="w-2 h-2 rounded-full bg-neutral-300" />
              </div>
              <span className="text-[12px] text-neutral-500 font-medium">Nuovo post</span>
            </div>
            <div className="py-8 px-6 text-center">
              <div className="w-12 h-12 mx-auto mb-3.5 rounded-[10px] bg-neutral-100 grid place-items-center">
                <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                  <polyline points="17 8 12 3 7 8" />
                  <line x1="12" y1="3" x2="12" y2="15" />
                </svg>
              </div>
              <p className="mb-3.5 font-semibold text-sm text-black">estate-2026.mp4</p>
              <div className="h-1.5 bg-neutral-100 rounded-full max-w-[220px] mx-auto overflow-hidden">
                <div className="w-[68%] h-full bg-[#0A84FF] animate-pulse rounded-full" />
              </div>
            </div>
          </div>
          <PlatformPills />
        </div>

        {/* Platforms row */}
        <div className="mt-20 py-7 border-y border-neutral-100">
          <div className="flex items-center justify-center gap-8 flex-wrap">
            <span className="text-[13px] text-neutral-400 font-medium">Compatibile con</span>
            <div className="flex gap-7 flex-wrap items-center justify-center">
              {["Instagram", "Facebook", "TikTok", "YouTube", "X"].map((p) => (
                <span key={p} className="text-[15px] font-semibold tracking-tight text-black/55">
                  {p}
                </span>
              ))}
            </div>
          </div>
        </div>
      </div>

      <DemoModal open={demoOpen} onClose={() => setDemoOpen(false)} />
    </section>
  );
}
