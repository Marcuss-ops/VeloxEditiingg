export function PlatformPills() {
  const platforms = [
    {
      name: "Instagram",
      svg: (
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4 opacity-85">
          <rect x="2" y="2" width="20" height="20" rx="5" />
          <path d="M16 11.37A4 4 0 1 1 12.63 8 4 4 0 0 1 16 11.37z" />
          <line x1="17.5" y1="6.5" x2="17.5" y2="6.5" />
        </svg>
      ),
    },
    {
      name: "Facebook",
      svg: (
        <svg viewBox="0 0 24 24" fill="currentColor" className="w-4 h-4 opacity-85">
          <path d="M13.5 22v-8h2.7l.4-3.2H13.5V8.5c0-.9.3-1.5 1.6-1.5h1.7V4.1c-.3 0-1.3-.1-2.5-.1-2.5 0-4.2 1.5-4.2 4.3v2.5H7.3V14h2.8v8h3.4z" />
        </svg>
      ),
    },
    {
      name: "TikTok",
      svg: (
        <svg viewBox="0 0 24 24" fill="currentColor" className="w-4 h-4 opacity-85">
          <path d="M19.6 8.2c-1.2 0-2.3-.4-3.2-1.1v6.4c0 3.5-2.8 6.3-6.3 6.3S3.8 17 3.8 13.5 6.6 7.2 10.1 7.2c.4 0 .7 0 1 .1v2.8c-.3-.1-.7-.2-1-.2-1.9 0-3.5 1.6-3.5 3.6s1.6 3.5 3.5 3.5 3.5-1.6 3.5-3.5V3.5h2.7c.3 1.2 1.3 2.2 2.5 2.5v2.2z" />
        </svg>
      ),
    },
    {
      name: "YouTube",
      svg: (
        <svg viewBox="0 0 24 24" fill="currentColor" className="w-4 h-4 opacity-85">
          <path d="M21.6 7.2c-.2-.8-.8-1.4-1.6-1.6-1.6-.4-8-.4-8-.4s-6.4 0-8 .4c-.8.2-1.4.8-1.6 1.6C2 8.8 2 12 2 12s0 3.2.4 4.8c.2.8.8 1.4 1.6 1.6 1.6.4 8 .4 8 .4s6.4 0 8-.4c.8-.2 1.4-.8 1.6-1.6.4-1.6.4-4.8.4-4.8s0-3.2-.4-4.8zM10 15.2V8.8l5.2 3.2-5.2 3.2z" />
        </svg>
      ),
    },
    {
      name: "X",
      svg: (
        <svg viewBox="0 0 24 24" fill="currentColor" className="w-4 h-4 opacity-85">
          <path d="M17.5 4.5h3.1l-6.8 7.8 8 10.6h-6.3l-4.9-6.4-5.6 6.4H2l7.3-8.3L1.7 4.5h6.4l4.4 5.9 5-5.9z" />
        </svg>
      ),
    },
  ];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2.5 mt-7">
      {platforms.map((p) => (
        <div
          key={p.name}
          className="flex items-center justify-center gap-2 py-2.5 px-3 border border-neutral-100 rounded-xl bg-white text-[13px] font-medium hover:-translate-y-[1px] hover:border-neutral-300 hover:shadow-[0_4px_12px_rgba(0,0,0,0.04)] transition-all cursor-default"
        >
          {p.svg}
          {p.name}
        </div>
      ))}
    </div>
  );
}
