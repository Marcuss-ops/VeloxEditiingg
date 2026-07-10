import { useState } from "react";
import { Menu, X } from "lucide-react";
import { cn } from "../lib/utils";

const links = [
  { href: "#funzioni", label: "Funzioni" },
  { href: "#come-funziona", label: "Come funziona" },
  { href: "#prezzi", label: "Prezzi" },
];

export function Nav() {
  const [open, setOpen] = useState(false);

  const scrollTo = (e: React.MouseEvent<HTMLAnchorElement>, id: string) => {
    e.preventDefault();
    setOpen(false);
    document.querySelector(id)?.scrollIntoView({ behavior: "smooth" });
  };

  return (
    <nav className="sticky top-0 z-50 bg-white/85 backdrop-blur-xl border-b border-neutral-100">
      <div className="max-w-[1100px] mx-auto px-6 h-16 flex items-center justify-between gap-4">
        <a href="#" className="font-extrabold text-[19px] tracking-tight no-underline text-black">
          SocialSync
        </a>

        {/* Desktop links */}
        <div className="hidden md:flex items-center gap-7">
          {links.map((l) => (
            <a
              key={l.href}
              href={l.href}
              onClick={(e) => scrollTo(e, l.href)}
              className="text-sm font-medium text-black/70 hover:text-black transition-colors no-underline"
            >
              {l.label}
            </a>
          ))}
          <a
            href="#accedi"
            onClick={(e) => scrollTo(e, "#accedi")}
            className="text-sm font-medium text-black/70 hover:text-black transition-colors no-underline"
          >
            Accedi
          </a>
        </div>

        {/* Desktop CTA */}
        <div className="hidden md:block">
          <a
            href="#"
            className="inline-flex items-center gap-2 px-[18px] py-[10px] rounded-xl text-sm font-semibold bg-black text-white no-underline hover:-translate-y-[1px] hover:bg-neutral-900 transition-all"
          >
            Inizia gratis
          </a>
        </div>

        {/* Mobile toggle */}
        <button
          onClick={() => setOpen(!open)}
          className="md:hidden p-2"
          aria-label="Menu"
        >
          {open ? <X size={24} /> : <Menu size={24} />}
        </button>
      </div>

      {/* Mobile menu */}
      <div
        className={cn(
          "md:hidden border-b border-neutral-100 bg-white flex-col px-6 pb-4 pt-2 gap-0",
          open ? "flex" : "hidden"
        )}
      >
        {links.map((l) => (
          <a
            key={l.href}
            href={l.href}
            onClick={(e) => scrollTo(e, l.href)}
            className="py-3.5 text-sm font-medium border-b border-neutral-100 last:border-b-0 no-underline text-black"
          >
            {l.label}
          </a>
        ))}
        <a
          href="#accedi"
          onClick={(e) => scrollTo(e, "#accedi")}
          className="py-3.5 text-sm font-medium border-b border-neutral-100 last:border-b-0 no-underline text-black"
        >
          Accedi
        </a>
        <a
          href="#"
          className="mt-3 inline-flex items-center justify-center gap-2 px-[18px] py-[10px] rounded-xl text-sm font-semibold bg-black text-white no-underline"
        >
          Inizia gratis
        </a>
      </div>
    </nav>
  );
}
