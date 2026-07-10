export function Footer() {
  return (
    <footer className="border-t border-neutral-100 py-8">
      <div className="max-w-[1100px] mx-auto px-6 flex justify-between items-center gap-4 flex-wrap text-[13px] text-neutral-400">
        <span>© 2026 SocialSync</span>
        <div className="flex gap-5">
          <a href="#" className="text-neutral-400 no-underline hover:text-black transition-colors">
            Privacy
          </a>
          <a href="#" className="text-neutral-400 no-underline hover:text-black transition-colors">
            Termini
          </a>
          <a href="#" className="text-neutral-400 no-underline hover:text-black transition-colors">
            Contatti
          </a>
        </div>
      </div>
    </footer>
  );
}
