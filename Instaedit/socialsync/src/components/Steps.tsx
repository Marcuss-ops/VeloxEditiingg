export function Steps() {
  const items = [
    {
      num: 1,
      title: "Connetti",
      desc: "OAuth sicuro. Colleghi tutti i tuoi account in 60 secondi. Nessuna password salvata.",
    },
    {
      num: 2,
      title: "Carica",
      desc: "Un solo upload per video, foto e testo. Aggiungi hashtag e menzioni una volta.",
    },
    {
      num: 3,
      title: "Pubblica",
      desc: "SocialSync adatta formati, ritaglia e pubblica ovunque, in simultanea o programmato.",
    },
  ];

  return (
    <section id="come-funziona" className="py-24">
      <div className="max-w-[1100px] mx-auto px-6">
        <div className="text-center max-w-[640px] mx-auto mb-14">
          <h2 className="text-[clamp(32px,4.5vw,44px)] font-extrabold tracking-[-0.02em] mb-3 text-black">
            Come funziona
          </h2>
          <p className="text-neutral-500 text-[17px]">
            Tre passaggi, zero complicazioni. Dal primo login alla pubblicazione.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {items.map((item) => (
            <div
              key={item.num}
              className="bg-white border border-neutral-100 rounded-xl p-7 hover:-translate-y-[2px] hover:shadow-[0_8px_24px_rgba(0,0,0,0.05)] transition-all"
            >
              <span className="inline-flex w-7 h-7 rounded-lg bg-neutral-100 items-center justify-center text-[13px] font-bold mb-4 text-black">
                {item.num}
              </span>
              <h3 className="text-xl font-bold tracking-tight mb-2 text-black">{item.title}</h3>
              <p className="text-neutral-500 text-[15px] leading-relaxed">{item.desc}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
