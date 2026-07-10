import { Nav } from "./components/Nav";
import { Hero } from "./components/Hero";
import { Steps } from "./components/Steps";
import { Features } from "./components/Features";
import { CTA } from "./components/CTA";
import { Footer } from "./components/Footer";

function App() {
  return (
    <div className="min-h-screen bg-white text-black antialiased">
      <Nav />
      <main>
        <Hero />
        <Steps />
        <Features />
        <CTA />
      </main>
      <Footer />
    </div>
  );
}

export default App;
